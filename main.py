"""Photon iMessage 插件 (AstrBot Star 插件)。

通过 photon.codes 托管的 Spectrum 连接 iMessage。Photon 只提供 TypeScript 版
spectrum-ts SDK (gRPC)，因此本插件内置一个受监督的 Node sidecar 做桥接:

- Python 侧: 启动并监督 sidecar 进程; 读取 sidecar 的 ``GET /inbound`` NDJSON 流,
  把收到的 iMessage 转成 AstrMessageEvent 提交到 ``context.get_event_queue()``,
  走与平台适配器相同的管线 (其他插件 + LLM 都能正常处理)。
- sidecar 侧: 用 ``PHOTON_PROJECT_ID/PROJECT_SECRET`` 连接 Spectrum,
  推送入站消息; 暴露 /healthz /send /send-attachment /typing 等本地 HTTP 接口。
- 回复: ``PhotonPlatformEvent.send()`` 通过本插件调用 sidecar 的 ``POST /send``。

配置项由 ``_conf_schema.json`` 定义, 直接显示在 AstrBot WebUI 的「插件配置」弹窗。
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import time

import aiohttp

from astrbot import logger
from astrbot.api import AstrBotConfig
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.message_components import File, Image, Plain, Record, Video
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    PlatformMetadata,
)
from astrbot.api.star import Context, Star

# GreedyStr: 指令参数里的「其余全部文本」标记类型（AstrBot 内置，做兼容导入）。
# 必须能在模块全局解析到（inspect.signature(eval_str=True) 会用它求值注解）。
try:  # pragma: no cover - 取决于 AstrBot 版本
    from astrbot.core.star.filter.command import GreedyStr
except Exception:  # noqa: BLE001
    GreedyStr = str  # type: ignore[assignment,misc]

from .photon_platform_event import PhotonPlatformEvent

_PLUGIN_NAME = "astrbot_plugin_photon_imessage"
_DEFAULT_SIDECAR_PORT = 8789
_SIDECAR_READY_TIMEOUT = 20.0  # 秒，冷启动 npm install 可能需要更久

# 句子结束符（中英文）
_SENTENCE_END = "。！？!?；;…\n"


def split_text(text: str, max_len: int, mode: str = "smart") -> list[str]:
    """把长文本切成不超过 max_len 的分段。

    mode:
      - "length":   只按长度硬切
      - "paragraph": 只按空行分段
      - "sentence": 按句子结束符逐句分段（类似 AstrBot 的「分段回复」手感），
                    超长句再按长度切
      - "smart":    空行 > 换行 > 句子 > 硬切，并尽量合并到 max_len（默认）
    """
    text = (text or "").strip()
    if not text:
        return []
    if mode == "sentence":
        # 逐句切分，不再合并，接近「像人一样一条条发」
        parts = re.split(r"(?<=[。！？!?；;…~])\s*", text)
        out: list[str] = []
        for part in parts:
            seg = part.strip()
            if not seg:
                continue
            if max_len > 0 and len(seg) > max_len:
                out.extend(
                    seg[i : i + max_len].strip()
                    for i in range(0, len(seg), max_len)
                    if seg[i : i + max_len].strip()
                )
            else:
                out.append(seg)
        return out or [text]
    if max_len <= 0 or len(text) <= max_len:
        return [text]
    if mode == "length":
        return [text[i : i + max_len] for i in range(0, len(text), max_len)]
    if mode == "paragraph":
        chunks = [c for c in re.split(r"(\n\s*\n)", text)]
    else:
        chunks = [c for c in re.split(r"(\n\s*\n|\n|(?<=[。！？!?；;…]))", text)]

    # 2) 贪心合并小块，超过 max_len 就分段；单块超长再按长度硬切
    segments: list[str] = []
    cur = ""
    for chunk in chunks:
        if not chunk:
            continue
        if len(chunk) > max_len:
            # 先把当前累积的收尾
            if cur.strip():
                segments.append(cur.strip())
                cur = ""
            for i in range(0, len(chunk), max_len):
                piece = chunk[i : i + max_len]
                if len(piece) == max_len:
                    segments.append(piece.strip())
                else:
                    cur = piece
            continue
        if len(cur) + len(chunk) > max_len:
            if cur.strip():
                segments.append(cur.strip())
            cur = chunk
        else:
            cur += chunk
    if cur.strip():
        segments.append(cur.strip())
    return [s for s in segments if s]


class PhotonImsgPlugin(Star):
    def __init__(
        self, context: Context, config: AstrBotConfig | None = None
    ) -> None:
        super().__init__(context)
        self.config = config
        self.event_queue = context.get_event_queue()
        self.platform_meta = PlatformMetadata(
            name="photon_imessage",
            description="iMessage Adapter (Photon)",
            id="photon_imessage",
        )

        # sidecar 进程
        self._sidecar_proc: subprocess.Popen | None = None
        self._supervisor_task: asyncio.Task | None = None
        self._inbound_task: asyncio.Task | None = None
        self._http: aiohttp.ClientSession | None = None
        self._stop_event = asyncio.Event()

        # derived
        self._project_id = ""
        self._project_secret = ""
        self._sidecar_dir = os.path.join(os.path.dirname(__file__), "sidecar")
        self._sidecar_port = _DEFAULT_SIDECAR_PORT
        self._sidecar_token = ""
        self._node_bin = "node"
        self._running = False
        self._typing_tasks: dict[str, asyncio.Task] = {}
        self._stub_platform = None

        # 初始化向导使用的临时状态（device login / 选择的项目等）
        self._setup = {
            "project_name": "astrbot-imessage",
            "token": None,
            "dashboard_project_id": None,
            "project_secret": None,
            "phone": "",
            "assigned_line": None,
        }
        self._register_setup_apis()

    # ------------------------------------------------------------------
    # 配置读取 (兼容 config 注入与文件兜底)
    # ------------------------------------------------------------------
    def _cfg(self, key: str, default=None):
        # 优先读文件（初始化向导写入这里的持久值，如 assigned_line）
        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_config_path

            path = os.path.join(get_astrbot_config_path(), f"{_PLUGIN_NAME}_config.json")
            if os.path.exists(path):
                with open(path, encoding="utf-8-sig") as f:
                    saved = json.load(f)
                if saved.get(key) not in (None, ""):
                    return saved.get(key)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Photon] 读取配置文件失败: {e}")
        # 其次读注入的 AstrBotConfig（全局配置/默认值）
        if self.config is not None and key in self.config:
            return self.config.get(key)
        return default

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        # 先注册轻量平台实例，让其他插件的主动发送 / 平台查询能找到一个载体
        await self._register_stub_platform()

        # 接管 iMessage 的分段回复（覆盖 AstrBot 原生分段，避免两套叠加）
        if bool(self._cfg("override_astrbot_segment", True)):
            from . import photon_overrides

            photon_overrides.install()

        self._project_id = str(self._cfg("project_id") or "").strip()
        self._project_secret = str(self._cfg("project_secret") or "").strip()
        self._sidecar_port = int(self._cfg("sidecar_port", _DEFAULT_SIDECAR_PORT) or _DEFAULT_SIDECAR_PORT)
        self._sidecar_token = str(self._cfg("sidecar_token") or "").strip()
        node = str(self._cfg("node_bin") or "").strip()
        self._node_bin = node or shutil.which("node") or "node"
        auto_install = bool(self._cfg("auto_install_sidecar", True))

        if not self._project_id or not self._project_secret:
            logger.warning(
                "[Photon] 尚未配置 project_id / project_secret，请在插件配置中填写。"
            )
            return
        if not self._sidecar_token:
            import secrets

            self._sidecar_token = secrets.token_urlsafe(24)

        # 启动 sidecar（首次会 npm install，较长）
        try:
            await self._start_sidecar(auto_install=auto_install)
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Photon] sidecar 启动失败: {e}")
            return

        self._running = True
        # sidecar 就绪后，把平台实例状态置为 RUNNING，
        # 让第三方主动聊天插件（如 astrbot_plugin_proactive_chat）能直接走
        # send_by_session，而不是因状态非运行而放弃主动发送。
        if self._stub_platform is not None:
            try:
                await self._stub_platform._mark_running()
            except Exception as e:  # noqa: BLE001
                logger.debug(f"[Photon] 标记平台实例运行状态失败: {e}")
        self._http = aiohttp.ClientSession()
        self._inbound_task = asyncio.create_task(self._inbound_loop())
        logger.info("[Photon] 插件已运行，等待 iMessage ...")

    async def terminate(self) -> None:
        """插件卸载/停用时清理。"""
        self._running = False
        self._stop_event.set()
        for space_id in list(self._typing_tasks.keys()):
            await self._stop_typing_task(space_id)
        self._unregister_stub_platform()
        try:
            from . import photon_overrides

            photon_overrides.uninstall()
        except Exception:  # noqa: BLE001
            pass
        if self._inbound_task:
            self._inbound_task.cancel()
            try:
                await self._inbound_task
            except asyncio.CancelledError:
                pass
        if self._http and not self._http.closed:
            await self._http.close()
        if self._supervisor_task:
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except asyncio.CancelledError:
                pass
        await self._stop_sidecar()

    # ------------------------------------------------------------------
    # sidecar 管理
    # ------------------------------------------------------------------
    @staticmethod
    def _sidecar_deps_installed(sidecar_dir: str) -> bool:
        return os.path.exists(os.path.join(sidecar_dir, "node_modules", "spectrum-ts"))

    async def _start_sidecar(self, auto_install: bool) -> None:
        if not os.path.exists(os.path.join(self._sidecar_dir, "index.mjs")):
            raise RuntimeError(f"sidecar 目录缺失: {self._sidecar_dir}")

        # 清理遗留的 sidecar（例如上次 AstrBot 被强杀），否则端口被占用会启动失败
        await self._reap_stale_sidecar()

        if auto_install and not self._sidecar_deps_installed(self._sidecar_dir):
            logger.info("[Photon] 安装 sidecar 依赖 (npm install)，首次可能需要较长时间 ...")
            npm = shutil.which("npm")
            if not npm:
                raise RuntimeError("未找到 npm，无法安装 spectrum-ts。请安装 Node.js。")
            await asyncio.to_thread(
                self._run_npm_install, npm, self._sidecar_dir
            )
            if not self._sidecar_deps_installed(self._sidecar_dir):
                raise RuntimeError("npm install 后仍未找到 spectrum-ts，安装失败。")

        env = os.environ.copy()
        env.update({
            "PHOTON_PROJECT_ID": self._project_id,
            "PHOTON_PROJECT_SECRET": self._project_secret,
            "SIDECAR_PORT": str(self._sidecar_port),
            "SIDECAR_TOKEN": self._sidecar_token,
            "SIDECAR_BIND": "127.0.0.1",
        })

        try:
            self._sidecar_proc = subprocess.Popen(
                [self._node_bin, os.path.join(self._sidecar_dir, "index.mjs")],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=(sys.platform != "win32"),
                cwd=self._sidecar_dir,
            )
        except FileNotFoundError:
            raise RuntimeError(f"node 二进制未找到: {self._node_bin!r}")

        self._supervisor_task = asyncio.create_task(
            self._supervise_sidecar(self._sidecar_proc)
        )

        # 等待 /healthz 就绪
        deadline = time.time() + _SIDECAR_READY_TIMEOUT
        last_err: Exception | None = None
        while time.time() < deadline:
            if self._sidecar_proc.poll() is not None:
                raise RuntimeError(
                    f"sidecar 提前退出，退出码 {self._sidecar_proc.returncode}"
                )
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.post(
                        self._sidecar_url("/healthz"),
                        timeout=aiohttp.ClientTimeout(total=2),
                    ) as resp:
                        if resp.status == 200:
                            return
            except Exception as e:  # noqa: BLE001
                last_err = e
            await asyncio.sleep(0.25)
        raise RuntimeError(f"sidecar 20 秒内未就绪: {last_err}")

    async def _reap_stale_sidecar(self) -> None:
        """若端口已被本插件遗留的 sidecar 占用，先结束它。

        场景：上次 AstrBot 被强杀（SIGKILL），sidecar 未能随父进程退出。
        sidecar 自身有 stdin EOF 保护，这里再做一层兜底。
        """
        try:
            # 先用 /shutdown 礼貌关闭（如果是我们自己的 sidecar，它能响应）
            async with aiohttp.ClientSession() as s:
                try:
                    async with s.post(
                        self._sidecar_url("/shutdown"),
                        headers=self._sidecar_headers(),
                        timeout=aiohttp.ClientTimeout(total=2),
                    ):
                        pass
                    await asyncio.sleep(0.5)  # 等它释放端口
                except Exception:  # noqa: BLE001
                    pass  # 端口没人监听 / 不是我们的 sidecar

            # 再看端口是否仍被占用（非本插件进程则报警，避免误杀）
            if await self._port_in_use():
                logger.warning(
                    f"[Photon] 端口 {self._sidecar_port} 仍被占用，且不是本插件的 sidecar。"
                    "请在配置里换一个 sidecar_port。"
                )
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Photon] 清理遗留 sidecar 时出错: {e}")

    async def _port_in_use(self) -> bool:
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", self._sidecar_port), timeout=1.5
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass
            return True
        except Exception:  # noqa: BLE001
            return False

    @staticmethod
    def _run_npm_install(npm: str, cwd: str) -> None:
        """同步执行 npm install（在后台线程中调用）。"""
        try:
            subprocess.run(
                [npm, "install", "--no-audit", "--no-fund"],
                cwd=cwd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=600,
            )
        except subprocess.TimeoutExpired:
            logger.error("[Photon] npm install 超时。")

    async def _supervise_sidecar(self, proc: subprocess.Popen) -> None:
        """把 sidecar 的输出泵进日志，并在其意外退出时记录。"""
        if proc.stdout is None:
            return
        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(None, proc.stdout.readline)
            if not line:
                break
            logger.info("[photon-sidecar] %s", line.decode("utf-8", "replace").rstrip())
        if self._running:
            logger.error(
                f"[Photon] sidecar 意外退出 (code {proc.poll()})"
            )

    async def _stop_sidecar(self) -> None:
        proc = self._sidecar_proc
        self._sidecar_proc = None
        if proc is not None:
            try:
                if proc.stdin is not None:
                    try:
                        proc.stdin.close()
                    except Exception:  # noqa: BLE001
                        pass
                async with aiohttp.ClientSession() as s:
                    try:
                        async with s.post(
                            self._sidecar_url("/shutdown"),
                            headers=self._sidecar_headers(),
                            timeout=aiohttp.ClientTimeout(total=2),
                        ):
                            pass
                    except Exception:  # noqa: BLE001
                        pass
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self._kill_proc(proc)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[Photon] 关闭 sidecar 时出错: {e}")

    @staticmethod
    def _kill_proc(proc: subprocess.Popen) -> None:
        """强制结束 sidecar；POSIX 下连同其进程组一起结束。"""
        try:
            if sys.platform != "win32":
                import os as _os
                import signal as _signal

                try:
                    _os.killpg(_os.getpgid(proc.pid), _signal.SIGKILL)
                    return
                except Exception:  # noqa: BLE001
                    pass
            proc.kill()
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Photon] kill sidecar 失败: {e}")

    # ------------------------------------------------------------------
    # HTTP 辅助
    # ------------------------------------------------------------------
    def _sidecar_url(self, path: str) -> str:
        return f"http://127.0.0.1:{self._sidecar_port}{path}"

    def _sidecar_headers(self) -> dict:
        return {"X-Sidecar-Token": self._sidecar_token}

    async def _sidecar_post(self, path: str, payload: dict) -> dict:
        """向 sidecar 发送 POST。网络异常时返回 ok=False，不向上抛。"""
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession()
        try:
            async with self._http.post(
                self._sidecar_url(path),
                json=payload,
                headers=self._sidecar_headers(),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                text = await resp.text()
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    data = {"ok": False, "error": {"message": text}}
                if not data.get("ok"):
                    logger.warning(f"[Photon] sidecar {path} 错误: {data}")
                return data
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Photon] 调用 sidecar {path} 失败: {e}")
            return {"ok": False, "error": {"message": str(e)}}

    # ------------------------------------------------------------------
    # 入站消息: 读取 NDJSON 流
    # ------------------------------------------------------------------
    async def _inbound_loop(self) -> None:
        url = self._sidecar_url("/inbound")
        headers = self._sidecar_headers()
        backoff = 1.0
        while self._running:
            try:
                if self._http is None or self._http.closed:
                    self._http = aiohttp.ClientSession()
                async with self._http.get(
                    url, headers=headers, timeout=None
                ) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"/inbound 返回 {resp.status}")
                    backoff = 1.0
                    # 按块读取并自行切行：附件会被内联成较大的 base64，
                    # 单行可能远超 aiohttp 行读取上限（默认 64KB），
                    # 直接用 async for line 会抛 "Line too long" 而中断。
                    buf = b""
                    async for chunk in resp.content.iter_any():
                        buf += chunk
                        while b"\n" in buf:
                            raw, buf = buf.split(b"\n", 1)
                            line = raw.decode("utf-8", "replace").strip()
                            if not line:
                                continue  # heartbeat
                            await self._on_inbound_line(line)
                    # 流结束，处理残留
                    tail = buf.decode("utf-8", "replace").strip()
                    if tail:
                        await self._on_inbound_line(tail)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                if not self._running:
                    break
                logger.warning(
                    f"[Photon] 入站流断开 ({e})，{backoff:.1f}s 后重连"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _on_inbound_line(self, line: str) -> None:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            return
        # 只处理自己发来的入站文本/附件消息
        content = event.get("content") or {}
        ctype = content.get("type")
        if ctype != "text" and ctype != "attachment":
            return

        abm = await self._build_abm(event)
        if abm is None:
            return
        ev = PhotonPlatformEvent(
            message_str=abm.message_str,
            message_obj=abm,
            platform_meta=self.platform_meta,
            session_id=abm.session_id,
            plugin=self,
        )
        try:
            self.event_queue.put_nowait(ev)
        except asyncio.QueueFull:
            logger.warning("[Photon] 事件队列已满，丢弃一条消息。")

    async def _build_abm(self, event: dict) -> AstrBotMessage | None:
        """把 sidecar 的入站 JSON 事件转换为 AstrBotMessage。"""
        space = event.get("space") or {}
        space_id = space.get("id") or ""
        if not space_id:
            return None

        content = event.get("content") or {}
        ctype = content.get("type")
        sender = event.get("sender") or {}
        sender_id = sender.get("id") or ""
        message_id = event.get("messageId") or ""

        text = ""
        chain: list = []
        if ctype == "text":
            text = content.get("text", "") or ""
            if text:
                chain.append(Plain(text=text))
        elif ctype == "attachment":
            chain = self._attachment_components(content)
            text = self._attachment_summary(content)
            # 附件也要能唤醒 LLM（私聊会自动唤醒，这里给出占位文本）
            if not text:
                text = "[附件]"

        abm = AstrBotMessage()
        # iMessage 私聊自动唤醒 LLM（AstrBot waking 阶段对私聊默认唤醒）。
        abm.type = MessageType.FRIEND_MESSAGE
        # self_id 必须是**机器人自己**的标识（AstrBot 用它判断"是否自己发的消息"，
        # 见 waking_check: get_self_id() == get_sender_id()）。绝不能填 sender_id，
        # 否则开启 ignore_bot_self_message 时会丢弃全部消息。
        abm.self_id = str(self._cfg("assigned_line") or "")
        abm.session_id = space_id
        abm.message_id = message_id
        abm.message_str = text
        abm.sender = MessageMember(
            user_id=sender_id or space_id,
            nickname=sender_id or "unknown",
        )
        abm.message = chain
        abm.raw_message = event
        return abm

    @staticmethod
    def _attachment_components(content: dict) -> list:
        """把 sidecar 内联的附件(base64)转换成 AstrBot 消息组件。"""
        from astrbot.api.message_components import File, Image

        data = content.get("dataBase64")
        mime = (content.get("mimeType") or "").lower()
        name = content.get("name") or "attachment"
        if not data:
            return []
        if mime.startswith("image/"):
            try:
                return [Image.fromBase64(data)]
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[Photon] 解析图片附件失败: {e}")
                return []
        # 非图片：用 File 组件承载 base64（AstrBot 媒体工具可解析）
        try:
            return [File(name=name, file=f"base64://{data}")]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Photon] 解析文件附件失败: {e}")
            return []

    @staticmethod
    def _attachment_summary(content: dict) -> str:
        name = content.get("name")
        mime = content.get("mimeType")
        if mime and str(mime).startswith("image/"):
            return "[图片]"
        return f"[附件: {name}]" if name else "[附件]"

    # ------------------------------------------------------------------
    # 初始化向导 (Web API)
    # ------------------------------------------------------------------
    def _register_setup_apis(self) -> None:
        """注册 WebUI 设置向导的后端接口。"""
        ctx = self.context
        ctx.register_web_api(
            f"/{_PLUGIN_NAME}/setup/begin_login", self._api_begin_login, ["POST"],
            "Photon: 开始设备登录",
        )
        ctx.register_web_api(
            f"/{_PLUGIN_NAME}/setup/poll_login", self._api_poll_login, ["POST"],
            "Photon: 轮询登录状态",
        )
        ctx.register_web_api(
            f"/{_PLUGIN_NAME}/setup/create_project", self._api_create_project, ["POST"],
            "Photon: 创建/选择项目",
        )
        ctx.register_web_api(
            f"/{_PLUGIN_NAME}/setup/register_phone", self._api_register_phone, ["POST"],
            "Photon: 注册手机号用户",
        )
        ctx.register_web_api(
            f"/{_PLUGIN_NAME}/setup/apply_config", self._api_apply_config, ["POST"],
            "Photon: 写入插件配置",
        )

    async def _api_begin_login(self):
        """第一步：请求设备码，返回授权链接 + 用户码，并启动后台轮询任务。"""
        from astrbot.api.web import error_response, json_response

        from . import photon_setup

        # 若已有登录任务在跑，先取消，避免重复轮询
        old = self._setup.get("login_task")
        if old is not None and not old.done():
            old.cancel()

        try:
            code = await photon_setup.request_device_code()
        except photon_setup.PhotonSetupError as e:
            return error_response(str(e), status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.exception("[Photon] 请求设备码失败")
            return error_response(f"请求设备码失败: {e}", status_code=400)

        self._setup["device_code"] = code
        self._setup["login_state"] = "pending"
        self._setup["login_error"] = None
        self._setup["token"] = None
        # 后台任务负责完整轮询，前端只查状态（避免堆叠长轮询请求）
        self._setup["login_task"] = asyncio.create_task(self._login_worker(code))

        return json_response({
            "ok": True,
            "verification_uri": code.verification_uri_complete or code.verification_uri,
            "user_code": code.user_code,
            "interval": code.interval,
        })

    async def _login_worker(self, code) -> None:
        """后台完成设备登录轮询，把结果写入 self._setup。"""
        from . import photon_setup

        try:
            token = await photon_setup.poll_for_token(code)
            await photon_setup.validate_photon_token(token)
            self._setup["token"] = token
            self._setup["login_state"] = "ok"
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Photon] 设备登录失败: {e}")
            self._setup["login_state"] = "error"
            self._setup["login_error"] = str(e)

    async def _api_poll_login(self):
        """第二步：查询登录状态（不阻塞）。"""
        from astrbot.api.web import error_response, json_response

        state = self._setup.get("login_state")
        if state is None:
            return error_response("请先开始登录", status_code=400)
        if state == "ok":
            return json_response({"ok": True, "logged_in": True})
        if state == "error":
            return json_response({
                "ok": False,
                "logged_in": False,
                "state": "error",
                "error": self._setup.get("login_error") or "登录失败",
            })
        return json_response({"ok": True, "logged_in": False, "state": "pending"})

    async def _api_create_project(self):
        """第三步：创建独立项目（或按名字复用），存下 dashboard project id + secret。"""
        from astrbot.api.web import error_response, json_response

        from . import photon_setup

        token = self._setup.get("token")
        if not token:
            return error_response("请先登录", status_code=400)

        # 前端可传 name 覆盖默认项目名
        from astrbot.api.web import request

        req_json = await request.json(default={})
        name = (
            str(req_json.get("name") or "").strip()
            or self._setup.get("project_name")
            or photon_setup.DEFAULT_PROJECT_NAME
        )
        self._setup["project_name"] = name

        # 优先按 name 复用；否则新建。前端可传 want_new 强制新建。
        want_new = bool(req_json.get("want_new"))
        existing = None
        try:
            if not want_new:
                existing = await photon_setup.find_project_by_name(token, name)
            if existing:
                project = existing
            else:
                project = await photon_setup.create_project(token, name=name)
        except photon_setup.PhotonSetupError as e:
            return error_response(str(e), status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.exception("[Photon] 创建/查询项目失败")
            return error_response(f"创建/查询项目失败: {e}", status_code=400)

        project_id = project.get("id")
        if not project_id:
            return error_response("项目缺少 id", status_code=400)

        # 拿 Spectrum secret
        try:
            secret = await photon_setup.regenerate_project_secret(token, project_id)
        except photon_setup.PhotonSetupError as e:
            return error_response(str(e), status_code=400)

        self._setup["dashboard_project_id"] = project_id
        self._setup["project_secret"] = secret
        created = existing is None
        return json_response({
            "ok": True,
            "project_id": project_id,
            "project_name": project.get("name"),
            "created": created,
        })

    async def _api_register_phone(self):
        """第四步：注册 iMessage 手机号用户，尝试取独立 line。"""
        from astrbot.api.web import error_response, json_response

        from . import photon_setup

        token = self._setup.get("token")
        project_id = self._setup.get("dashboard_project_id")
        secret = self._setup.get("project_secret")
        if not (token and project_id and secret):
            return error_response("请先完成登录与项目创建", status_code=400)

        from astrbot.api.web import request

        req_json = await request.json(default={})
        phone = str(req_json.get("phone") or "").strip()
        first_name = str(req_json.get("first_name") or "").strip() or None
        if not phone:
            return error_response("请填写你的 iMessage 手机号 (E.164)", status_code=400)

        try:
            user, created = await photon_setup.register_user_if_absent(
                project_id, secret, phone=phone, first_name=first_name
            )
        except photon_setup.PhotonSetupError as e:
            return error_response(str(e), status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.exception("[Photon] 注册手机号失败 (未预期异常)")
            return error_response(f"注册手机号失败: {e}", status_code=400)

        assigned_line = photon_setup.user_assigned_line(user)
        if not assigned_line:
            try:
                line = await photon_setup.get_imessage_line(
                    token, project_id, create_if_missing=True
                )
                if line:
                    assigned_line = line.get("phoneNumber")
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[Photon] 获取 line 失败: {e}")

        self._setup["phone"] = phone
        self._setup["assigned_line"] = assigned_line
        return json_response({
            "ok": True,
            "phone": phone,
            "assigned_line": assigned_line,
            "created": created,
        })

    async def _api_apply_config(self):
        """第五步：把 project_id/project_secret 写进插件配置。"""
        from astrbot.api.web import error_response, json_response

        project_id = self._setup.get("dashboard_project_id")
        secret = self._setup.get("project_secret")
        if not (project_id and secret):
            return error_response("缺少 project_id / project_secret", status_code=400)

        try:
            updates = {
                "project_id": project_id,
                "project_secret": secret,
            }
            if self._setup.get("phone"):
                updates["phone"] = self._setup.get("phone")
            if self._setup.get("assigned_line"):
                updates["assigned_line"] = self._setup.get("assigned_line")
            self._set_plugin_config(updates)
        except Exception as e:  # noqa: BLE001
            return error_response(f"写入配置失败: {e}", status_code=500)

        return json_response({
            "ok": True,
            "message": "已写入插件配置，请重载插件使其生效。",
        })

    def _set_plugin_config(self, updates: dict) -> None:
        """写入插件配置文件 (data/config/<name>_config.json)。"""
        from astrbot.core.utils.astrbot_path import get_astrbot_config_path

        path = os.path.join(get_astrbot_config_path(), f"{_PLUGIN_NAME}_config.json")
        data = {}
        if os.path.exists(path):
            with open(path, encoding="utf-8-sig") as f:
                data = json.load(f)
        data.update(updates)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # ------------------------------------------------------------------
    # 状态指令
    # ------------------------------------------------------------------
    @filter.on_llm_request()
    async def _inject_imessage_persona(self, event: AstrMessageEvent, req):
        """为 iMessage 单独指定人格：开启时，复用 AstrBot 已有的人格，
        把对应 persona 的 system prompt 追加到 LLM 请求（不覆盖其他必要 prompt）。"""
        try:
            if event.get_platform_name() != "photon_imessage":
                return
            if not bool(self._cfg("imessage_persona_enable")):
                return
            persona_id = str(self._cfg("imessage_persona_id") or "").strip()
            if not persona_id:
                return
            persona = self.context.persona_manager.get_persona_v3_by_id(persona_id)
            if not persona:
                logger.warning(f"[Photon] 未找到 persona_id={persona_id!r}，跳过人格注入。")
                return
            prompt = (persona.get("prompt") or "").strip()
            if prompt:
                req.system_prompt = f"{req.system_prompt or ''}\n# Persona Instructions\n\n{prompt}\n"
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Photon] 注入 iMessage 人格失败: {e}")

    @filter.command("photonstatus")
    async def photon_status(self, event: AstrMessageEvent):
        """查看 Photon iMessage 插件当前配置状态。"""
        pid = self._cfg("project_id") or "（未配置）"
        line = self._cfg("assigned_line") or ""
        phone = self._cfg("phone") or ""
        running = "运行中" if self._running else "未运行"
        project_secret_ok = "已配置" if self._cfg("project_secret") else "未配置"
        # 主动发送载体（平台实例）是否就绪
        stub = getattr(self, "_stub_platform", None)
        if stub is None:
            proactive = "未注册（主动发送不可用）"
        else:
            try:
                proactive = (
                    "就绪 (RUNNING)"
                    if getattr(stub, "status", None) and getattr(stub, "status").value == "running"
                    else "已注册但未运行"
                )
            except Exception:  # noqa: BLE001
                proactive = "已注册（状态未知）"
        parts = [
            f"✨ Photon iMessage 状态",
            f"项目 ID: {pid}",
            f"项目密钥: {project_secret_ok}",
            f"你的 iMessage: {phone or '未填'}",
            f"分配给机器人的号码 (TEXTS ON): {line or '未查询到，请看 Photon Dashboard'}",
            f"运行状态: {running}",
            f"Sidecar 连接: {'是' if self._sidecar_proc and self._sidecar_proc.poll() is None else '否'}",
            f"主动发送载体: {proactive}",
        ]
        yield event.plain_result("\n".join(parts))

    @filter.command("photonpush")
    async def photon_push(self, event: AstrMessageEvent, text: GreedyStr) -> None:
        """主动发送自检：用 AstrBot 的主动发送 API 给当前会话推一条消息。

        这条指令走的就是「定时任务」「第三方主动聊天插件」的同一条路：
        ``context.send_message(umo, chain)`` → 在
        ``platform_manager.platform_insts`` 里找 ``meta().id == 会话平台名``
        的实例 → ``PhotonPlatformStub.send_by_session()`` → sidecar ``/send``。

        因此它的输出能直接把故障段定位出来：
        - 平台实例显示「未注册」→ AstrBot 根本没找到 photon_imessage 载体；
        - ``context.send_message`` 返回 False → 同上，消息在核心 API 就被丢弃；
        - 两者都正常却收不到消息 → 问题在 sidecar / Spectrum 发送侧。
        """
        umo = event.unified_msg_origin
        payload = str(text or "").strip() or "（自检）这是一条主动消息。"

        lines = ["✨ 主动发送自检", f"会话: {umo}"]

        # 1) 平台实例表里有没有 photon_imessage（主动发送的唯一入口）
        try:
            insts = list(
                getattr(self.context.platform_manager, "platform_insts", []) or []
            )
        except Exception as e:  # noqa: BLE001
            insts = []
            lines.append(f"❌ 读取 platform_insts 失败: {e}")
        found: list[str] = []
        for p in insts:
            try:
                meta = p.meta()
            except Exception:  # noqa: BLE001
                continue
            if meta.id != "photon_imessage":
                continue
            status = getattr(p.status, "value", p.status)
            tag = "(本插件)" if p is self._stub_platform else "(他人注册)"
            found.append(f"{meta.id}={status}{tag}")
        lines.append(
            "平台实例: " + ("; ".join(found) if found else "❌ 未注册（主动发送不可用）")
        )
        lines.append(f"实例表总数: {len(insts)}")

        # 2) 真正跑一次核心主动发送 API
        try:
            from astrbot.api.event import MessageChain

            ok = await self.context.send_message(umo, MessageChain([Plain(payload)]))
            lines.append(f"context.send_message -> {ok}")
            if not ok:
                lines.append("❌ AstrBot 未匹配到平台，消息止步于核心 API。")
        except Exception as e:  # noqa: BLE001
            lines.append(f"❌ context.send_message 异常: {e}")

        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # 轻量平台实例（让主动发送 / 平台实例查询生效）
    # ------------------------------------------------------------------
    @filter.on_platform_loaded()
    async def _on_platform_loaded(self) -> None:
        """平台适配器（重新）加载后，确保主动发送载体仍在实例表里。

        AstrBot 的 ``PlatformManager.terminate()`` 会执行
        ``platform_insts.clear()``（见 core/platform/manager.py）。只在
        ``initialize()`` 里注册一次的 stub 会被这样抹掉，而被动回复不查这张表，
        于是表现为「能回消息、但主动消息静默失败」。这里在每次平台加载后补注册。
        """
        await self._register_stub_platform()

    async def _register_stub_platform(self) -> None:
        """把 PhotonPlatformStub 加入 platform_manager.platform_insts。

        AstrBot 的 context.send_message() 依赖该列表按 meta().id 匹配平台实例，
        没有它时其他插件的主动发送会失败（cannot find platform for session）。

        注意：本方法现在含 await，必须在 async 上下文调用（如 initialize）。
        Enrollment 时机不影响发送匹配；但会把 status 置为 RUNNING，
        以兼容第三方主动聊天插件对平台状态的检查。
        """
        try:
            pm = getattr(self.context, "platform_manager", None)
            insts = getattr(pm, "platform_insts", None)
            if insts is None:
                logger.warning("[Photon] 无法访问 platform_insts，主动发送可能不可用。")
                return
            # 避免热重载时重复注册；若已存在则把引用指向当前插件实例
            for p in list(insts):
                try:
                    if p.meta().id != "photon_imessage":
                        continue
                except Exception:  # noqa: BLE001
                    continue
                from .photon_platform_stub import PhotonPlatformStub

                if isinstance(p, PhotonPlatformStub):
                    # 热重载：复用实例但更新插件引用，避免指向已失效的旧插件对象
                    p._plugin = self
                    self._stub_platform = p
                    if self._running:
                        await p._mark_running()
                    logger.info("[Photon] 已复用既有平台实例（主动发送已启用）。")
                    return
                # 不是我们的实例（id 冲突），移除后重建
                try:
                    insts.remove(p)
                except ValueError:
                    pass
            from .photon_platform_stub import PhotonPlatformStub

            stub = PhotonPlatformStub(self)
            insts.append(stub)
            self._stub_platform = stub
            if self._running:
                await stub._mark_running()
            logger.info("[Photon] 已注册平台实例，主动发送已启用。")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Photon] 注册平台实例失败（主动发送可能不可用）: {e}")

    def _unregister_stub_platform(self) -> None:
        try:
            stub = self._stub_platform
            self._stub_platform = None
            if stub is None:
                return
            insts = getattr(self.context.platform_manager, "platform_insts", None)
            if insts and stub in insts:
                insts.remove(stub)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Photon] 注销平台实例失败: {e}")

    # ------------------------------------------------------------------
    # 对外发送（供 PhotonPlatformEvent 与主动发送共用）
    # ------------------------------------------------------------------
    async def send_chain(self, space_id: str, chain) -> None:
        """统一发送入口：文本按配置分段，媒体走附件接口。

        同时供两类调用方使用：
        - ``PhotonPlatformEvent.send()``（被动回复，传 ``message.chain`` 列表）
        - ``PhotonPlatformStub.send_by_session()``（其他插件主动推送，传 ``MessageChain``）

        因此这里兼容 ``MessageChain`` 对象与组件列表两种入参
        （``MessageChain`` 本身不可迭代，必须取 ``.chain``）。
        """
        if not space_id:
            return
        comps = getattr(chain, "chain", chain) or []

        # 注意：这里**不能**合并相邻的 Plain 组件。
        # AstrBot 的「分段回复」功能(platform_settings.segmented_reply)会把一段
        # 文本拆成多个 Plain 放进同一条链，若在此合并会破坏它的分段效果。
        for comp in comps:
            if isinstance(comp, Plain):
                if comp.text:
                    await self.send_segmented(space_id, comp.text)
            elif isinstance(comp, (Image, File, Record, Video)):
                await self.send_media(space_id, comp)
            else:
                # 其余组件(iMessage 不原生支持)尽量用文本兜底
                text = getattr(comp, "text", None) or getattr(comp, "url", None)
                if text:
                    await self.send_segmented(space_id, str(text))

    async def send_media(self, space_id: str, comp) -> None:
        """把媒体组件（图片/文件/语音/视频）通过 sidecar 作为附件发送。

        表情包插件通常发送 Image 组件，走的就是这里。
        """
        try:
            file_path = await comp.convert_to_file_path()
        except Exception as e:  # noqa: BLE001
            logger.error(
                f"[Photon] 无法解析媒体文件 {getattr(comp, 'file', None)}: {e}"
            )
            return
        kind = "voice" if isinstance(comp, Record) else "attachment"
        result = await self._sidecar_post(
            "/send-attachment",
            {"spaceId": space_id, "path": file_path, "kind": kind},
        )
        if not result.get("ok"):
            logger.error(f"[Photon] 发送媒体失败: {result}")

    async def send_text(self, space_id: str, text: str) -> bool:
        result = await self._sidecar_post(
            "/send", {"spaceId": space_id, "text": text, "format": "text"}
        )
        return bool(result.get("ok"))

    async def send_segmented(self, space_id: str, text: str) -> None:
        """按配置把长文本分段发送。"""
        enabled = bool(self._cfg("segment_enable", True))
        max_len = int(self._cfg("segment_max_length", 500) or 500)
        delay = float(self._cfg("segment_delay", 0.8) or 0)
        mode = str(self._cfg("segment_split_mode") or "sentence")
        segments = split_text(text, max_len, mode) if enabled else [text]
        for idx, seg in enumerate(segments):
            if not seg:
                continue
            await self.send_text(space_id, seg)
            if delay > 0 and idx < len(segments) - 1:
                await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # 「正在输入」状态
    # ------------------------------------------------------------------
    async def set_typing(self, space_id: str, state: str) -> None:
        """通知 sidecar 发送 iMessage 输入状态 start/stop。"""
        if not space_id:
            return
        await self._sidecar_post("/typing", {"spaceId": space_id, "state": state})

    async def start_typing(self, space_id: str) -> None:
        """开始「正在输入」，并启动保活任务（iMessage 气泡会超时消失）。"""
        if not bool(self._cfg("typing_enable", True)) or not space_id:
            return
        await self._stop_typing_task(space_id)
        await self.set_typing(space_id, "start")
        keepalive = int(self._cfg("typing_keepalive_sec", 8) or 0)
        if keepalive > 0:
            self._typing_tasks[space_id] = asyncio.create_task(
                self._typing_keepalive(space_id, keepalive)
            )

    async def _typing_keepalive(self, space_id: str, interval: int) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                await self.set_typing(space_id, "start")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Photon] typing 保活结束: {e}")

    async def stop_typing(self, space_id: str) -> None:
        """结束「正在输入」。"""
        if not space_id:
            return
        await self._stop_typing_task(space_id)
        await self.set_typing(space_id, "stop")

    async def _stop_typing_task(self, space_id: str) -> None:
        task = self._typing_tasks.pop(space_id, None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass