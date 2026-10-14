"""Photon 入站消息事件 (AstrMessageEvent 子类)。

当 AstrBot 需要回复时，本类把 AstrBot 生成的消息链转换为对 sidecar 的
``POST /send`` / ``POST /send-attachment`` 调用，从而通过 Spectrum 发送到 iMessage。
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from astrbot import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import File, Image, Plain, Record, Video
from astrbot.api.platform import AstrBotMessage, PlatformMetadata

if TYPE_CHECKING:
    from .main import PhotonImsgPlugin


class PhotonPlatformEvent(AstrMessageEvent):
    def __init__(
        self,
        message_str: str,
        message_obj: AstrBotMessage,
        platform_meta: PlatformMetadata,
        session_id: str,
        plugin: "PhotonImsgPlugin",
    ) -> None:
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self.plugin = plugin
        # sidecar 事件里的 space.id 就是会话标识，直接作为回复目标
        self.space_id: str = session_id

    async def _resolve_target(self) -> str:
        return self.space_id or self.session_id or ""

    async def send(self, message: MessageChain) -> None:
        """把 AstrBot 生成的消息链发送到 iMessage。"""
        target = await self._resolve_target()
        if not target:
            return
        # 统一发送逻辑（文本分段 + 媒体附件），与主动发送共用同一套实现
        await self.plugin.send_chain(target, message.chain)
        await super().send(message)

    async def send_typing(self) -> None:
        """AstrBot 在请求 LLM 前调用：显示 iMessage「正在输入」。"""
        try:
            target = await self._resolve_target()
            await self.plugin.start_typing(target)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Photon] send_typing 失败: {e}")

    async def stop_typing(self) -> None:
        """AstrBot 在回复结束后调用：结束「正在输入」。"""
        try:
            target = await self._resolve_target()
            await self.plugin.stop_typing(target)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Photon] stop_typing 失败: {e}")

    async def send_streaming(self, generator, use_fallback: bool = False) -> None:
        """iMessage 不支持真正的流式输出，这里做「累积后一次性发送」。

        AstrBot 在 ``unsupported_streaming_strategy = realtime_segmenting``（默认值）
        时，会把 LLM 的**每个增量片段（streaming_delta）**交给本方法。若逐片发送，
        一条正常回复会被拆成很多条 iMessage —— 表现为「不该分段的也分段了」。

        因此这里累积完整文本，最后按分段配置统一发送；只有收到 AstrBot 的
        ``type == "break"`` 分段信号（例如工具调用前后的分界）才切成新的一条。
        """
        target = await self._resolve_target()
        if not target:
            return

        buf = ""
        media: list = []

        async def _flush() -> None:
            nonlocal buf, media
            text, buf = buf, ""
            comps, media = media, []
            if text.strip():
                await self.plugin.send_segmented(target, text)
            for comp in comps:
                await self.plugin.send_media(target, comp)

        async for chain in generator:
            ctype = getattr(chain, "type", None)
            if ctype == "break":
                # AstrBot 要求在此处结束当前消息，后续内容另起一条
                await _flush()
                continue
            if ctype == "reasoning":
                continue  # 思考过程不发给用户
            for comp in getattr(chain, "chain", None) or []:
                if isinstance(comp, Plain):
                    piece = comp.text or ""
                    if not piece:
                        continue
                    # 同时兼容「增量片段」与「全量快照」两种上游形态：
                    # 若新片段以已累积内容开头，说明它是全量快照 → 替换；
                    # 否则视为增量 → 追加。
                    if buf and piece.startswith(buf):
                        buf = piece
                    else:
                        buf += piece
                elif isinstance(comp, (Image, File, Record, Video)):
                    media.append(comp)
                else:
                    text = getattr(comp, "text", None) or getattr(comp, "url", None)
                    if text:
                        buf += str(text)

        await _flush()
        await super().send_streaming(generator, use_fallback)