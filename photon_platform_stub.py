"""轻量平台实例（stub），让 AstrBot 的主动发送等 API 对 photon_imessage 生效。

背景：本插件是普通 Star 插件（自己管理 Node sidecar），并未通过平台适配器注册。
但 AstrBot 的 ``Context.send_message()`` / ``get_platform_inst()`` 都依赖
``platform_manager.platform_insts`` 中存在匹配 ``meta().id`` 的实例：

    for platform in self.platform_manager.platform_insts:
        if platform.meta().id == session.platform_name:
            await platform.send_by_session(session, message_chain)
            return True
    logger.warning("cannot find platform for session ...")

没有实例时，其他插件（如表情包/定时推送）调用主动发送就会失败。
因此这里注册一个最小实现：它不负责接收（接收与 sidecar 由插件管理），
只把 ``send_by_session`` 转发给插件的统一发送逻辑。
"""
from __future__ import annotations

import asyncio

from astrbot.api.event import MessageChain
from astrbot.api.platform import Platform, PlatformMetadata

# MessageSesion（历史拼写）在部分版本位于 astr_message_event，部分位于 message_session；
# 兜底尝试，避免平台主动发送时因导入失败而静默失效。
try:  # pragma: no cover - 取决于 AstrBot 版本
    from astrbot.core.platform.astr_message_event import (
        MessageSesion,
        MessageSession,
    )
except ImportError:  # pragma: no cover
    try:
        from astrbot.core.platform.message_session import (
            MessageSesion,
            MessageSession,
        )
    except ImportError:
        MessageSesion = MessageSession = None  # type: ignore[assignment, misc]

from astrbot.core.platform.platform import PlatformStatus


class PhotonPlatformStub(Platform):
    """仅用于承载主动发送的最小平台实例。"""

    def __init__(self, plugin) -> None:
        # 复用插件持有的事件队列；config 仅用于标识
        super().__init__(
            {"id": "photon_imessage", "type": "photon_imessage"},
            plugin.event_queue,
        )
        self._plugin = plugin

    def meta(self) -> PlatformMetadata:
        return PlatformMetadata(
            name="photon_imessage",
            description="iMessage via Photon (Star 插件)",
            id="photon_imessage",
            support_streaming_message=False,
            support_proactive_message=True,
        )

    async def run(self) -> None:
        """本实例不由平台管理器启动（sidecar 由插件管理），保持挂起即可。"""
        await asyncio.Event().wait()

    async def _mark_running(self) -> None:
        """把自身状态置为 RUNNING。

        AstrBot 的主动发送框架（Context.send_message 依赖 platform_insts 按
        meta().id 匹配，不检查状态）本身不受状态影响；但第三方“主动聊天”插件
        （如 astrbot_plugin_proactive_chat）在 send_by_session 之前会检查
        platform.status == RUNNING，不满足时可能直接放弃或走兜底路径。
        因此插件在 sidecar 就绪后显式置为运行中，避免主动消息静默失败。
        """
        try:
            self.status = PlatformStatus.RUNNING
        except Exception:  # noqa: BLE001
            pass

    async def send_by_session(
        self,
        session: MessageSesion,
        message_chain: MessageChain,
    ) -> None:
        """主动发送：把消息链交给插件的统一发送逻辑（文本分段 + 媒体附件）。"""
        await self._plugin.send_chain(session.session_id, message_chain)
        await super().send_by_session(session, message_chain)

    async def terminate(self) -> None:
        return