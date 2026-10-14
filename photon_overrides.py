"""覆盖 AstrBot 原生的「分段回复」，改由本插件接管 iMessage 的分段。

背景
----
AstrBot 的 ``platform_settings.segmented_reply`` 会在 ``ResultDecorateStage`` 里
把一段文本拆成多个 ``Plain`` 组件（硬编码只排除 qq_official_webhook /
weixin_official_account / dingtalk 三个平台）。因此 ``photon_imessage`` 也会被
AstrBot 分段，与本插件的分段逻辑叠加，导致分段效果不受控。

做法
----
在 ``ResultDecorateStage.process`` 外面包一层：**仅当事件来自 photon_imessage 时**，
临时把该实例的 ``enable_segmented_reply`` 置为 False，让 AstrBot 不再拆分，
从而完全由本插件的 ``segment_*`` 配置决定分段行为。

其它平台走原始逻辑，完全不受影响。插件卸载时会还原原始方法。

该改动是防御性的：若 AstrBot 后续版本改了内部结构，patch 会安全失败并回退，
不影响插件其它功能。
"""
from __future__ import annotations

from astrbot import logger

_PLATFORM_NAME = "photon_imessage"
_orig_process = None
_patched = False


def install() -> bool:
    """安装覆盖。返回是否成功。多次调用安全（幂等）。"""
    global _orig_process, _patched
    if _patched:
        return True
    try:
        from astrbot.core.pipeline.result_decorate.stage import ResultDecorateStage

        _orig_process = ResultDecorateStage.process

        async def _patched_process(self, event):
            # 判断是否本插件的平台；异常时按"不是"处理，走原始逻辑
            try:
                is_target = event.get_platform_name() == _PLATFORM_NAME
            except Exception:  # noqa: BLE001
                is_target = False

            if not is_target:
                async for item in _orig_process(self, event):
                    yield item
                return

            # 仅对 iMessage 临时关闭 AstrBot 的分段，交由本插件接管
            saved = getattr(self, "enable_segmented_reply", None)
            try:
                self.enable_segmented_reply = False
                async for item in _orig_process(self, event):
                    yield item
            finally:
                if saved is not None:
                    self.enable_segmented_reply = saved

        ResultDecorateStage.process = _patched_process
        _patched = True
        logger.info("[Photon] 已接管 iMessage 的分段回复（覆盖 AstrBot 原生分段）。")
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[Photon] 覆盖 AstrBot 分段回复失败（将使用其原生行为）: {e}")
        return False


def uninstall() -> None:
    """还原 AstrBot 原始方法。"""
    global _orig_process, _patched
    if not _patched or _orig_process is None:
        return
    try:
        from astrbot.core.pipeline.result_decorate.stage import ResultDecorateStage

        ResultDecorateStage.process = _orig_process
        logger.info("[Photon] 已还原 AstrBot 的分段回复逻辑。")
    except Exception as e:  # noqa: BLE001
        logger.debug(f"[Photon] 还原分段覆盖失败: {e}")
    finally:
        _orig_process = None
        _patched = False