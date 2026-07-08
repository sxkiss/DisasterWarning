"""
灾害预警插件入口。
适配 allbot 的 PluginBase 框架。
"""
from __future__ import annotations

import os
import tomllib
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from WechatAPI import WechatAPIClient

from loguru import logger


from utils.decorators import on_text_message
from utils.plugin_base import PluginBase

from .service import DisasterWarningService


class DisasterWarning(PluginBase):
    """多数据源灾害预警插件 - 地震/海啸/气象预警实时推送"""

    description = "多数据源灾害预警插件，支持地震、海啸、气象预警实时推送"
    author = "DBJD-CR (adapted by sxkiss)"
    version = "1.0.0"
    priority = 90  # 高优先级，尽早拦截命令

    def __init__(self):
        super().__init__()
        self.service: Optional[DisasterWarningService] = None
        self._loaded_config: dict = {}

    async def on_enable(self, bot: WechatAPIClient):
        """插件启用时加载配置并启动服务。"""
        try:
            # 加载插件配置
            config_path = os.path.join(os.path.dirname(__file__), "config.toml")
            if os.path.exists(config_path):
                with open(config_path, "rb") as f:
                    self._loaded_config = tomllib.load(f)
            else:
                self._loaded_config = {"DisasterWarning": {"enabled": True}}

            # 检查总开关
            if not self._loaded_config.get("DisasterWarning", {}).get("enabled", True):
                logger.info("[灾害预警] 插件已禁用")
                return

            # 创建服务
            self.service = DisasterWarningService(self._loaded_config, bot)
            await self.service.start()
            logger.success("[灾害预警] 插件已加载并启动")

        except Exception as e:
            logger.error(f"[灾害预警] 插件启动失败: {e}")
            import traceback
            traceback.print_exc()

    async def on_disable(self):
        """插件禁用时停止服务。"""
        if self.service:
            await self.service.stop()
            logger.info("[灾害预警] 插件已停止")

    async def async_init(self):
        pass

    @on_text_message(priority=90)
    async def handle_command(self, bot: WechatAPIClient, message: dict):
        """
        拦截文本消息，匹配灾害预警命令。
        返回 False 表示消费该消息（不再传递给其他插件）。
        返回 True 表示不处理，让其他插件继续处理。
        返回 None 表示不改变消息流。
        """
        if not self.service:
            return True

        content = message.get("Content", "").strip()
        sender_wxid = message.get("SenderWxid", "")
        from_wxid = message.get("FromWxid", "")
        is_group = from_wxid.endswith("chatroom")

        # 群聊中需要 @机器人 或包含唤醒词
        if is_group:
            # 检查是否包含唤醒词
            has_trigger = False
            for word in ("灾害预警", "disaster"):
                if word in content.lower():
                    has_trigger = True
                    break

            if not has_trigger:
                return True

            # 仅去除 @机器人 前缀，保留完整命令
            if content.startswith("@"):
                at_end = content.find(chr(10))
                if at_end == -1:
                    at_end = content.find(" ")
                if at_end != -1:
                    content = content[at_end:].strip()

        # 直接匹配命令（私聊不需要唤醒词）
        if not is_group or content:
            result = await self.service.handle_command(
                content, sender_wxid, from_wxid, is_group
            )
            if result is not None:
                # 发送回复
                at_list = [sender_wxid] if is_group else None
                try:
                    if is_group and at_list:
                        await bot.send_at_message(from_wxid, result, at_list)
                    else:
                        await bot.send_text_message(from_wxid, result)
                except Exception as e:
                    logger.error(f"[灾害预警] 发送消息失败: {e}")
                return False  # 消费消息

        return True
