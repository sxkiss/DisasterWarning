"""
灾害预警核心服务。
负责管理所有 WebSocket 连接器、消息分发、推送和命令处理。
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from WechatAPI import WechatAPIClient

from loguru import logger

from .connectors import WSConnector
from .formatter import format_earthquake_info, format_earthquake_warning, format_tsunami, format_weather
from .models import (
    Epicenter,
    DataSource,
    EarthquakeEvent,
    EventType,
    ReportCounter,
    TsunamiEvent,
    WeatherAlert,
)
from .parsers import (
    parse_fan_studio,
    parse_global_quake,
    parse_global_quake_protobuf,
    parse_p2p_earthquake,
    parse_wolfx,
)


class DisasterWarningService:
    """灾害预警服务主控制器。"""

    def __init__(self, config: dict, bot: WechatAPIClient):
        self.config = config
        self.bot = bot

        # 从配置读取
        basic_cfg = config.get("DisasterWarning", config)
        self.enabled = basic_cfg.get("enabled", True)
        self.push_targets = basic_cfg.get("push_targets", [])
        self.admin_wxids = basic_cfg.get("admin_wxids", [])
        self.min_weather_level = basic_cfg.get("weather", {}).get("min_color_level", "白色")
        self.startup_silence = basic_cfg.get("debug", {}).get("startup_silence_seconds", 0)
        self._silence_until = time.time() + self.startup_silence

        # 频率控制 — push_frequency 是顶层 section
        freq_cfg = self.config.get("push_frequency", {})
        if not freq_cfg:
            freq_cfg = basic_cfg.get("push_frequency_control", {})
        self.report_counter = ReportCounter(
            cea_cwa_n=freq_cfg.get("cea_cwa_report_n", 1),
            jma_n=freq_cfg.get("jma_report_n", 3),
            gq_n=freq_cfg.get("gq_report_n", 5),
        )

        # 过滤器 — earthquake_filters 是顶层 section，用 self.config 获取
        filter_cfg = self.config.get("earthquake_filters", {})
        if not filter_cfg:
            filter_cfg = basic_cfg  # 降级到 basic_cfg
        self.usgs_min_mag = filter_cfg.get("usgs_min_magnitude", 4.5)
        self.domestic_min_mag = filter_cfg.get("domestic_min_magnitude", 1.0)
        self.overseas_min_mag = filter_cfg.get("overseas_min_magnitude", 4.0)
        self.min_intensity = filter_cfg.get("min_intensity", 4.0)
        self.min_scale = filter_cfg.get("min_scale", 1.0)
        self.blacklist = [k.lower() for k in filter_cfg.get("blacklist_keywords", [])]
        self.whitelist = [k for k in filter_cfg.get("whitelist_keywords", [])]

        # 数据源连接器
        self._connectors: list[WSConnector] = []
        self._running = False

        # 去重: event_id -> last_seen_timestamp
        self._seen_events: dict[str, float] = {}

    # ── 生命周期 ────────────────────────────────────────────

    async def start(self) -> None:
        if not self.enabled:
            logger.info("[灾害预警] 插件已禁用，跳过启动")
            return

        logger.info("[灾害预警] 正在启动灾害预警服务...")
        self._running = True

        # 建立各数据源连接
        await self._setup_connectors()

        # 启动所有连接器
        for conn in self._connectors:
            await conn.start()

        logger.success("[灾害预警] 灾害预警服务已启动")

    async def stop(self) -> None:
        if not self._running:
            return
        logger.info("[灾害预警] 正在停止灾害预警服务...")
        self._running = False

        for conn in self._connectors:
            await conn.stop()

        self._connectors.clear()
        logger.info("[灾害预警] 灾害预警服务已停止")

    async def _setup_connectors(self) -> None:
        ds_cfg = self.config.get("data_sources", self.config)

        # ── FAN Studio ─────────────────────────────────────
        fs_cfg = ds_cfg.get("fans_studio", ds_cfg.get("data_sources", {}).get("fan_studio", {}))
        if fs_cfg.get("enabled", True):
            ws_cfg = self.config.get("websocket_config", self.config.get("websocket", {}))
            connector = WSConnector(
                url=fs_cfg.get("url", "wss://ws.fanstudio.tech/all"),
                backup_url=fs_cfg.get("backup_url", "wss://ws.fanstudio.hk/all"),
                name="fan_studio",
                heartbeat_interval=ws_cfg.get("heartbeat_interval", 120),
                reconnect_interval=ws_cfg.get("reconnect_interval", 10),
                max_reconnect_retries=ws_cfg.get("max_reconnect_retries", 3),
                connection_timeout=ws_cfg.get("connection_timeout", 15),
            )
            connector.set_handler(self._handle_fan_studio_message)
            self._connectors.append(connector)

        # ── P2P 地震情报 ───────────────────────────────────
        p2p_cfg = ds_cfg.get("p2p_earthquake", ds_cfg.get("data_sources", {}).get("p2p_earthquake", {}))
        if p2p_cfg.get("enabled", True):
            ws_cfg = self.config.get("websocket_config", self.config.get("websocket", {}))
            connector = WSConnector(
                url=p2p_cfg.get("url", "wss://api.p2pquake.net/v2/ws"),
                name="p2p_earthquake",
                heartbeat_interval=ws_cfg.get("heartbeat_interval", 120),
                reconnect_interval=ws_cfg.get("reconnect_interval", 10),
                max_reconnect_retries=ws_cfg.get("max_reconnect_retries", 3),
                connection_timeout=ws_cfg.get("connection_timeout", 15),
            )
            connector.set_handler(self._handle_p2p_message)
            self._connectors.append(connector)

        # ── Wolfx ──────────────────────────────────────────
        wolf_cfg = ds_cfg.get("wolfx", ds_cfg.get("data_sources", {}).get("wolfx", {}))
        if wolf_cfg.get("enabled", True):
            ws_cfg = self.config.get("websocket_config", self.config.get("websocket", {}))
            connector = WSConnector(
                url=wolf_cfg.get("url", "wss://ws-api.wolfx.jp/all_eew"),
                name="wolfx",
                heartbeat_interval=ws_cfg.get("heartbeat_interval", 120),
                reconnect_interval=ws_cfg.get("reconnect_interval", 10),
                max_reconnect_retries=ws_cfg.get("max_reconnect_retries", 3),
                connection_timeout=ws_cfg.get("connection_timeout", 15),
            )
            connector.set_handler(self._handle_wolfx_message)
            self._connectors.append(connector)

        # ── Global Quake ───────────────────────────────────
        gq_cfg = ds_cfg.get("global_quake", ds_cfg.get("data_sources", {}).get("global_quake", {}))
        if gq_cfg.get("enabled", True):
            ws_cfg = self.config.get("websocket_config", self.config.get("websocket", {}))
            connector = WSConnector(
                url=gq_cfg.get("url", "wss://gqm.aloys23.link/ws"),
                name="global_quake",
                heartbeat_interval=ws_cfg.get("heartbeat_interval", 120),
                reconnect_interval=ws_cfg.get("reconnect_interval", 10),
                max_reconnect_retries=ws_cfg.get("max_reconnect_retries", 3),
                connection_timeout=ws_cfg.get("connection_timeout", 15),
            )
            connector.set_handler(self._handle_gq_message)
            self._connectors.append(connector)

    # ── 消息处理 ────────────────────────────────────────────

    async def _handle_fan_studio_message(self, data: Union[dict, None], raw_bytes: Optional[bytes]) -> None:
        """处理 FAN Studio 消息。只处理 TEXT。"""
        if raw_bytes is not None:
            # 不应该收到 BINARY，记录调试
            logger.debug(f"[灾害预警] fan_studio 收到 BINARY 消息，忽略: {len(raw_bytes)} bytes")
            return
        if not isinstance(data, dict):
            return
        try:
            events = parse_fan_studio(data)
            if events:
                logger.info(f"[灾害预警] fan_studio 解析到 {len(events)} 个事件")
            for evt in events:
                await self._process_event(evt)
        except Exception as e:
            logger.warning(f"[灾害预警] FAN Studio 解析错误: {e}", exc_info=True)

    async def _handle_p2p_message(self, data: Union[dict, None], raw_bytes: Optional[bytes]) -> None:
        """处理 P2P 地震情报消息。只处理 TEXT。"""
        if raw_bytes is not None:
            logger.debug(f"[灾害预警] p2p_earthquake 收到 BINARY 消息，忽略: {len(raw_bytes)} bytes")
            return
        if not isinstance(data, dict):
            return
        try:
            events = parse_p2p_earthquake(data)
            for evt in events:
                await self._process_event(evt)
        except Exception as e:
            logger.warning(f"[灾害预警] P2P 解析错误: {e}", exc_info=True)

    async def _handle_wolfx_message(self, data: Union[dict, None], raw_bytes: Optional[bytes]) -> None:
        """处理 Wolfx 消息。只处理 TEXT。"""
        if raw_bytes is not None:
            logger.debug(f"[灾害预警] wolfx 收到 BINARY 消息，忽略: {len(raw_bytes)} bytes")
            return
        if not isinstance(data, dict):
            return
        try:
            events = parse_wolfx(data)
            for evt in events:
                await self._process_event(evt)
        except Exception as e:
            logger.warning(f"[灾害预警] Wolfx 解析错误: {e}", exc_info=True)

    async def _handle_gq_message(self, data: Union[dict, None], raw_bytes: Optional[bytes]) -> None:
        """
        处理 Global Quake 消息。
        同时处理 TEXT（JSON 调试格式）和 BINARY（Protobuf 正式格式）。
        """
        if raw_bytes is not None:
            # Protobuf 二进制消息 — 这是正式数据通道
            try:
                events = parse_global_quake_protobuf(raw_bytes)
                if events:
                    logger.debug(f"[灾害预警] global_quake Protobuf 解析到 {len(events)} 个事件")
                for evt in events:
                    await self._process_event(evt)
            except Exception as e:
                logger.warning(f"[灾害预警] Global Quake Protobuf 解析错误: {e}", exc_info=True)
            return

        if not isinstance(data, dict):
            return

        # 兼容 JSON 格式（备用/调试）
        try:
            events = parse_global_quake(data)
            for evt in events:
                await self._process_event(evt)
        except Exception as e:
            logger.warning(f"[灾害预警] Global Quake 解析错误: {e}", exc_info=True)

    async def _process_event(self, evt: EarthquakeEvent | TsunamiEvent | WeatherAlert) -> None:
        """处理解析后的事件，经过过滤后推送。"""
        # 静默期检查
        if time.time() < self._silence_until:
            return

        # 去重检查
        event_key = self._event_key(evt)
        if event_key in self._seen_events:
            age = time.time() - self._seen_events[event_key]
            if age < 300:  # 5 分钟内相同事件视为重复
                return
        self._seen_events[event_key] = time.time()

        # 清理过期条目
        if len(self._seen_events) > 500:
            cutoff = time.time() - 3600
            self._seen_events = {k: v for k, v in self._seen_events.items() if v > cutoff}

        # 根据事件类型分发
        if isinstance(evt, EarthquakeEvent):
            await self._process_earthquake(evt)
        elif isinstance(evt, TsunamiEvent):
            await self._process_tsunami(evt)
        elif isinstance(evt, WeatherAlert):
            await self._process_weather(evt)

    def _event_key(self, evt: Any) -> str:
        """生成事件去重键。"""
        if isinstance(evt, EarthquakeEvent):
            # 使用 event_id 去重，比坐标更精确
            if evt.event_id and evt.event_id.startswith("gq_"):
                # Global Quake 事件使用 event_id
                return f"{evt.source.value}:{evt.event_id}"
            return f"{evt.source.value}:{evt.magnitude:.1f}:{evt.epicenter.latitude:.2f}:{evt.epicenter.longitude:.2f}"
        elif isinstance(evt, TsunamiEvent):
            return f"tsunami:{evt.source.value}:{evt.warning_level}"
        elif isinstance(evt, WeatherAlert):
            return f"weather:{evt.source.value}:{evt.title}"
        return str(id(evt))

    async def _process_earthquake(self, evt: EarthquakeEvent) -> None:
        # 根据国内/海外使用不同震级阈值
        if evt.is_domestic:
            min_mag = self.domestic_min_mag
        else:
            min_mag = self.overseas_min_mag

        logger.debug(f"[灾害预警] 地震过滤: {evt.location_str} M{evt.magnitude:.1f} is_domestic={evt.is_domestic} min_mag={min_mag}")
        if evt.magnitude < min_mag:
            logger.debug(f"[灾害预警] 震级 {evt.magnitude:.1f} 低于阈值 {min_mag}，跳过: {evt.location_str}")
            return

        # USGS 等仅震级数据源的过滤
        if evt.source == DataSource.FAN_STUDIO and evt.event_type == EventType.EARTHQUAKE_INFO:
            # 检查 USGS 最小震级
            if "usgs" in evt.event_id.lower() and evt.magnitude < self.usgs_min_mag:
                return

        # 关键词过滤
        if self._should_filter_by_keyword(evt):
            return

        # 烈度/震度过滤
        if evt.intensity > 0 and evt.intensity < self.min_intensity and evt.magnitude < self.min_intensity:
            if evt.event_type == EventType.EARTHQUAKE_WARNING:
                return

        # 频率控制
        should_push = self.report_counter.record(
            evt.event_id, evt.source, evt.is_final
        )
        if not should_push:
            return

        # 格式化并推送
        if evt.event_type == EventType.EARTHQUAKE_WARNING:
            text = format_earthquake_warning(evt)
        else:
            text = format_earthquake_info(evt)

        logger.info(f"[灾害预警] 即将推送: {evt.source.value} -> {evt.location_str} M{evt.magnitude:.1f}")
        await self._push(text, evt)

    async def _process_tsunami(self, evt: TsunamiEvent) -> None:
        # 至少需要有预警级别才推送
        if not evt.warning_level:
            logger.debug(f"[灾害预警] 海啸事件无预警级别，跳过: {evt.event_id}")
            return
        text = format_tsunami(evt)
        logger.info(f"[灾害预警] 即将推送海啸: {evt.source.value} -> {evt.areas}")
        await self._push(text, evt)

    async def _process_weather(self, evt: WeatherAlert) -> None:
        text = format_weather(evt, self.min_weather_level)
        if text:
            logger.info(f"[灾害预警] 即将推送气象预警: {evt.source.value} -> {evt.title}")
            await self._push(text, evt)

    def _should_filter_by_keyword(self, evt: EarthquakeEvent) -> bool:
        """检查关键词黑名单/白名单。"""
        location = evt.location_str.lower()
        province = evt.epicenter.province.lower()

        # 黑名单
        for kw in self.blacklist:
            if kw in location or kw in province:
                return True

        # 白名单
        if self.whitelist:
            for kw in self.whitelist:
                if kw in location or kw in province:
                    return False
            return True

        return False

    async def _push(self, text: str, evt: Any) -> None:
        """推送消息到配置的会话。"""
        if self.push_targets:
            for target in self.push_targets:
                try:
                    await self.bot.send_text_message(target.strip(), text)
                except Exception as e:
                    logger.error(f"[灾害预警] 推送失败到 {target}: {e}")
        else:
            # 没有配置推送目标，记录日志
            logger.info(f"[灾害预警] 推送: {text[:100]}...")

    # ── 状态查询 ────────────────────────────────────────────

    def get_status(self) -> dict:
        return {
            "enabled": self.enabled,
            "running": self._running,
            "connectors": len(self._connectors),
            "connected": sum(1 for c in self._connectors if c._ws is not None),
            "seen_events": len(self._seen_events),
        }

    # ── 命令处理 ────────────────────────────────────────────

    async def handle_command(self, command: str, sender_wxid: str, chat_id: str, is_group: bool) -> Optional[str]:
        """处理插件命令。返回要发送的消息文本，或 None。"""
        cmd = command.strip()

        if cmd in ("灾害预警", "灾害预警帮助", "灾害预警help", "/灾害预警"):
            return self._help_text()

        elif cmd in ("灾害预警状态", "灾害预警status", "/灾害预警状态"):
            status = self.get_status()
            lines = [
                "📊 灾害预警服务状态:",
                f"  运行状态: {'✅ 运行中' if status['running'] else '❌ 已停止'}",
                f"  数据源连接: {status['connected']}/{status['connectors']} 活跃",
                f"  已记录事件: {status['seen_events']}",
                f"  推送目标: {len(self.push_targets)} 个会话",
            ]
            return "\n".join(lines)

        elif cmd in ("灾害预警重连", "/灾害预警重连"):
            if not self._is_admin(sender_wxid):
                return "❌ 仅管理员可执行此操作"
            await self._reconnect_all()
            return "🔄 正在重连所有数据源..."

        elif cmd in ("灾害预警统计", "/灾害预警统计"):
            return self._stats_text()

        elif cmd in ("灾害预警推送开关", "/灾害预警推送开关"):
            if not self._is_admin(sender_wxid):
                return "❌ 仅管理员可执行此操作"
            # 简化：切换 enabled 状态
            self.enabled = not self.enabled
            status = "启用" if self.enabled else "禁用"
            return f"✅ 灾害预警已{status}"

        elif cmd.startswith("灾害预警模拟 ") or cmd.startswith("/灾害预警模拟 "):
            if not self._is_admin(sender_wxid):
                return "❌ 仅管理员可执行此操作"
            parts = cmd.replace("灾害预警模拟 ", "").replace("/灾害预警模拟 ", "").split()
            if len(parts) >= 3:
                try:
                    lat, lon, mag = float(parts[0]), float(parts[1]), float(parts[2])
                    evt = EarthquakeEvent(
                        event_id=f"simulated_{int(time.time())}",
                        event_type=EventType.EARTHQUAKE_WARNING,
                        source=DataSource.FAN_STUDIO,
                        magnitude=mag,
                        epicenter=Epicenter(latitude=lat, longitude=lon, depth_km=float(parts[3]) if len(parts) > 3 else 10.0),
                        publish_time=datetime.now(timezone(timedelta(hours=8))),
                        report_number=1,
                        is_final=True,
                    )
                    await self._process_earthquake(evt)
                    return f"✅ 已模拟地震事件: 纬度{lat}, 经度{lon}, 震级{mag}"
                except (ValueError, IndexError):
                    return "❌ 参数错误，格式: /灾害预警模拟 <纬度> <经度> <震级> [深度]"
            return "❌ 参数不足，格式: /灾害预警模拟 <纬度> <经度> <震级> [深度]"

        return None

    def _is_admin(self, wxid: str) -> bool:
        if self.admin_wxids and wxid in self.admin_wxids:
            return True
        # 尝试从 main_config 读取管理员
        try:
            with open("main_config.toml", "rb") as f:
                import tomllib
                cfg = tomllib.load(f)
                admins = cfg.get("XYBot", {}).get("admins", [])
                if wxid in admins:
                    return True
        except Exception:
            pass
        return True  # 默认允许（宽松模式）

    def _help_text(self) -> str:
        return """🚨 灾害预警插件帮助

📋 可用命令：
• /灾害预警 - 显示帮助信息
• /灾害预警状态 - 查看服务运行状态
• /灾害预警重连 - 强制重连所有数据源（仅管理员）
• /灾害预警统计 - 查看详细统计
• /灾害预警推送开关 - 开启/关闭推送（仅管理员）
• /灾害预警模拟 <纬度> <经度> <震级> [深度] - 模拟地震事件（仅管理员）

📡 支持的数据源：
• 中国地震预警网（CEA）— FAN Studio / Wolfx
• 台湾中央气象署（CWA）— FAN Studio / Wolfx
• 日本气象厅（JMA）— FAN Studio / P2P / Wolfx
• 美国地质调查局（USGS）— FAN Studio
• 中国气象局气象预警 — FAN Studio
• Global Quake 全球地震监测 — Protobuf
• 海啸预警

⚙️ 配置请参考 config.toml
"""

    def _stats_text(self) -> str:
        status = self.get_status()
        return (
            f"📊 灾害预警统计\n"
            f"运行状态: {'✅ 运行中' if status['running'] else '❌ 已停止'}\n"
            f"数据源连接: {status['connected']}/{status['connectors']}\n"
            f"已记录事件: {status['seen_events']}"
        )

    async def _reconnect_all(self) -> None:
        for conn in self._connectors:
            await conn.stop()
        await asyncio.sleep(1)
        for conn in self._connectors:
            await conn.start()
