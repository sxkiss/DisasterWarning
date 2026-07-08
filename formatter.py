"""
灾害预警消息格式化。
将事件模型转换为适合微信推送的文本消息。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from .models import (
    DataSource,
    EarthquakeEvent,
    EventType,
    TsunamiEvent,
    WeatherAlert,
)


# ── 震级描述 ──────────────────────────────────────────────

def _magnitude_desc(mag: float) -> str:
    if mag < 1:
        return f"M<{1}"
    elif mag < 3:
        return f"M{mag:.1f}（微震）"
    elif mag < 4.5:
        return f"M{mag:.1f}（有感地震）"
    elif mag < 6:
        return f"M{mag:.1f}（中强震）"
    elif mag < 7:
        return f"M{mag:.1f}（强震）"
    elif mag < 8:
        return f"M{mag:.1f}（大震）"
    else:
        return f"M{mag:.1f}（巨大地震）"


def _intensity_desc(intensity: float) -> str:
    if intensity <= 0:
        return ""
    return f"烈度{intensity:.1f}"


def _scale_desc(scale: float) -> str:
    if scale <= 0:
        return ""
    return f"震度{scale:.0f}"


def _depth_desc(depth: float) -> str:
    if depth <= 0:
        return ""
    if depth < 100:
        return f"深度{depth:.0f}km（浅源）"
    elif depth < 300:
        return f"深度{depth:.0f}km（中源）"
    else:
        return f"深度{depth:.0f}km（深源）"


# ── 地震预警消息 ──────────────────────────────────────────

def format_earthquake_warning(evt: EarthquakeEvent) -> str:
    """格式化地震预警消息。"""
    lines = []
    emoji = "🚨" if evt.event_type == EventType.EARTHQUAKE_WARNING else "📋"

    lines.append(f"{emoji} 【灾害预警】")
    lines.append("")

    # 标题
    if evt.event_type == EventType.EARTHQUAKE_WARNING:
        lines.append("⚡ 地震预警")
    else:
        lines.append("📊 地震信息")

    lines.append("")

    # 基本信息
    lines.append(f"🕐 时间: {evt.timestamp}")
    loc = evt.location_str if evt.location_str else f"坐标 {evt.epicenter.latitude:.4f}, {evt.epicenter.longitude:.4f}"
    lines.append(f"📍 地点: {loc}")
    lines.append(f"💥 震级: {_magnitude_desc(evt.magnitude)}")

    if evt.epicenter.depth_km > 0:
        lines.append(f"🔽 {_depth_desc(evt.epicenter.depth_km)}")

    if evt.intensity > 0:
        lines.append(f"🌊 烈度: {_intensity_desc(evt.intensity)}")

    if evt.scale > 0:
        lines.append(f"🇯🇵 震度: {_scale_desc(evt.scale)}")

    if evt.report_number > 0:
        lines.append(f"📝 报次: 第{evt.report_number}报")

    if evt.is_final:
        lines.append("✅ 最终报")

    # 坐标
    if evt.epicenter.latitude != 0 or evt.epicenter.longitude != 0:
        lines.append(f"🌐 坐标: {evt.epicenter.latitude:.4f}°, {evt.epicenter.longitude:.4f}°")

    # 数据源
    source_names = {
        DataSource.FAN_STUDIO: "FAN Studio",
        DataSource.P2P_EARTHQUAKE: "P2P地震情报",
        DataSource.WOLFX: "Wolfx",
        DataSource.GLOBAL_QUAKE: "Global Quake",
    }
    lines.append(f"📡 来源: {source_names.get(evt.source, str(evt.source))}")

    return "\n".join(lines)


def format_earthquake_info(evt: EarthquakeEvent) -> str:
    """格式化地震测定/报告消息。"""
    return format_earthquake_warning(evt)


# ── 海啸预警消息 ──────────────────────────────────────────

def format_tsunami(evt: TsunamiEvent) -> str:
    lines = []
    lines.append("🌊 【海啸预警】")
    lines.append("")
    lines.append(f"🕐 时间: {evt.timestamp}")

    if evt.warning_level:
        lines.append(f"⚠️ 级别: {evt.warning_level}")

    if evt.areas:
        lines.append(f"📍 影响区域: {', '.join(evt.areas[:10])}")

    source_names = {
        DataSource.FAN_STUDIO: "FAN Studio",
        DataSource.P2P_EARTHQUAKE: "P2P地震情报",
        DataSource.WOLFX: "Wolfx",
    }
    lines.append(f"📡 来源: {source_names.get(evt.source, str(evt.source))}")

    return "\n".join(lines)


# ── 气象预警消息 ──────────────────────────────────────────

WEATHER_LEVEL_ORDER = ["白色", "蓝色", "黄色", "橙色", "红色"]

def _level_rank(level: str) -> int:
    if level in WEATHER_LEVEL_ORDER:
        return WEATHER_LEVEL_ORDER.index(level)
    return 0


def format_weather(alert: WeatherAlert, min_level: str = "白色") -> Optional[str]:
    """格式化气象预警消息。如果级别低于 min_level 则返回 None。"""
    if _level_rank(alert.level) < _level_rank(min_level):
        return None

    lines = []
    lines.append("⛈️ 【气象预警】")
    lines.append("")

    if alert.title:
        lines.append(f"📌 标题: {alert.title}")

    lines.append(f"🕐 发布时间: {alert.timestamp}")

    if alert.level:
        lines.append(f"🎨 级别: {alert.level}")

    if alert.areas:
        lines.append(f"📍 发布区域: {', '.join(alert.areas[:10])}")

    if alert.description:
        desc = alert.description
        if len(desc) > 512:
            desc = desc[:512] + "..."
        lines.append(f"📝 详情: {desc}")

    source_names = {
        DataSource.FAN_STUDIO: "FAN Studio",
    }
    lines.append(f"📡 来源: {source_names.get(alert.source, str(alert.source))}")

    return "\n".join(lines)
