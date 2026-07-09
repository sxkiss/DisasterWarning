"""
数据源消息解析器。将各数据源的原始消息转换为统一的事件模型。
支持 FAN Studio（JSON）、P2P 地震情报、Wolfx、Global Quake（Protobuf + JSON）。
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from loguru import logger

from .models import (
    DataSource,
    EarthquakeEvent,
    Epicenter,
    EventType,
    TsunamiEvent,
    WeatherAlert,
)

try:
    from .websocket_message_pb2 import WsMessage, MessageType, MessageAction
    HAS_PROTOBUF = True
except ImportError:
    HAS_PROTOBUF = False


# ── 通用工具函数 ──────────────────────────────────────────

def _parse_time(raw: Any, tz_offset_hours: int = 8, assume_local: bool = True) -> Optional[datetime]:
    """尝试解析多种时间格式。

    Args:
        raw: 原始时间字符串
        tz_offset_hours: 目标时区偏移（小时）
        assume_local: 如果时间没有时区信息，是否假设已是本地时间（UTC+8）。
                      FAN Studio/Wolfx/P2P 的时间都是北京时间，应为 True。
                      Global Quake 的 ISO 时间自带时区，不受此参数影响。
    """
    if raw is None:
        return None
    s = str(raw)
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y%m%d%H%M%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                # 没有时间信息，假设已是本地时间（UTC+8），直接设置时区
                if assume_local:
                    dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
                else:
                    dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone(timedelta(hours=tz_offset_hours)))
        except (ValueError, TypeError):
            continue
    return None


def _parse_intensity(data: dict) -> tuple[float, float, float]:
    """提取震级和烈度/震度。"""
    mag = 0.0
    intensity = 0.0
    scale = 0.0

    # 震级
    for key in ("mag", "magnitude", "Mw", "Ml", "mg", "ml", "M", "magnitudeValue", "magValue"):
        if key in data:
            try:
                mag = float(data[key])
                break
            except (ValueError, TypeError):
                pass

    # 烈度（中国标准）
    for key in ("intensity", "epiIntensity", "intensityValue", "烈度", "震度"):
        if key in data:
            try:
                intensity = float(data[key])
                break
            except (ValueError, TypeError):
                pass

    # 震度（日本标准）
    for key in ("scale", "jmaScale", "震度", "maxScale"):
        if key in data:
            try:
                scale = float(data[key])
                break
            except (ValueError, TypeError):
                pass

    return mag, intensity, scale


def _parse_epicenter(data: dict, default_lat: float = 0, default_lon: float = 0) -> Epicenter:
    """从原始数据中提取震中信息。"""
    lat = default_lat
    lon = default_lon

    for lat_key in ("lat", "latitude", "epiLat", "epicenterLat", "震央纬度", "震央緯度", "epiLatitude"):
        if lat_key in data:
            try:
                lat = float(data[lat_key])
                break
            except (ValueError, TypeError):
                pass

    for lon_key in ("lon", "longitude", "epiLon", "epicenterLon", "震央经度", "震央經度", "epiLongitude"):
        if lon_key in data:
            try:
                lon = float(data[lon_key])
                break
            except (ValueError, TypeError):
                pass

    depth = 0.0
    for d_key in ("depth", "epiDepth", "震源深度", "depthKm"):
        if d_key in data:
            try:
                depth = float(data[d_key])
                break
            except (ValueError, TypeError):
                pass

    # 地点名称
    location = ""
    for loc_key in (
        "location", "locationName", "epiLocation", "address",
        "震央", "震中", "地点", "city", "area", "placeName",
    ):
        if loc_key in data:
            location = str(data[loc_key])
            break

    province = ""
    city = ""
    if location:
        for sep in ("/", " ", "｜", "|"):
            parts = location.split(sep)
            if len(parts) >= 2:
                province = parts[0].strip()
                city = parts[1].strip()
                break
        if not province:
            province = location

    return Epicenter(
        latitude=lat, longitude=lon, depth_km=depth,
        location_name=location, province=province, city=city,
    )


def _unwrap_nested_payload(data: dict, max_depth: int = 3) -> dict:
    """提取嵌套的 data/Data 层，避免外层包装干扰。"""
    msg_data: Any = data
    depth = 0
    while (
        isinstance(msg_data, dict)
        and ("Data" in msg_data or "data" in msg_data)
        and depth < max_depth
    ):
        msg_data = msg_data.get("Data") or msg_data.get("data")
        depth += 1
    if isinstance(msg_data, dict):
        return msg_data
    return data


# ── FAN Studio 解析器（增强版） ────────────────────────────

def parse_fan_studio(data: dict) -> list:
    """
    解析 FAN Studio WebSocket 消息。
    支持 initial_all（全量初始化）、update（增量更新）、以及带 Data 包装的消息。
    """
    results = []
    msg_type = str(data.get("type", "")).strip()
    payload = _unwrap_nested_payload(data)

    # 处理 initial_all：一条消息包含多个来源的初始化快照
    if msg_type == "initial_all":
        # initial_all 用于建立初始状态，不产生推送事件
        # 只记录日志，不返回任何事件
        logger.debug(f"[灾害预警] fan_studio 收到 initial_all 初始化快照，跳过推送")
        return results  # 直接返回空列表

    # 处理 update：增量更新消息
    if msg_type == "update":
        source_name = str(data.get("source", "")).strip()
        # 优先用 source 字段识别数据来源
        if source_name:
            evt = _build_earthquake_from(payload, DataSource.FAN_STUDIO, source_name)
            if evt:
                results.append(evt)
            else:
                # 尝试海啸
                evt = _build_tsunami_from(payload, DataSource.FAN_STUDIO, source_name)
                if evt:
                    results.append(evt)
                else:
                    # 尝试气象
                    evt = _build_weather_from(payload, DataSource.FAN_STUDIO, source_name)
                    if evt:
                        results.append(evt)
        return results

    # 处理心跳/保活消息
    if msg_type in ("heartbeat", "ping", "pong"):
        return results

    # 传统 type 匹配（向后兼容）
    _parse_fan_studio_by_type(data, results)

    return results


def _parse_fan_studio_by_type(data: dict, results: list) -> None:
    """按 msg_type 分类解析 FAN Studio 消息（向后兼容）。"""
    msg_type = data.get("type", data.get("msgType", data.get("message_type", "")))
    payload = _unwrap_nested_payload(data)

    # 地震预警
    if msg_type in ("cenc_eew", "cea", "cwa-eew", "jma", "jma_eew", "sc_eew", "fj_eew", "cwa_eew"):
        evt = _build_earthquake_from(payload, DataSource.FAN_STUDIO, msg_type)
        if evt:
            results.append(evt)

    # 地震报告/测定
    elif msg_type in ("cenc", "cenc_eqlist", "usgs", "cwa", "jma_eqlist"):
        evt = _build_earthquake_from(payload, DataSource.FAN_STUDIO, msg_type)
        if evt:
            evt.event_type = EventType.EARTHQUAKE_INFO
            results.append(evt)

    # 海啸
    elif msg_type in ("tsunami", "jma_tsunami"):
        evt = _build_tsunami_from(payload, DataSource.FAN_STUDIO, msg_type)
        if evt:
            results.append(evt)

    # 气象预警
    elif msg_type in ("weatheralert", "weather_alarm", "weather"):
        evt = _build_weather_from(payload, DataSource.FAN_STUDIO, msg_type)
        if evt:
            results.append(evt)


# ── P2P 地震情报解析器 ───────────────────────────────────

def parse_p2p_earthquake(data: dict) -> list:
    """解析 P2P 地震情报 WebSocket 消息。"""
    results = []
    code = str(data.get("code", data.get("type", ""))).strip()

    if code in ("556", "ee"):
        evt = _build_earthquake_from(data, DataSource.P2P_EARTHQUAKE, "jma_eew")
        if evt:
            results.append(evt)
    elif code in ("551", "eq"):
        evt = _build_earthquake_from(data, DataSource.P2P_EARTHQUAKE, "jma_info")
        if evt:
            evt.event_type = EventType.EARTHQUAKE_INFO
            results.append(evt)
    elif code in ("552", "ts"):
        evt = _build_tsunami_from(data, DataSource.P2P_EARTHQUAKE, "jma_tsunami")
        if evt:
            results.append(evt)

    return results


# ── Wolfx 解析器 ─────────────────────────────────────────

def parse_wolfx(data: dict) -> list:
    """解析 Wolfx WebSocket 消息。"""
    results = []
    msg_type = str(data.get("type", data.get("msgType", ""))).strip()

    if msg_type in ("jma_eew", "cenc_eew", "sc_eew", "fj_eew", "cwa_eew"):
        evt = _build_earthquake_from(data, DataSource.WOLFX, msg_type)
        if evt:
            results.append(evt)
    elif msg_type in ("jma_eqlist", "cenc_eqlist"):
        evt = _build_earthquake_from(data, DataSource.WOLFX, msg_type)
        if evt:
            evt.event_type = EventType.EARTHQUAKE_INFO
            results.append(evt)
    elif msg_type in ("jma_tsunami",):
        evt = _build_tsunami_from(data, DataSource.WOLFX, msg_type)
        if evt:
            results.append(evt)

    return results


# ── Global Quake 解析器 ──────────────────────────────────

def parse_global_quake(data: dict) -> list:
    """
    解析 Global Quake WebSocket 消息（JSON 格式，用于调试/兼容）。
    注意：实际数据通常是 BINARY Protobuf，由 parse_global_quake_protobuf 处理。
    """
    results = []
    eq = _build_earthquake_from(data, DataSource.GLOBAL_QUAKE, "gq")
    if eq:
        results.append(eq)
    return results


def parse_global_quake_protobuf(raw_bytes: bytes) -> list:
    """
    解析 Global Quake 的 Protobuf 二进制消息。
    这是修复 global_quake 数据被静默丢弃的关键函数。
    """
    if not HAS_PROTOBUF:
        logger.warning("[灾害预警] Global Quake: protobuf 未安装，跳过二进制消息解析")
        return []

    try:
        ws_msg = WsMessage()
        ws_msg.ParseFromString(raw_bytes)
    except Exception as e:
        logger.debug(f"[灾害预警] Global Quake Protobuf 解码失败: {e}")
        return []

    # 心跳消息，忽略
    if ws_msg.type == MessageType.HEARTBEAT:
        return []

    # 状态消息，忽略
    if ws_msg.type == MessageType.STATUS:
        logger.debug(f"[灾害预警] Global Quake 状态消息: {ws_msg.status_data.server_status}")
        return []

    # 取消/撤销消息
    if ws_msg.type == MessageType.EARTHQUAKE and ws_msg.action == MessageAction.CANCELLED:
        logger.info(f"[灾害预警] Global Quake 地震取消: ID={ws_msg.earthquake_removal_data.id}")
        return []

    # 地震消息
    if ws_msg.type == MessageType.EARTHQUAKE:
        return _parse_gq_protobuf_earthquake(ws_msg)

    # 未知类型，记录调试信息
    logger.debug(f"[灾害预警] Global Quake 收到未知消息类型: {ws_msg.type}")
    return []


def _parse_gq_protobuf_earthquake(ws_msg: WsMessage) -> list:
    """从 Protobuf 消息构建 EarthquakeEvent。"""
    try:
        eq = ws_msg.earthquake_data
        if not eq:
            return []

        # 震级
        magnitude = round(eq.magnitude, 1) if eq.magnitude else 0.0
        if magnitude < 0.5:
            return []

        # 时间
        shock_time = None
        if eq.origin_time_iso:
            shock_time = _parse_time(eq.origin_time_iso, tz_offset_hours=0)
        elif eq.origin_time_ms:
            shock_time = datetime.fromtimestamp(eq.origin_time_ms / 1000, tz=timezone.utc)

        # 烈度
        intensity = eq.intensity  # 罗马数字或字符串

        # 地点翻译
        region = eq.region or "未知地点"

        # 报告编号
        report_num = 1
        if eq.revision_id:
            try:
                report_num = int(eq.revision_id)
            except (ValueError, TypeError):
                report_num = 1
        if report_num <= 0:
            report_num = 1

        # 是否最终报
        is_final = ws_msg.action == MessageAction.ARCHIVED

        event_id = str(eq.id or f"gq_{int(time.time())}")

        evt = EarthquakeEvent(
            event_id=event_id,
            event_type=EventType.EARTHQUAKE_WARNING,
            source=DataSource.GLOBAL_QUAKE,
            magnitude=magnitude,
            epicenter=Epicenter(
                latitude=eq.latitude,
                longitude=eq.longitude,
                depth_km=round(eq.depth, 1) if eq.depth else 0.0,
                location_name=region,
            ),
            intensity=0.0,  # Protobuf 使用字符串烈度
            scale=0.0,
            publish_time=shock_time,
            raw_data={"protobuf": True, "id": eq.id, "region": region},
            report_number=report_num,
            is_final=is_final,
            is_domestic=_is_chinese_location(region),
        )
        return [evt]
    except Exception as e:
        logger.error(f"[灾害预警] Global Quake Protobuf 地震解析失败: {e}")
        return []


# ── 通用构建函数 ─────────────────────────────────────────

def _is_chinese_location(text: str) -> bool:
    """判断文本是否包含中文地名关键词。"""
    if not text:
        return False
    domestic_keywords = [
        "北京", "天津", "上海", "重庆", "河北", "山西", "辽宁", "吉林", "黑龙江",
        "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南",
        "广东", "海南", "四川", "贵州", "云南", "陕西", "甘肃", "青海", "台湾",
        "内蒙古", "广西", "西藏", "宁夏", "新疆", "香港", "澳门",
        "中国", "成都", "台北", "高雄", "台中", "台南", "新北", "基隆", "台东",
        "花莲", "宜兰", "屏东", "南投", "嘉义", "新竹", "彰化",
    ]
    text_lower = text.lower()
    for kw in domestic_keywords:
        if kw in text_lower:
            return True
    has_chinese = any('一' <= c <= '鿿' for c in text)
    return has_chinese


def _infer_domestic(data: dict, msg_type: str, source: DataSource) -> bool:
    """推断地震是否为国内。"""
    if source in (DataSource.FAN_STUDIO, DataSource.WOLFX, DataSource.P2P_EARTHQUAKE):
        if any(kw in msg_type for kw in ("cenc", "cea", "sc_", "fj_", "cwa")):
            return True
        for key in ("location", "locationName", "epiLocation", "address",
                     "placeName", "region", "city", "area", "震央", "震中", "地点"):
            val = data.get(key, "")
            if val and _is_chinese_location(str(val)):
                return True
        for key in ("province", "city"):
            val = data.get(key, "")
            if val and _is_chinese_location(str(val)):
                return True
    if source == DataSource.GLOBAL_QUAKE:
        for key in ("region", "placeName", "location", "address"):
            val = data.get(key, "")
            if val and _is_chinese_location(str(val)):
                return True
    return False


def _build_earthquake_from(data: dict, source: DataSource, msg_type: str) -> Optional[EarthquakeEvent]:
    """从原始数据构建地震事件。"""
    mag, intensity, scale = _parse_intensity(data)
    if mag < 0.5:
        return None

    epicenter = _parse_epicenter(data)

    # 尝试获取事件 ID
    event_id = (
        data.get("eventId", data.get("event_id", data.get("id", "")))
        or f"{source.value}_{msg_type}_{int(time.time())}"
    )

    # 时间
    pub_time = None
    for time_key in (
        "create_time", "update_time", "publishTime", "originTime",
        "time", "timestamp", "shockTime", "震央時間", "地震発生時刻",
        "origin_time", "originTimeMs", "updateTime",
    ):
        if time_key in data:
            pub_time = _parse_time(data[time_key])
            if pub_time:
                break

    # 报告编号
    report_num = 0
    for rn_key in ("updates", "serialNo", "reportNum", "报次", "report_num", "revisionId", "revision_id"):
        if rn_key in data:
            try:
                report_num = int(data[rn_key])
            except (ValueError, TypeError):
                pass
            break

    # 是否最终报
    is_final = False
    for final_key in ("final", "isFinal", "is_final", "type"):
        val = data.get(final_key, "")
        if isinstance(val, bool) and val:
            is_final = True
            break
        if isinstance(val, str) and val.lower() in ("final", "true", "1", "最终报", "第1报", "archived"):
            is_final = True
            break

    # 如果 msg_type 包含 jma 且 report_num > 0，也标记为非最终
    if "jma" in msg_type and report_num > 0:
        is_final = False

    return EarthquakeEvent(
        event_id=event_id,
        event_type=EventType.EARTHQUAKE_WARNING if "eew" in msg_type else EventType.EARTHQUAKE_INFO,
        source=source,
        magnitude=mag,
        epicenter=epicenter,
        intensity=intensity,
        scale=scale,
        publish_time=pub_time,
        raw_data=data,
        report_number=report_num,
        is_final=is_final,
        is_domestic=_infer_domestic(data, msg_type, source),
    )


def _build_tsunami_from(data: dict, source: DataSource, msg_type: str) -> Optional[TsunamiEvent]:
    """从原始数据构建海啸事件。"""
    warning_level = ""
    for key in ("warningLevel", "level", "警報レベル", "海啸预警级别", "warning"):
        if key in data:
            warning_level = str(data[key])
            break

    areas = []
    for key in ("affectedAreas", "areas", "影响区域", "津波到達地域", "targetArea"):
        if key in data:
            val = data[key]
            if isinstance(val, list):
                areas = [str(a) for a in val]
            elif isinstance(val, str):
                areas = [a.strip() for a in val.split(",")]
            break

    return TsunamiEvent(
        event_id=f"tsunami_{source.value}_{msg_type}_{int(time.time())}",
        source=source,
        warning_level=warning_level,
        areas=areas,
        raw_data=data,
    )


def _build_weather_from(data: dict, source: DataSource, msg_type: str) -> Optional[WeatherAlert]:
    """从原始数据构建气象预警。"""
    title = ""
    for key in ("title", "alertTitle", "预警标题", "subject", "headline"):
        if key in data:
            title = str(data[key])
            break

    desc = ""
    for key in ("description", "content", "预警内容", "detail", "desc"):
        if key in data:
            desc = str(data[key])
            break

    level = ""
    for key in ("level", "colorLevel", "预警级别", "color", "severity"):
        if key in data:
            level = str(data[key])
            break

    areas = []
    for key in ("areas", "affectedAreas", "发布区域", "region"):
        if key in data:
            val = data[key]
            if isinstance(val, list):
                areas = [str(a) for a in val]
            elif isinstance(val, str):
                areas = [a.strip() for a in val.split(",")]
            break

    issue_time = None
    for key in ("issueTime", "publishTime", "发布时间", "time", "effective"):
        if key in data:
            issue_time = _parse_time(data[key])
            if issue_time:
                break

    return WeatherAlert(
        alert_id=f"weather_{source.value}_{msg_type}_{int(time.time())}",
        source=source,
        level=level,
        title=title,
        description=desc[:512] if len(desc) > 512 else desc,
        issue_time=issue_time,
        areas=areas,
        raw_data=data,
    )
