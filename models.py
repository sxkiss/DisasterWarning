"""
灾害预警数据模型。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional


class EventType(Enum):
    EARTHQUAKE_WARNING = "earthquake_warning"
    EARTHQUAKE_INFO = "earthquake_info"
    TSUNAMI = "tsunami"
    WEATHER = "weather"


class DataSource(Enum):
    FAN_STUDIO = "fan_studio"
    P2P_EARTHQUAKE = "p2p_earthquake"
    WOLFX = "wolfx"
    GLOBAL_QUAKE = "global_quake"


@dataclass
class Epicenter:
    latitude: float
    longitude: float
    depth_km: float = 0.0
    location_name: str = ""
    province: str = ""
    city: str = ""


@dataclass
class EarthquakeEvent:
    event_id: str
    event_type: EventType = EventType.EARTHQUAKE_INFO
    source: DataSource = DataSource.FAN_STUDIO
    magnitude: float = 0.0
    epicenter: Epicenter = field(default_factory=Epicenter)
    intensity: float = 0.0
    scale: float = 0.0
    publish_time: Optional[datetime] = None
    raw_data: dict = field(default_factory=dict)
    report_number: int = 0
    is_final: bool = False
    received_at: float = field(default_factory=time.time)
    is_domestic: bool = False

    @property
    def timestamp(self) -> str:
        if self.publish_time:
            # 统一转为 UTC+8 显示
            dt = self.publish_time
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone(timedelta(hours=8)))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

    @property
    def location_str(self) -> str:
        parts = []
        if self.epicenter.province:
            parts.append(self.epicenter.province)
        if self.epicenter.city:
            parts.append(self.epicenter.city)
        if self.epicenter.location_name:
            parts.append(self.epicenter.location_name)
        if parts:
            return " ".join(parts)
        return f"坐标 {self.epicenter.latitude:.4f}, {self.epicenter.longitude:.4f}"


@dataclass
class TsunamiEvent:
    event_id: str
    source: DataSource = DataSource.FAN_STUDIO
    warning_level: str = ""
    areas: list = field(default_factory=list)
    publish_time: Optional[datetime] = None
    raw_data: dict = field(default_factory=dict)
    received_at: float = field(default_factory=time.time)
    is_domestic: bool = False

    @property
    def timestamp(self) -> str:
        if self.publish_time:
            dt = self.publish_time
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone(timedelta(hours=8)))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class WeatherAlert:
    alert_id: str
    source: DataSource = DataSource.FAN_STUDIO
    level: str = ""
    title: str = ""
    description: str = ""
    issue_time: Optional[datetime] = None
    areas: list = field(default_factory=list)
    raw_data: dict = field(default_factory=dict)
    received_at: float = field(default_factory=time.time)
    is_domestic: bool = False

    @property
    def timestamp(self) -> str:
        if self.issue_time:
            dt = self.issue_time
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone(timedelta(hours=8)))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


# ── 频率控制 ──────────────────────────────────────────────

class ReportCounter:
    """按地震事件追踪推送计数。"""

    def __init__(self, cea_cwa_n: int = 1, jma_n: int = 3, gq_n: int = 5):
        self.cea_cwa_n = cea_cwa_n
        self.jma_n = jma_n
        self.gq_n = gq_n
        self._counts: dict[str, int] = {}
        self._last_seen: dict[str, float] = {}

    def record(self, event_id: str, source: DataSource, is_final: bool) -> bool:
        """返回 True 表示应该推送。"""
        self._counts[event_id] = self._counts.get(event_id, 0) + 1
        self._last_seen[event_id] = time.time()
        count = self._counts[event_id]

        if is_final:
            return True

        if source in (DataSource.FAN_STUDIO,):
            n = self.cea_cwa_n
        elif source in (DataSource.P2P_EARTHQUAKE, DataSource.WOLFX):
            n = self.jma_n
        elif source == DataSource.GLOBAL_QUAKE:
            n = self.gq_n
        else:
            n = 1

        return count >= n

    def clear(self, event_id: str) -> None:
        self._counts.pop(event_id, None)
        self._last_seen.pop(event_id, None)

    def cleanup_old(self, max_age_seconds: int = 3600) -> None:
        now = time.time()
        stale = [eid for eid, ts in self._last_seen.items() if now - ts > max_age_seconds]
        for eid in stale:
            self.clear(eid)
