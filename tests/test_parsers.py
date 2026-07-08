"""
灾害预警插件解析器单元测试。
"""
import pytest
from datetime import datetime, timezone, timedelta

from ..parsers import (
    parse_fan_studio,
    parse_p2p_earthquake,
    parse_wolfx,
    parse_global_quake,
    _parse_intensity,
    _parse_epicenter,
)
from ..models import DataSource, EventType


class TestParseIntensity:
    def test_basic_magnitude(self):
        mag, intensity, scale = _parse_intensity({"mag": 6.5})
        assert mag == 6.5
        assert intensity == 0.0
        assert scale == 0.0

    def test_chinese_intensity(self):
        mag, intensity, scale = _parse_intensity({"Mw": 5.0, "epiIntensity": 4.5})
        assert mag == 5.0
        assert intensity == 4.5

    def test_japanese_scale(self):
        mag, intensity, scale = _parse_intensity({"magnitude": 7.0, "scale": 5})
        assert mag == 7.0
        assert scale == 5.0

    def test_empty(self):
        mag, intensity, scale = _parse_intensity({})
        assert mag == 0.0


class TestParseEpicenter:
    def test_standard_fields(self):
        ep = _parse_epicenter({"lat": 35.5, "lon": 139.5, "depth": 50})
        assert ep.latitude == 35.5
        assert ep.longitude == 139.5
        assert ep.depth_km == 50.0

    def test_chinese_fields(self):
        ep = _parse_epicenter({
            "震央纬度": 24.5,
            "震央经度": 121.0,
            "震源深度": 30,
            "震央": "台湾花莲县",
        })
        assert ep.latitude == 24.5
        assert ep.longitude == 121.0
        assert ep.location_name == "台湾花莲县"

    def test_location_split(self):
        ep = _parse_epicenter({
            "lat": 39.9,
            "lon": 116.4,
            "location": "河北省唐山市/古冶区",
        })
        assert ep.province == "河北省唐山市"
        assert ep.city == "古冶区"


class TestParseFanStudio:
    def test_earthquake_warning(self):
        data = {
            "type": "cenc_eew",
            "data": {
                "mag": 6.0,
                "lat": 35.0,
                "lon": 135.0,
                "epiIntensity": 5.0,
                "create_time": "2025-01-01T12:00:00+08:00",
                "updates": 2,
            },
        }
        results = parse_fan_studio(data)
        assert len(results) == 1
        evt = results[0]
        assert isinstance(evt, EarthquakeEvent)
        assert evt.magnitude == 6.0
        assert evt.event_type == EventType.EARTHQUAKE_WARNING
        assert evt.source == DataSource.FAN_STUDIO

    def test_earthquake_info(self):
        data = {
            "type": "cenc",
            "data": {
                "mag": 5.5,
                "lat": 30.0,
                "lon": 120.0,
                "update_time": "2025-01-01 12:00:00",
                "report_num": 1,
            },
        }
        results = parse_fan_studio(data)
        assert len(results) == 1
        evt = results[0]
        assert evt.event_type == EventType.EARTHQUAKE_INFO

    def test_no_magnitude(self):
        data = {"type": "cenc", "data": {"lat": 30.0, "lon": 120.0}}
        results = parse_fan_studio(data)
        assert len(results) == 0

    def test_unknown_type(self):
        data = {"type": "heartbeat", "data": {}}
        results = parse_fan_studio(data)
        assert len(results) == 0


class TestParseP2PEarthquake:
    def test_eeew(self):
        data = {"code": "556", "magnitude": 6.5, "lat": 36.0, "lon": 140.0}
        results = parse_p2p_earthquake(data)
        assert len(results) == 1
        assert results[0].magnitude == 6.5

    def test_earthquake_info(self):
        data = {"code": "551", "mag": 5.0, "lat": 35.0, "lon": 138.0}
        results = parse_p2p_earthquake(data)
        assert len(results) == 1
        assert results[0].event_type == EventType.EARTHQUAKE_INFO

    def test_tsunami(self):
        data = {"code": "552", "warningLevel": "大津波注意報"}
        results = parse_p2p_earthquake(data)
        assert len(results) == 1
        from ..models import TsunamiEvent
        assert isinstance(results[0], TsunamiEvent)

    def test_ignored_code(self):
        data = {"code": "999"}
        results = parse_p2p_earthquake(data)
        assert len(results) == 0


class TestParseWolfx:
    def test_jma_eew(self):
        data = {"type": "jma_eew", "magnitude": 7.0, "lat": 38.0, "lon": 142.0}
        results = parse_wolfx(data)
        assert len(results) == 1
        assert results[0].magnitude == 7.0
        assert results[0].source == DataSource.WOLFX

    def test_cwa_eew(self):
        data = {"type": "cwa_eew", "mag": 5.5, "lat": 23.5, "lon": 121.0}
        results = parse_wolfx(data)
        assert len(results) == 1

    def test_cenc_eqlist(self):
        data = {"type": "cenc_eqlist", "magnitude": 4.0, "lat": 30.0, "lon": 120.0}
        results = parse_wolfx(data)
        assert len(results) == 1
        assert results[0].event_type == EventType.EARTHQUAKE_INFO


class TestParseGlobalQuake:
    def test_basic(self):
        data = {"mag": 5.0, "lat": 40.0, "lon": -100.0, "time": "2025-01-01 12:00:00"}
        results = parse_global_quake(data)
        assert len(results) == 1
        assert results[0].magnitude == 5.0
        assert results[0].source == DataSource.GLOBAL_QUAKE


class TestReportCounter:
    @pytest.fixture
    def counter(self):
        from ..models import ReportCounter
        return ReportCounter(cea_cwa_n=2, jma_n=3, gq_n=5)

    def test_first_push(self, counter):
        from ..models import EarthquakeEvent, Epicenter
        evt = EarthquakeEvent(
            event_id="test_1",
            source=DataSource.FAN_STUDIO,
            magnitude=5.0,
            epicenter=Epicenter(latitude=35.0, longitude=135.0),
        )
        assert counter.record(evt.event_id, evt.source, evt.is_final) is True

    def test_second_push_skipped(self, counter):
        assert counter.record("test_1", DataSource.FAN_STUDIO, False) is False

    def test_third_push(self, counter):
        assert counter.record("test_1", DataSource.FAN_STUDIO, False) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
