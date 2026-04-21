from api_service.main import (
    _alert_summary,
    _anomaly_summary,
    _history_summary,
    _streaming_counts_from_bulk_runs,
    _system_scale_projection,
)


def test_history_summary_computes_range_and_returns():
    rows = [
        {"trading_date": "2026-03-01", "high": 101.0, "low": 95.0, "close": 100.0, "volume": 1000},
        {"trading_date": "2026-03-02", "high": 103.0, "low": 99.0, "close": 102.0, "volume": 1100},
        {"trading_date": "2026-03-03", "high": 106.0, "low": 101.0, "close": 105.0, "volume": 1200},
        {"trading_date": "2026-03-04", "high": 109.0, "low": 104.0, "close": 108.0, "volume": 1300},
        {"trading_date": "2026-03-05", "high": 111.0, "low": 106.0, "close": 110.0, "volume": 1400},
        {"trading_date": "2026-03-06", "high": 113.0, "low": 107.0, "close": 112.0, "volume": 1500},
    ]

    summary = _history_summary(rows)

    assert summary["session_count"] == 6
    assert summary["period_high"] == 113.0
    assert summary["period_low"] == 95.0
    assert round(summary["range_position_pct"], 2) == round(((112.0 - 95.0) / (113.0 - 95.0)) * 100, 2)
    assert round(summary["return_5d_pct"], 2) == 12.0
    assert summary["return_20d_pct"] is None


def test_anomaly_summary_tracks_flagged_points():
    rows = [
        {"timestamp_ist": "2026-03-16T09:35:00+05:30", "composite_score": 1.4, "is_anomalous": False},
        {"timestamp_ist": "2026-03-16T09:36:00+05:30", "composite_score": 2.7, "is_anomalous": True},
        {"timestamp_ist": "2026-03-16T09:37:00+05:30", "composite_score": 3.1, "is_anomalous": True},
    ]

    summary = _anomaly_summary(rows)

    assert summary["point_count"] == 3
    assert summary["flagged_count"] == 2
    assert summary["peak_composite_score"] == 3.1
    assert summary["latest_flagged_at"] == "2026-03-16T09:37:00+05:30"


def test_alert_summary_counts_status_and_severity():
    rows = [
        {"severity": "critical", "status": "open"},
        {"severity": "high", "status": "acknowledged"},
        {"severity": "high", "status": "open"},
        {"severity": "low", "status": "open"},
    ]

    summary = _alert_summary(rows)

    assert summary["open_count"] == 3
    assert summary["acknowledged_count"] == 1
    assert summary["latest_severity"] == "critical"
    assert summary["severity_breakdown"] == {"critical": 1, "high": 2, "medium": 0, "low": 1}


def test_system_scale_projection_for_full_nse_minute_footprint():
    projection = _system_scale_projection(
        listed_symbols=2389,
        hydrated_trading_days=65,
        actual_materialized_rows=139702,
    )

    assert projection["minute_rows_per_trading_day"] == 895875
    assert projection["minute_rows_for_loaded_window"] == 58231875
    assert projection["minute_rows_per_year"] == 223968750
    assert projection["tick_and_anomaly_rows_for_loaded_window"] == 116463750
    assert projection["tick_and_anomaly_rows_per_year"] == 447937500
    assert projection["five_year_tick_and_anomaly_rows"] == 2239687500
    assert projection["crosses_crore_in_loaded_window"] is True
    assert projection["crosses_crore_annually"] is True


def test_streaming_counts_from_bulk_runs_uses_bulk_metadata_only():
    counts = _streaming_counts_from_bulk_runs(
        [
            {"mode": "replay", "records_published": 750, "notes": {"fixture": "tests/fixtures/replay_ticks.jsonl"}},
            {
                "mode": "minute_backfill",
                "records_published": 10750500,
                "notes": {"tick_rows_written": 10750500, "anomaly_rows_written": 10177255},
            },
        ]
    )

    assert counts["market_ticks"] == 10750500
    assert counts["anomaly_metrics"] == 10177255
