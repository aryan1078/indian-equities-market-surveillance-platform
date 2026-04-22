from datetime import date

from api_service.main import (
    WarehouseQueryRequest,
    _alert_summary,
    _anomaly_summary,
    _filter_latest_trading_session,
    _history_summary,
    _overview_feed_mode,
    _resolve_alert_scope,
    _streaming_counts_from_bulk_runs,
    _system_scale_projection,
    _warehouse_normalize_query,
    _warehouse_query_catalog,
    _warehouse_report,
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


def test_system_scale_projection_uses_loaded_intraday_scope():
    projection = _system_scale_projection(
        listed_symbols=2389,
        intraday_symbols_loaded=5,
        intraday_trading_days_loaded=1,
        actual_intraday_tick_and_anomaly_rows=3725,
    )

    assert projection["minute_rows_per_trading_day"] == 1875
    assert projection["minute_rows_for_loaded_window"] == 1875
    assert projection["minute_rows_per_year"] == 468750
    assert projection["tick_and_anomaly_rows_for_loaded_window"] == 3750
    assert projection["tick_and_anomaly_rows_per_year"] == 937500
    assert projection["five_year_tick_and_anomaly_rows"] == 4687500
    assert projection["current_scope_share_of_listed_universe_pct"] == 0.21
    assert round(projection["actual_capture_vs_loaded_window_pct"], 4) == round((3725 / 3750) * 100, 4)
    assert projection["crosses_crore_in_loaded_window"] is False
    assert projection["crosses_crore_annually"] is False


def test_streaming_counts_from_bulk_runs_uses_bulk_metadata_only():
    counts = _streaming_counts_from_bulk_runs(
        [
            {"mode": "replay", "records_published": 1875, "notes": {"fixture": "tests/fixtures/replay_ticks.real.jsonl"}},
            {
                "mode": "backfill",
                "records_published": 10750500,
                "notes": {"tick_rows_written": 10750500, "anomaly_rows_written": 10177255},
            },
        ]
    )

    assert counts["market_ticks"] == 10750500
    assert counts["anomaly_metrics"] == 10177255


def test_filter_latest_trading_session_drops_stale_symbols():
    filtered = _filter_latest_trading_session(
        {
            "RELIANCE.BO": {
                "symbol": "RELIANCE.BO",
                "trading_date": "2026-03-16",
                "timestamp_ist": "2026-03-16T15:29:00+05:30",
            },
            "INFY.NS": {
                "symbol": "INFY.NS",
                "trading_date": "2026-04-20",
                "timestamp_ist": "2026-04-20T15:29:00+05:30",
            },
            "SBIN.NS": {
                "symbol": "SBIN.NS",
                "timestamp_ist": "2026-04-20T15:29:00+05:30",
            },
        }
    )

    assert set(filtered) == {"INFY.NS", "SBIN.NS"}


def test_overview_feed_mode_prefers_intraday_run_over_latest_ingestion():
    mode = _overview_feed_mode(
        {"mode": "hydrate_daily"},
        {"mode": "replay"},
    )

    assert mode == "replay"


def test_resolve_alert_scope_separates_current_and_stale_open_alerts():
    snapshot = _resolve_alert_scope(
        {
            date(2026, 3, 16): 4,
        },
        date(2026, 4, 20),
    )

    assert snapshot["current_trading_date"] == date(2026, 4, 20)
    assert snapshot["current_open_count"] == 0
    assert snapshot["stale_open_count"] == 4
    assert snapshot["total_open_count"] == 4
    assert snapshot["latest_stale_alert_date"] == date(2026, 3, 16)


def test_resolve_alert_scope_falls_back_to_latest_open_date_without_market_reference():
    snapshot = _resolve_alert_scope(
        {
            date(2026, 3, 16): 4,
            date(2026, 4, 8): 2,
        },
        None,
    )

    assert snapshot["current_trading_date"] == date(2026, 4, 8)
    assert snapshot["current_open_count"] == 2
    assert snapshot["stale_open_count"] == 4
    assert snapshot["total_open_count"] == 6
    assert snapshot["latest_stale_alert_date"] == date(2026, 3, 16)


def test_warehouse_normalize_query_uses_defaults_and_swaps_dates():
    request = WarehouseQueryRequest(
        dataset="sector_day",
        dimensions=["unknown_dimension"],
        measures=["unknown_measure"],
        date_from=date(2026, 4, 20),
        date_to=date(2026, 4, 10),
        limit=500,
    )

    normalized = _warehouse_normalize_query(request)
    dataset = _warehouse_query_catalog()["sector_day"]

    assert normalized["dimensions"] == list(dataset.default_dimensions)
    assert normalized["measures"] == list(dataset.default_measures)
    assert normalized["date_from"] == date(2026, 4, 10)
    assert normalized["date_to"] == date(2026, 4, 20)
    assert normalized["limit"] == 500


def test_warehouse_report_surfaces_top_finding():
    dataset = _warehouse_query_catalog()["sector_day"]
    query = {
        "dimensions": ["calendar_date", "sector_name"],
        "measures": ["max_composite_score"],
        "date_from": date(2026, 4, 1),
        "date_to": date(2026, 4, 20),
        "sector": None,
        "exchange": None,
        "symbol_search": None,
        "limit": 25,
    }
    rows = [
        {"calendar_date": "2026-04-18", "sector_name": "Banking", "max_composite_score": 2.85},
        {"calendar_date": "2026-04-18", "sector_name": "IT", "max_composite_score": 1.92},
    ]

    report = _warehouse_report(dataset, query, rows, 31)

    assert report["headline"] == "Sector daily rollups report"
    assert any("Banking" in highlight["value"] for highlight in report["highlights"])
    assert any("Banking" in finding for finding in report["findings"])
