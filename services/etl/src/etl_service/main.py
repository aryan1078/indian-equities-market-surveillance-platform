from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

from cassandra.concurrent import execute_concurrent_with_args

from market_surveillance.db import get_cassandra_session, pg_connection
from market_surveillance.market_time import date_sk
from market_surveillance.metadata import load_stock_references, valid_peer_sector
from market_surveillance.sql import iter_date_dimension


STOCK_DIM_VALID_FROM = date(2000, 1, 1)
ETL_CASSANDRA_CONCURRENCY = 128


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Warehouse ETL")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--trading-date", required=True)
    run_window = subparsers.add_parser("run-window")
    run_window.add_argument("--start-date")
    run_window.add_argument("--end-date")
    run_window.add_argument("--latest-minute-window", action="store_true")
    return parser


def start_run(trading_date: date) -> str:
    run_id = uuid4().hex
    with pg_connection() as conn:
        conn.execute(
            """
            UPDATE operational.etl_runs
            SET finished_at = now(),
                status = 'failed',
                notes = COALESCE(notes, '{}'::jsonb) || jsonb_build_object('error', 'Superseded by a newer ETL attempt')
            WHERE trading_date = %s
              AND status = 'running'
            """,
            (trading_date,),
        )
        conn.execute(
            """
            INSERT INTO operational.etl_runs (run_id, trading_date)
            VALUES (%s, %s)
            """,
            (run_id, trading_date),
        )
    return run_id


def supersede_running_runs(start_date: date, end_date: date, reason: str) -> None:
    with pg_connection() as conn:
        conn.execute(
            """
            UPDATE operational.etl_runs
            SET status = 'failed',
                finished_at = NOW(),
                notes = jsonb_build_object('error', CAST(%s AS text))
            WHERE status = 'running'
              AND trading_date BETWEEN %s AND %s
            """,
            (reason, start_date, end_date),
        )


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text)


def latest_minute_backfill_window() -> tuple[date, date] | None:
    with pg_connection() as conn:
        row = conn.execute(
            """
            SELECT notes
            FROM operational.ingestion_runs
            WHERE mode = 'minute_backfill'
              AND status = 'completed'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return None
    notes = row["notes"] if isinstance(row["notes"], dict) else {}
    start_date = _coerce_date(notes.get("window_start"))
    end_date = _coerce_date(notes.get("window_end"))
    if start_date is None or end_date is None:
        return None
    return start_date, end_date


def trading_dates_for_window(start_date: date, end_date: date) -> list[date]:
    with pg_connection() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT trading_date
            FROM operational.stock_daily_bars
            WHERE trading_date BETWEEN %s AND %s
            ORDER BY trading_date ASC
            """,
            (start_date, end_date),
        ).fetchall()
    return [row["trading_date"] for row in rows]


def load_date_dimension(trading_date: date) -> None:
    dim_date = iter_date_dimension(trading_date)
    with pg_connection() as conn:
        conn.execute(
            """
            INSERT INTO warehouse.dim_date (
                date_sk, calendar_date, year, quarter, month, month_name, day, day_of_week, is_weekend
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_sk) DO NOTHING
            """,
            (
                dim_date["date_sk"],
                dim_date["calendar_date"],
                dim_date["year"],
                dim_date["quarter"],
                dim_date["month"],
                dim_date["month_name"],
                dim_date["day"],
                dim_date["day_of_week"],
                dim_date["is_weekend"],
            ),
        )


def load_static_dimensions() -> None:
    with pg_connection() as conn:
        for minute in range(24 * 60):
            label = f"{minute // 60:02d}:{minute % 60:02d}"
            conn.execute(
                """
                INSERT INTO warehouse.dim_time (time_sk, minute_of_day, hour, minute, label)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (time_sk) DO NOTHING
                """,
                (minute, minute, minute // 60, minute % 60, label),
            )

        for stock in load_stock_references():
            conn.execute(
                "INSERT INTO warehouse.dim_sector (sector_name) VALUES (%s) ON CONFLICT (sector_name) DO NOTHING",
                (stock.sector,),
            )
            conn.execute(
                "INSERT INTO warehouse.dim_exchange (exchange_code) VALUES (%s) ON CONFLICT (exchange_code) DO NOTHING",
                (stock.exchange,),
            )
            conn.execute(
                """
                UPDATE warehouse.dim_stock
                SET is_current = false,
                    valid_to = COALESCE(valid_to, %s)
                WHERE symbol = %s
                  AND valid_from <> %s
                  AND is_current = true
                """,
                (STOCK_DIM_VALID_FROM - timedelta(days=1), stock.symbol, STOCK_DIM_VALID_FROM),
            )
            conn.execute(
                """
                INSERT INTO warehouse.dim_stock (symbol, company_name, sector_name, exchange_code, valid_from, is_current)
                VALUES (%s, %s, %s, %s, %s, true)
                ON CONFLICT (symbol, valid_from) DO UPDATE
                SET company_name = EXCLUDED.company_name,
                    sector_name = EXCLUDED.sector_name,
                    exchange_code = EXCLUDED.exchange_code,
                    is_current = true,
                    valid_to = NULL
                """,
                (stock.symbol, stock.company_name, stock.sector, stock.exchange, STOCK_DIM_VALID_FROM),
            )


def load_dimensions(trading_date: date) -> None:
    load_date_dimension(trading_date)
    load_static_dimensions()


def extract_anomalies(trading_date: date) -> list[dict]:
    session = get_cassandra_session()
    select_stmt = session.prepare(
        """
        SELECT symbol, trading_date, timestamp_utc, timestamp_ist, exchange, sector, close, volume,
               return_pct, rolling_volatility, price_z_score, volume_z_score, composite_score,
               is_anomalous, source_run_id, dedupe_key
        FROM anomaly_metrics
        WHERE symbol = ? AND trading_date = ?
        """
    )
    rows: list[dict] = []
    query_args = [(stock.symbol, trading_date) for stock in load_stock_references()]
    for success, result in execute_concurrent_with_args(
        session,
        select_stmt,
        query_args,
        concurrency=ETL_CASSANDRA_CONCURRENCY,
        raise_on_first_error=True,
    ):
        if not success:
            raise result
        for row in result:
            if row["price_z_score"] is None:
                continue
            rows.append(row)
    return rows


def normalize_trading_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    if hasattr(value, "date") and callable(value.date):
        normalized = value.date()
        if isinstance(normalized, date):
            return normalized
    return date.fromisoformat(str(value))


def normalize_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def canonical_stage_sector(symbol: str, sector: Any, metadata_lookup: dict[str, Any]) -> str:
    normalized = str(sector or "").strip()
    if valid_peer_sector(normalized):
        return normalized

    metadata_stock = metadata_lookup.get(symbol)
    metadata_sector = getattr(metadata_stock, "sector", None)
    if valid_peer_sector(metadata_sector):
        return str(metadata_sector)
    return normalized or "Unknown"


def stage_rows(run_id: str, rows: list[dict]) -> tuple[int, int]:
    if not rows:
        return 0, 0
    metadata_lookup = {stock.symbol: stock for stock in load_stock_references()}
    with pg_connection() as conn:
        conn.execute("DELETE FROM staging.anomaly_metrics_stage WHERE run_id = %s", (run_id,))
        with conn.cursor() as cur:
            with cur.copy(
                """
                COPY staging.anomaly_metrics_stage (
                    run_id, symbol, trading_date, timestamp_utc, timestamp_ist, exchange, sector,
                    close, volume, return_pct, rolling_volatility, price_z_score, volume_z_score,
                    composite_score, is_anomalous, source_run_id, dedupe_key
                ) FROM STDIN
                """
            ) as copy:
                for row in rows:
                    trading_date = normalize_trading_date(row["trading_date"])
                    timestamp_utc = normalize_timestamp(row["timestamp_utc"])
                    timestamp_ist = normalize_timestamp(row["timestamp_ist"])
                    sector = canonical_stage_sector(str(row["symbol"]), row["sector"], metadata_lookup)
                    copy.write_row(
                        (
                            run_id,
                            row["symbol"],
                            trading_date,
                            timestamp_utc,
                            timestamp_ist,
                            row["exchange"],
                            sector,
                            row["close"],
                            row["volume"],
                            row["return_pct"],
                            row["rolling_volatility"],
                            row["price_z_score"],
                            row["volume_z_score"],
                            row["composite_score"],
                            row["is_anomalous"],
                            row["source_run_id"],
                            row["dedupe_key"],
                        )
                    )
        conn.execute(
            """
            UPDATE staging.anomaly_metrics_stage s
            SET contagion_flag = EXISTS (
                SELECT 1
                FROM operational.contagion_events ce
                WHERE ce.trigger_symbol = s.symbol
                  AND ce.event_timestamp BETWEEN s.timestamp_utc - interval '5 minutes' AND s.timestamp_utc + interval '5 minutes'
            )
            WHERE s.run_id = %s
            """,
            (run_id,),
        )
    return len(rows), 0


def rebuild_materialized_views(conn) -> int:
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS warehouse.mv_sector_daily_summary")
    conn.execute(
        """
        CREATE MATERIALIZED VIEW warehouse.mv_sector_daily_summary AS
        SELECT
            d.calendar_date,
            s.sector_name,
            COUNT(*) AS active_minutes,
            AVG(f.composite_score) AS avg_composite_score,
            MAX(f.composite_score) AS max_composite_score,
            SUM(CASE WHEN f.contagion_flag THEN 1 ELSE 0 END) AS contagion_minutes
        FROM warehouse.fact_anomaly_minute f
        JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
        JOIN warehouse.dim_sector s ON s.sector_sk = f.sector_sk
        GROUP BY d.calendar_date, s.sector_name
        """
    )
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS warehouse.mv_sector_monthly_summary")
    conn.execute(
        """
        CREATE MATERIALIZED VIEW warehouse.mv_sector_monthly_summary AS
        WITH market_summary AS (
            SELECT
                d.year,
                d.quarter,
                d.month,
                s.sector_name,
                AVG(f.avg_composite_score) AS avg_daily_composite_score,
                MAX(f.max_composite_score) AS max_daily_composite_score
            FROM warehouse.fact_market_day f
            JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
            JOIN warehouse.dim_sector s ON s.sector_sk = f.sector_sk
            GROUP BY d.year, d.quarter, d.month, s.sector_name
        ),
        contagion_summary AS (
            SELECT
                d.year,
                d.quarter,
                d.month,
                s.sector_name,
                COUNT(*) AS contagion_event_count
            FROM warehouse.fact_contagion_event f
            JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
            JOIN warehouse.dim_sector s ON s.sector_sk = f.sector_sk
            GROUP BY d.year, d.quarter, d.month, s.sector_name
        )
        SELECT
            market_summary.year,
            market_summary.quarter,
            market_summary.month,
            market_summary.sector_name,
            market_summary.avg_daily_composite_score,
            market_summary.max_daily_composite_score,
            COALESCE(contagion_summary.contagion_event_count, 0) AS contagion_event_count
        FROM market_summary
        LEFT JOIN contagion_summary
          ON contagion_summary.year = market_summary.year
         AND contagion_summary.quarter = market_summary.quarter
         AND contagion_summary.month = market_summary.month
         AND contagion_summary.sector_name = market_summary.sector_name
        """
    )
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS warehouse.mv_sector_regime_summary")
    conn.execute(
        """
        CREATE MATERIALIZED VIEW warehouse.mv_sector_regime_summary AS
        WITH daily_summary AS (
            SELECT
                sec.sector_name,
                COUNT(DISTINCT f.date_sk) AS sessions_covered,
                COUNT(DISTINCT ds.symbol) AS symbols_covered,
                SUM(f.anomaly_count) AS total_anomalies,
                AVG(f.avg_composite_score) AS avg_daily_composite_score,
                MAX(f.max_composite_score) AS peak_daily_composite_score,
                SUM(f.contagion_event_count) AS contagion_event_count,
                MAX(d.calendar_date) AS latest_calendar_date
            FROM warehouse.fact_market_day f
            JOIN warehouse.dim_stock ds ON ds.stock_sk = f.stock_sk
            JOIN warehouse.dim_sector sec ON sec.sector_sk = f.sector_sk
            JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
            GROUP BY sec.sector_name
        ),
        minute_summary AS (
            SELECT
                sec.sector_name,
                COUNT(*) AS anomaly_minutes,
                SUM(CASE WHEN f.contagion_flag THEN 1 ELSE 0 END) AS contagion_minutes
            FROM warehouse.fact_anomaly_minute f
            JOIN warehouse.dim_sector sec ON sec.sector_sk = f.sector_sk
            GROUP BY sec.sector_name
        )
        SELECT
            daily_summary.sector_name,
            daily_summary.sessions_covered,
            daily_summary.symbols_covered,
            COALESCE(minute_summary.anomaly_minutes, 0) AS anomaly_minutes,
            daily_summary.total_anomalies,
            COALESCE(minute_summary.contagion_minutes, 0) AS contagion_minutes,
            daily_summary.contagion_event_count,
            daily_summary.avg_daily_composite_score,
            daily_summary.peak_daily_composite_score,
            daily_summary.latest_calendar_date
        FROM daily_summary
        LEFT JOIN minute_summary
          ON minute_summary.sector_name = daily_summary.sector_name
        """
    )
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS warehouse.mv_stock_signal_leaders")
    conn.execute(
        """
        CREATE MATERIALIZED VIEW warehouse.mv_stock_signal_leaders AS
        WITH daily_facts AS (
            SELECT
                ds.symbol,
                d.calendar_date,
                f.anomaly_count,
                f.avg_composite_score,
                f.max_composite_score,
                f.contagion_event_count
            FROM warehouse.fact_market_day f
            JOIN warehouse.dim_stock ds ON ds.stock_sk = f.stock_sk
            JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
        ),
        current_stock AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                company_name,
                sector_name
            FROM warehouse.dim_stock
            WHERE is_current = true
            ORDER BY symbol, valid_from DESC
        ),
        latest_snapshot AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                calendar_date AS latest_calendar_date,
                anomaly_count AS latest_anomaly_count,
                max_composite_score AS latest_peak_score
            FROM daily_facts
            ORDER BY symbol, calendar_date DESC
        )
        SELECT
            latest_snapshot.symbol,
            cs.company_name,
            cs.sector_name,
            COUNT(*) AS sessions_covered,
            COUNT(*) FILTER (WHERE f.anomaly_count > 0) AS anomaly_days,
            SUM(f.anomaly_count) AS total_anomalies,
            AVG(f.avg_composite_score) AS avg_daily_composite_score,
            MAX(f.max_composite_score) AS peak_daily_composite_score,
            SUM(f.contagion_event_count) AS contagion_event_count,
            latest_snapshot.latest_calendar_date,
            latest_snapshot.latest_anomaly_count,
            latest_snapshot.latest_peak_score
        FROM daily_facts f
        JOIN latest_snapshot ON latest_snapshot.symbol = f.symbol
        JOIN current_stock cs ON cs.symbol = latest_snapshot.symbol
        GROUP BY
            latest_snapshot.symbol,
            cs.company_name,
            cs.sector_name,
            latest_snapshot.latest_calendar_date,
            latest_snapshot.latest_anomaly_count,
            latest_snapshot.latest_peak_score
        """
    )
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS warehouse.mv_sector_momentum_summary")
    conn.execute(
        """
        CREATE MATERIALIZED VIEW warehouse.mv_sector_momentum_summary AS
        WITH ranked_dates AS (
            SELECT
                calendar_date,
                ROW_NUMBER() OVER (ORDER BY calendar_date DESC) AS recency_rank
            FROM (
                SELECT DISTINCT d.calendar_date
                FROM warehouse.fact_market_day f
                JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
            ) dates
        ),
        windowed AS (
            SELECT
                sec.sector_name,
                CASE
                    WHEN rd.recency_rank BETWEEN 1 AND 5 THEN 'recent'
                    WHEN rd.recency_rank BETWEEN 6 AND 10 THEN 'prior'
                    ELSE NULL
                END AS window_name,
                COUNT(DISTINCT d.calendar_date) AS sessions_covered,
                COALESCE(SUM(f.anomaly_count), 0) AS total_anomalies,
                AVG(f.avg_composite_score) AS avg_daily_composite_score,
                MAX(f.max_composite_score) AS peak_daily_composite_score,
                COALESCE(SUM(f.contagion_event_count), 0) AS contagion_event_count
            FROM warehouse.fact_market_day f
            JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
            JOIN warehouse.dim_sector sec ON sec.sector_sk = f.sector_sk
            JOIN ranked_dates rd ON rd.calendar_date = d.calendar_date
            WHERE rd.recency_rank <= 10
            GROUP BY
                sec.sector_name,
                CASE
                    WHEN rd.recency_rank BETWEEN 1 AND 5 THEN 'recent'
                    WHEN rd.recency_rank BETWEEN 6 AND 10 THEN 'prior'
                    ELSE NULL
                END
        )
        SELECT
            sector_name,
            COALESCE(MAX(CASE WHEN window_name = 'recent' THEN sessions_covered END), 0) AS recent_sessions,
            COALESCE(MAX(CASE WHEN window_name = 'prior' THEN sessions_covered END), 0) AS prior_sessions,
            COALESCE(MAX(CASE WHEN window_name = 'recent' THEN total_anomalies END), 0) AS recent_total_anomalies,
            COALESCE(MAX(CASE WHEN window_name = 'prior' THEN total_anomalies END), 0) AS prior_total_anomalies,
            COALESCE(MAX(CASE WHEN window_name = 'recent' THEN avg_daily_composite_score END), 0) AS recent_avg_daily_composite_score,
            COALESCE(MAX(CASE WHEN window_name = 'prior' THEN avg_daily_composite_score END), 0) AS prior_avg_daily_composite_score,
            COALESCE(MAX(CASE WHEN window_name = 'recent' THEN peak_daily_composite_score END), 0) AS recent_peak_daily_composite_score,
            COALESCE(MAX(CASE WHEN window_name = 'prior' THEN peak_daily_composite_score END), 0) AS prior_peak_daily_composite_score,
            COALESCE(MAX(CASE WHEN window_name = 'recent' THEN contagion_event_count END), 0) AS recent_contagion_event_count,
            COALESCE(MAX(CASE WHEN window_name = 'prior' THEN contagion_event_count END), 0) AS prior_contagion_event_count,
            COALESCE(MAX(CASE WHEN window_name = 'recent' THEN total_anomalies END), 0)
                - COALESCE(MAX(CASE WHEN window_name = 'prior' THEN total_anomalies END), 0) AS anomaly_delta,
            COALESCE(MAX(CASE WHEN window_name = 'recent' THEN avg_daily_composite_score END), 0)
                - COALESCE(MAX(CASE WHEN window_name = 'prior' THEN avg_daily_composite_score END), 0) AS score_delta,
            COALESCE(MAX(CASE WHEN window_name = 'recent' THEN contagion_event_count END), 0)
                - COALESCE(MAX(CASE WHEN window_name = 'prior' THEN contagion_event_count END), 0) AS contagion_delta
        FROM windowed
        GROUP BY sector_name
        """
    )
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS warehouse.mv_stock_persistence_summary")
    conn.execute(
        """
        CREATE MATERIALIZED VIEW warehouse.mv_stock_persistence_summary AS
        WITH ranked_dates AS (
            SELECT
                calendar_date,
                ROW_NUMBER() OVER (ORDER BY calendar_date DESC) AS recency_rank
            FROM (
                SELECT DISTINCT d.calendar_date
                FROM warehouse.fact_market_day f
                JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
            ) dates
        ),
        current_stock AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                company_name,
                sector_name
            FROM warehouse.dim_stock
            WHERE is_current = true
            ORDER BY symbol, valid_from DESC
        ),
        latest_window AS (
            SELECT MAX(calendar_date) AS latest_calendar_date
            FROM ranked_dates
        ),
        daily_facts AS (
            SELECT
                ds.symbol,
                cs.company_name,
                cs.sector_name,
                d.calendar_date,
                f.anomaly_count,
                f.avg_composite_score,
                f.max_composite_score,
                f.contagion_event_count
            FROM warehouse.fact_market_day f
            JOIN warehouse.dim_stock ds ON ds.stock_sk = f.stock_sk
            JOIN current_stock cs ON cs.symbol = ds.symbol
            JOIN warehouse.dim_date d ON d.date_sk = f.date_sk
        ),
        recent_activity AS (
            SELECT
                df.symbol,
                COALESCE(SUM(df.anomaly_count) FILTER (WHERE rd.recency_rank <= 5), 0) AS recent_5_session_anomalies,
                COUNT(*) FILTER (WHERE rd.recency_rank <= 5 AND df.anomaly_count > 0) AS recent_5_session_anomaly_days
            FROM daily_facts df
            JOIN ranked_dates rd ON rd.calendar_date = df.calendar_date
            GROUP BY df.symbol
        )
        SELECT
            df.symbol,
            MAX(df.company_name) AS company_name,
            MAX(df.sector_name) AS sector_name,
            COUNT(*) AS sessions_covered,
            COUNT(*) FILTER (WHERE df.anomaly_count > 0) AS anomaly_days,
            COALESCE(SUM(df.anomaly_count), 0) AS total_anomalies,
            AVG(df.avg_composite_score) AS avg_daily_composite_score,
            MAX(df.max_composite_score) AS peak_daily_composite_score,
            COALESCE(SUM(df.contagion_event_count), 0) AS contagion_event_count,
            MAX(df.calendar_date) FILTER (WHERE df.anomaly_count > 0) AS last_anomaly_date,
            COALESCE(ra.recent_5_session_anomalies, 0) AS recent_5_session_anomalies,
            COALESCE(ra.recent_5_session_anomaly_days, 0) AS recent_5_session_anomaly_days,
            CASE
                WHEN COUNT(*) = 0 THEN 0
                ELSE (COUNT(*) FILTER (WHERE df.anomaly_count > 0))::double precision / COUNT(*)
            END AS anomaly_day_ratio,
            CASE
                WHEN COUNT(*) FILTER (WHERE df.anomaly_count > 0) = 0 THEN 0
                ELSE COALESCE(SUM(df.anomaly_count), 0)::double precision / COUNT(*) FILTER (WHERE df.anomaly_count > 0)
            END AS avg_anomalies_per_active_day,
            CASE
                WHEN MAX(df.calendar_date) FILTER (WHERE df.anomaly_count > 0) IS NULL THEN NULL
                ELSE (lw.latest_calendar_date - MAX(df.calendar_date) FILTER (WHERE df.anomaly_count > 0))
            END AS days_since_last_anomaly
        FROM daily_facts df
        JOIN latest_window lw ON true
        LEFT JOIN recent_activity ra ON ra.symbol = df.symbol
        GROUP BY
            df.symbol,
            ra.recent_5_session_anomalies,
            ra.recent_5_session_anomaly_days,
            lw.latest_calendar_date
        """
    )
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS warehouse.mv_intraday_pressure_profile")
    conn.execute(
        """
        CREATE MATERIALIZED VIEW warehouse.mv_intraday_pressure_profile AS
        WITH corrected_dates AS (
            SELECT date_sk
            FROM warehouse.fact_anomaly_minute
            GROUP BY date_sk
            HAVING MIN(time_sk) >= 560
        )
        SELECT
            dt.time_sk,
            dt.label AS time_label,
            dt.hour,
            dt.minute,
            COUNT(*) AS anomaly_minutes,
            COUNT(DISTINCT f.stock_sk) AS distinct_stocks,
            COUNT(DISTINCT f.date_sk) AS sessions_covered,
            AVG(f.composite_score) AS avg_composite_score,
            MAX(f.composite_score) AS peak_composite_score,
            SUM(CASE WHEN f.contagion_flag THEN 1 ELSE 0 END) AS contagion_minutes
        FROM warehouse.fact_anomaly_minute f
        JOIN warehouse.dim_time dt ON dt.time_sk = f.time_sk
        JOIN corrected_dates cd ON cd.date_sk = f.date_sk
        GROUP BY dt.time_sk, dt.label, dt.hour, dt.minute
        """
    )
    daily_rows = conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.mv_sector_daily_summary").fetchone()["row_count"]
    monthly_rows = conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.mv_sector_monthly_summary").fetchone()["row_count"]
    sector_regime_rows = conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.mv_sector_regime_summary").fetchone()["row_count"]
    stock_leader_rows = conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.mv_stock_signal_leaders").fetchone()["row_count"]
    sector_momentum_rows = conn.execute(
        "SELECT COUNT(*) AS row_count FROM warehouse.mv_sector_momentum_summary"
    ).fetchone()["row_count"]
    stock_persistence_rows = conn.execute(
        "SELECT COUNT(*) AS row_count FROM warehouse.mv_stock_persistence_summary"
    ).fetchone()["row_count"]
    intraday_profile_rows = conn.execute(
        "SELECT COUNT(*) AS row_count FROM warehouse.mv_intraday_pressure_profile"
    ).fetchone()["row_count"]
    return (
        daily_rows
        + monthly_rows
        + sector_regime_rows
        + stock_leader_rows
        + sector_momentum_rows
        + stock_persistence_rows
        + intraday_profile_rows
    )


def purge_facts_for_date(conn, trading_date: date) -> None:
    day_key = date_sk(trading_date)
    conn.execute("DELETE FROM warehouse.fact_surveillance_coverage WHERE date_sk = %s", (day_key,))
    conn.execute("DELETE FROM warehouse.fact_contagion_event WHERE date_sk = %s", (day_key,))
    conn.execute("DELETE FROM warehouse.fact_market_day WHERE date_sk = %s", (day_key,))
    conn.execute("DELETE FROM warehouse.fact_anomaly_minute WHERE date_sk = %s", (day_key,))


def load_facts(run_id: str, trading_date: date, rebuild_views: bool = True) -> tuple[int, int]:
    with pg_connection() as conn:
        purge_facts_for_date(conn, trading_date)
        conn.execute(
            """
            INSERT INTO warehouse.fact_anomaly_minute (
                stock_sk, date_sk, time_sk, sector_sk, exchange_sk,
                composite_score, price_z_score, volume_z_score, rolling_volatility,
                contagion_flag, dedupe_key, source_run_id
            )
            SELECT
                ds.stock_sk,
                dd.date_sk,
                dt.time_sk,
                sec.sector_sk,
                ex.exchange_sk,
                s.composite_score,
                s.price_z_score,
                s.volume_z_score,
                s.rolling_volatility,
                s.contagion_flag,
                s.dedupe_key,
                s.source_run_id
            FROM staging.anomaly_metrics_stage s
            JOIN warehouse.dim_stock ds ON ds.symbol = s.symbol AND ds.is_current = true
            JOIN warehouse.dim_date dd ON dd.calendar_date = s.trading_date
            JOIN warehouse.dim_time dt
              ON dt.minute_of_day =
                    EXTRACT(HOUR FROM s.timestamp_ist AT TIME ZONE 'Asia/Kolkata') * 60
                  + EXTRACT(MINUTE FROM s.timestamp_ist AT TIME ZONE 'Asia/Kolkata')
            JOIN warehouse.dim_sector sec ON sec.sector_name = s.sector
            JOIN warehouse.dim_exchange ex ON ex.exchange_code = s.exchange
            WHERE s.run_id = %s
            ON CONFLICT (stock_sk, date_sk, time_sk) DO UPDATE
            SET composite_score = EXCLUDED.composite_score,
                price_z_score = EXCLUDED.price_z_score,
                volume_z_score = EXCLUDED.volume_z_score,
                rolling_volatility = EXCLUDED.rolling_volatility,
                contagion_flag = EXCLUDED.contagion_flag,
                dedupe_key = EXCLUDED.dedupe_key,
                source_run_id = EXCLUDED.source_run_id
            """,
            (run_id,),
        )
        inserted_rows = conn.execute(
            "SELECT COUNT(*) AS row_count FROM staging.anomaly_metrics_stage WHERE run_id = %s",
            (run_id,),
        ).fetchone()["row_count"]

        conn.execute(
            """
            INSERT INTO warehouse.fact_market_day (
                stock_sk, date_sk, sector_sk, exchange_sk,
                anomaly_count, max_composite_score, avg_composite_score, avg_volume_z_score, contagion_event_count
            )
            SELECT
                ds.stock_sk,
                dd.date_sk,
                sec.sector_sk,
                ex.exchange_sk,
                COUNT(*) FILTER (WHERE s.is_anomalous),
                MAX(s.composite_score),
                AVG(s.composite_score),
                AVG(COALESCE(s.volume_z_score, 0)),
                COUNT(*) FILTER (WHERE s.contagion_flag)
            FROM staging.anomaly_metrics_stage s
            JOIN warehouse.dim_stock ds ON ds.symbol = s.symbol AND ds.is_current = true
            JOIN warehouse.dim_date dd ON dd.calendar_date = s.trading_date
            JOIN warehouse.dim_sector sec ON sec.sector_name = s.sector
            JOIN warehouse.dim_exchange ex ON ex.exchange_code = s.exchange
            WHERE s.run_id = %s
            GROUP BY ds.stock_sk, dd.date_sk, sec.sector_sk, ex.exchange_sk
            ON CONFLICT (stock_sk, date_sk) DO UPDATE
            SET anomaly_count = EXCLUDED.anomaly_count,
                max_composite_score = EXCLUDED.max_composite_score,
                avg_composite_score = EXCLUDED.avg_composite_score,
                avg_volume_z_score = EXCLUDED.avg_volume_z_score,
                contagion_event_count = EXCLUDED.contagion_event_count
            """,
            (run_id,),
        )

        conn.execute(
            """
            INSERT INTO warehouse.fact_contagion_event (
                event_id, stock_sk, date_sk, sector_sk, event_timestamp, affected_count, peer_average_score, risk_score, rationale
            )
            SELECT
                ce.event_id,
                ds.stock_sk,
                dd.date_sk,
                sec.sector_sk,
                ce.event_timestamp,
                ce.affected_count,
                ce.peer_average_score,
                ce.risk_score,
                ce.rationale
            FROM operational.contagion_events ce
            JOIN warehouse.dim_stock ds ON ds.symbol = ce.trigger_symbol AND ds.is_current = true
            JOIN warehouse.dim_date dd ON dd.calendar_date = ce.trading_date
            JOIN warehouse.dim_sector sec ON sec.sector_name = ce.trigger_sector
            WHERE ce.trading_date = %s
            ON CONFLICT (event_id) DO UPDATE
            SET affected_count = EXCLUDED.affected_count,
                peer_average_score = EXCLUDED.peer_average_score,
                risk_score = EXCLUDED.risk_score,
                rationale = EXCLUDED.rationale
            """,
            (trading_date,),
        )

        conn.execute(
            """
            INSERT INTO warehouse.fact_surveillance_coverage (
                stock_sk, date_sk, time_sk, coverage_state, source_run_id
            )
            SELECT
                ds.stock_sk,
                dd.date_sk,
                dt.time_sk,
                sc.coverage_state,
                sc.source_run_id
            FROM operational.surveillance_coverage sc
            JOIN warehouse.dim_stock ds ON ds.symbol = sc.symbol AND ds.is_current = true
            JOIN warehouse.dim_date dd ON dd.calendar_date = sc.trading_date
            JOIN warehouse.dim_time dt
              ON dt.minute_of_day =
                    EXTRACT(HOUR FROM sc.timestamp_ist AT TIME ZONE 'Asia/Kolkata') * 60
                  + EXTRACT(MINUTE FROM sc.timestamp_ist AT TIME ZONE 'Asia/Kolkata')
            WHERE sc.trading_date = %s
            ON CONFLICT (stock_sk, date_sk, time_sk) DO UPDATE
            SET coverage_state = EXCLUDED.coverage_state,
                source_run_id = EXCLUDED.source_run_id
            """,
            (trading_date,),
        )

        conn.execute("DELETE FROM staging.anomaly_metrics_stage WHERE run_id = %s", (run_id,))
        aggregate_rows = rebuild_materialized_views(conn) if rebuild_views else 0
    return inserted_rows, aggregate_rows


def finish_run(run_id: str, extracted_rows: int, staged_rows: int, inserted_rows: int, aggregate_rows: int) -> None:
    checksum = hashlib.sha1(f"{run_id}:{extracted_rows}:{inserted_rows}".encode("utf-8")).hexdigest()
    with pg_connection() as conn:
        conn.execute(
            """
            UPDATE operational.etl_runs
            SET finished_at = now(),
                extracted_rows = %s,
                staged_rows = %s,
                inserted_rows = %s,
                aggregate_rows = %s,
                checksum = %s,
                status = 'completed'
            WHERE run_id = %s
            """,
            (extracted_rows, staged_rows, inserted_rows, aggregate_rows, checksum, run_id),
        )


def fail_run(run_id: str, error_message: str) -> None:
    with pg_connection() as conn:
        conn.execute(
            """
            UPDATE operational.etl_runs
            SET finished_at = now(),
                status = 'failed',
                notes = jsonb_build_object('error', CAST(%s AS text))
            WHERE run_id = %s
            """,
            (error_message[:500], run_id),
        )


def run_for_date(trading_date: date, rebuild_views: bool = True, prepare_dimensions: bool = True) -> None:
    run_id = start_run(trading_date)
    try:
        print(f"[etl] starting trading_date={trading_date.isoformat()} run_id={run_id}", flush=True)
        if prepare_dimensions:
            load_dimensions(trading_date)
            print(f"[etl] dimensions prepared for {trading_date.isoformat()}", flush=True)
        rows = extract_anomalies(trading_date)
        print(f"[etl] extracted {len(rows)} anomaly rows for {trading_date.isoformat()}", flush=True)
        extracted_rows, excluded_rows = stage_rows(run_id, rows)
        print(
            f"[etl] staged rows for {trading_date.isoformat()} extracted={extracted_rows} excluded={excluded_rows}",
            flush=True,
        )
        inserted_rows, aggregate_rows = load_facts(run_id, trading_date, rebuild_views=rebuild_views)
        print(
            f"[etl] loaded facts for {trading_date.isoformat()} inserted={inserted_rows} aggregate_rows={aggregate_rows}",
            flush=True,
        )
        with pg_connection() as conn:
            conn.execute(
                """
                UPDATE operational.etl_runs
                SET excluded_rows = %s
                WHERE run_id = %s
                """,
                (excluded_rows, run_id),
            )
        finish_run(run_id, extracted_rows, extracted_rows - excluded_rows, inserted_rows, aggregate_rows)
        print(f"[etl] completed trading_date={trading_date.isoformat()} run_id={run_id}", flush=True)
    except Exception as exc:
        fail_run(run_id, str(exc))
        print(f"[etl] failed trading_date={trading_date.isoformat()} run_id={run_id}: {exc}", flush=True)
        raise


def run_window(start_date: date, end_date: date) -> list[date]:
    trading_dates = trading_dates_for_window(start_date, end_date)
    if not trading_dates:
        return []

    print(
        f"[etl] starting window start_date={start_date.isoformat()} end_date={end_date.isoformat()} dates={len(trading_dates)}",
        flush=True,
    )
    supersede_running_runs(
        start_date,
        end_date,
        f"Superseded by rerun of ETL window {start_date.isoformat()} to {end_date.isoformat()}",
    )
    load_static_dimensions()
    last_index = len(trading_dates) - 1
    for index, trading_date in enumerate(trading_dates):
        print(f"[etl] window progress {index + 1}/{len(trading_dates)} date={trading_date.isoformat()}", flush=True)
        load_date_dimension(trading_date)
        run_for_date(trading_date, rebuild_views=index == last_index, prepare_dimensions=False)
    print(
        f"[etl] completed window start_date={start_date.isoformat()} end_date={end_date.isoformat()}",
        flush=True,
    )
    return trading_dates


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "run":
        run_for_date(date.fromisoformat(args.trading_date))
    elif args.command == "run-window":
        if args.latest_minute_window:
            latest_window = latest_minute_backfill_window()
            if latest_window is None:
                raise SystemExit("No completed minute_backfill window with window_start/window_end metadata was found.")
            start_date, end_date = latest_window
        else:
            if not args.start_date or not args.end_date:
                raise SystemExit("run-window requires --start-date and --end-date, or use --latest-minute-window.")
            start_date = date.fromisoformat(args.start_date)
            end_date = date.fromisoformat(args.end_date)
        if start_date > end_date:
            raise SystemExit("start-date must be on or before end-date")
        run_window(start_date, end_date)


if __name__ == "__main__":
    main()
