from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

from market_surveillance.db import get_cassandra_session, pg_connection
from market_surveillance.market_time import date_sk, market_tz, minute_of_day
from market_surveillance.metadata import load_stock_references
from market_surveillance.sql import iter_date_dimension


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Warehouse ETL")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--trading-date", required=True)
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


def load_dimensions(trading_date: date) -> None:
    local_tz = market_tz()
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
                INSERT INTO warehouse.dim_stock (symbol, company_name, sector_name, exchange_code, valid_from, is_current)
                VALUES (%s, %s, %s, %s, %s, true)
                ON CONFLICT (symbol, valid_from) DO NOTHING
                """,
                (stock.symbol, stock.company_name, stock.sector, stock.exchange, trading_date),
            )


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
    for stock in load_stock_references():
        for row in session.execute(select_stmt, (stock.symbol, trading_date)):
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


def stage_rows(run_id: str, rows: list[dict]) -> tuple[int, int]:
    if not rows:
        return 0, 0
    with pg_connection() as conn:
        conn.execute("DELETE FROM staging.anomaly_metrics_stage WHERE run_id = %s", (run_id,))
        for row in rows:
            trading_date = normalize_trading_date(row["trading_date"])
            timestamp_utc = normalize_timestamp(row["timestamp_utc"])
            timestamp_ist = normalize_timestamp(row["timestamp_ist"])
            conn.execute(
                """
                INSERT INTO staging.anomaly_metrics_stage (
                    run_id, symbol, trading_date, timestamp_utc, timestamp_ist, exchange, sector,
                    close, volume, return_pct, rolling_volatility, price_z_score, volume_z_score,
                    composite_score, is_anomalous, source_run_id, dedupe_key
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    row["symbol"],
                    trading_date,
                    timestamp_utc,
                    timestamp_ist,
                    row["exchange"],
                    row["sector"],
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
                ),
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
    daily_rows = conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.mv_sector_daily_summary").fetchone()["row_count"]
    monthly_rows = conn.execute("SELECT COUNT(*) AS row_count FROM warehouse.mv_sector_monthly_summary").fetchone()["row_count"]
    return daily_rows + monthly_rows


def purge_facts_for_date(conn, trading_date: date) -> None:
    day_key = date_sk(trading_date)
    conn.execute("DELETE FROM warehouse.fact_surveillance_coverage WHERE date_sk = %s", (day_key,))
    conn.execute("DELETE FROM warehouse.fact_contagion_event WHERE date_sk = %s", (day_key,))
    conn.execute("DELETE FROM warehouse.fact_market_day WHERE date_sk = %s", (day_key,))
    conn.execute("DELETE FROM warehouse.fact_anomaly_minute WHERE date_sk = %s", (day_key,))


def load_facts(run_id: str, trading_date: date) -> tuple[int, int]:
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
            JOIN warehouse.dim_time dt ON dt.minute_of_day = EXTRACT(HOUR FROM s.timestamp_ist) * 60 + EXTRACT(MINUTE FROM s.timestamp_ist)
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
            JOIN warehouse.dim_time dt ON dt.minute_of_day = EXTRACT(HOUR FROM sc.timestamp_ist) * 60 + EXTRACT(MINUTE FROM sc.timestamp_ist)
            WHERE sc.trading_date = %s
            ON CONFLICT (stock_sk, date_sk, time_sk) DO UPDATE
            SET coverage_state = EXCLUDED.coverage_state,
                source_run_id = EXCLUDED.source_run_id
            """,
            (trading_date,),
        )

        aggregate_rows = rebuild_materialized_views(conn)
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
                notes = jsonb_build_object('error', %s)
            WHERE run_id = %s
            """,
            (error_message[:500], run_id),
        )


def run_for_date(trading_date: date) -> None:
    run_id = start_run(trading_date)
    try:
        load_dimensions(trading_date)
        rows = extract_anomalies(trading_date)
        extracted_rows, excluded_rows = stage_rows(run_id, rows)
        inserted_rows, aggregate_rows = load_facts(run_id, trading_date)
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
    except Exception as exc:
        fail_run(run_id, str(exc))
        raise


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "run":
        run_for_date(date.fromisoformat(args.trading_date))


if __name__ == "__main__":
    main()
