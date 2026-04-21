# Indian Equities Market Surveillance Platform

Distributed market surveillance and cross-asset contagion detection system for Indian equities, built around Kafka, Cassandra, Redis, PostgreSQL, FastAPI, and Next.js.

## Monorepo Layout

- `infra/`: Docker, Cassandra, PostgreSQL, and Redis configuration.
- `services/collector`: `yfinance` backfill and replay producer.
- `services/storage-consumer`: Kafka-to-Cassandra persistence service.
- `services/anomaly-engine`: Redis-backed streaming anomaly detection.
- `services/contagion-engine`: sector-aware contagion event detection.
- `services/api`: FastAPI service for live and warehouse reads.
- `services/etl`: Cassandra-to-PostgreSQL warehouse ETL.
- `shared/contracts`: shared Python models, settings, and utilities.
- `shared/metadata`: stock universe and sector mappings.
- `frontend`: Next.js operator dashboard.
- `tests`: fixtures and unit tests.
- `docs`: architecture and viva-facing documentation.

## Quick Start

1. Copy `.env.example` to `.env`.
2. Run `powershell -ExecutionPolicy Bypass -File .\\shared\\scripts\\bootstrap.ps1`.
3. Install Python dependencies with `python -m venv .venv` and `.\\.venv\\Scripts\\pip install -r requirements.txt`.
4. Start the stack with `docker compose up --build`.
5. Open the API at `http://localhost:8000/docs` and the frontend at `http://localhost:3000`.

## Service Commands

- Collector backfill:
  - `python -m collector.main backfill --symbols RELIANCE.BO HDFCBANK.NS`
- Collector replay:
  - `python -m collector.main replay --fixture tests/fixtures/replay_ticks.jsonl --speed 30`
- Warehouse ETL:
  - `python -m etl_service.main run --trading-date 2026-03-16`

## Design Notes

- UTC is the system of record for timestamps, with IST trading date retained for business logic.
- Cassandra stores append-only operational facts.
- Redis stores stream state and hot dashboard views.
- PostgreSQL stores contagion events, ETL metadata, and analytical warehouse facts.
- Replay mode is a first-class workflow so the demo works outside market hours.

