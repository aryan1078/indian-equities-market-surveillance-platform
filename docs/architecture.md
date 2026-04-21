# Architecture Notes

## Core Runtime

- `collector`: emits normalized minute bars from `yfinance` or replay fixtures into Kafka.
- `storage-consumer`: writes append-only operational facts into Cassandra.
- `anomaly-engine`: computes EWMA-based price and volume anomalies, stores state in Redis, persists minute metrics to Cassandra, and emits anomaly detections.
- `contagion-engine`: opens 5-minute sector windows, deduplicates trigger storms, and writes relational contagion events to PostgreSQL.
- `etl`: loads warehouse dimensions and facts from Cassandra plus operational PostgreSQL tables into a star schema.
- `api`: exposes both live operational reads and warehouse analytical reads behind one FastAPI surface.
- `frontend`: provides an operator-facing console optimized for demo and analysis.

## Data Stores

- Cassandra: `market_ticks`, `anomaly_metrics`, `stock_reference`, `latest_market_state`
- Redis: anomaly engine state, latest market views, latest anomaly views, freshness markers
- PostgreSQL operational schema: ingestion runs, ETL runs, surveillance coverage, contagion events
- PostgreSQL warehouse schema: stock/sector/date/time dimensions, minute anomaly fact, daily market fact, contagion fact, surveillance coverage fact

## Replay-First Delivery

Replay is a primary workflow, not a fallback. The system is meant to demonstrate:

1. deterministic Kafka re-ingestion
2. warm-up-aware anomaly scoring
3. contagion event creation after a bounded observation window
4. warehouse roll-up materialization after end-of-day ETL

## Known Extension Points

- Add Redis Sentinel and multi-node Kafka/Cassandra profiles for cluster demonstration.
- Expand stock universe and sector metadata.
- Introduce bounded peer-correlation logic inside the contagion engine.
- Add benchmark runners for replay throughput and warehouse query latency.

