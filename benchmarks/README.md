# Benchmarks

This folder is reserved for throughput and latency benchmarks after the core stack is running.

Recommended first passes:

- replay ingestion records/sec
- Kafka lag during accelerated replay
- Cassandra write latency under replay load
- Redis hot-read latency for overview payloads
- PostgreSQL ETL duration by trading date
- sector daily roll-up query latency

