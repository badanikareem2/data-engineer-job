# data-engineer-job

Baseline stack for streaming user engagement events from PostgreSQL to multiple sinks (ClickHouse, Redis, external system) with Flink.

## Stack
- PostgreSQL (source of truth)
- Redpanda (Kafka-compatible broker) + Console
- Apache Flink (stream processing)
- ClickHouse (columnar store, “BigQuery-like”)
- Redis (real-time aggregates)
- External mock (HTTP receiver)

## Run
./scripts/up.sh
# Flink UI:        http://localhost:8081
# Redpanda Console http://localhost:8080
# ClickHouse ping: curl http://localhost:8123/ping
# External mock:   http://localhost:9009/health
