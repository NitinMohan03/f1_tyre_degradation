# STATE — F1 Tyre Degradation

Resume point. Update every session. Keep < 60 lines.

## Current phase

**Phases 10–13 code complete.** Single-race demo (Bahrain 2024) re-validated: 1129 rows, 90 anomalies (8.97%), weather attached, DuckDB views queryable. Pipeline still backwards-compatible with single-file raw parquet.

## Phase status

- [x] Phase 1–9 — base pipeline (see `docs/CHANGELOG.md`)
- [x] Phase 10 — multi-race ingestion (partitioned parquet, season sweep script)
- [x] Phase 11 — weather join (asof merge, weather cols through replay → Kafka → Redis → dashboard)
- [x] Phase 12 — IsolationForest anomaly (model trained, scored 1003 events, ML-only alert rule active)
- [x] Phase 13 — DuckDB explore page (`dashboard/pages/01_Explore.py`, 6 canned queries + free SQL)

## Next action

Run actual season sweep to populate Big Data volume:
```powershell
.venv\Scripts\python.exe ingestion\sweep_season.py
.venv\Scripts\python.exe features\build_lap_features.py
.venv\Scripts\python.exe features\train_anomaly_model.py
.venv\Scripts\python.exe features\build_degradation_baselines.py
```
Sweep takes ~30–60 min on first run (FastF1 fetches ~24 sessions). Subsequent runs skip cached partitions. After sweep validate ≥25k laps via `ingestion/sweep_season.py` summary.

Suggested commits (one per phase):
- `feat(ingest): multi-race season sweep with partitioned parquet`
- `feat(features): weather join + temp-bucketed baselines`
- `feat(ml): IsolationForest anomaly score in batch + stream`
- `feat(dashboard): DuckDB explore page for historical OLAP`

## Blockers

None.

## Quick commands

```powershell
# Full demo (4 terminals)
docker-compose up -d
python streaming/spark_degradation_consumer.py     # T1
python streaming/kafka_lap_replay_producer.py --speed 30   # T2
streamlit run dashboard/app.py                     # T3
docker exec -it f1_redis redis-cli                 # T4 inspect

# Reset Redis state mid-demo
docker exec f1_redis redis-cli FLUSHDB

# Reset Kafka offsets / topic (if consumer stuck)
docker exec f1_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic f1-lap-events
```
