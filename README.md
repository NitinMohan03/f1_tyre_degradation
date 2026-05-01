# F1 Tyre Degradation Monitoring

Near-real-time Formula 1 tyre degradation and race pace anomaly monitoring. Lambda-style Big Data pipeline: PySpark batch baselines → Kafka driver-lap event replay → Spark Structured Streaming → Redis live state → Streamlit dashboard.

Master's Big Data course project. Demo race: 2024 Bahrain Grand Prix.

## Architecture

```
FastF1 → raw Parquet → PySpark features → baselines + race_replay_events Parquet
                                                          │
                                                          ▼
                                        Kafka producer (driver-lap JSON, 1/lap/driver)
                                                          │
                                                          ▼
                                Spark Structured Streaming consumer (foreachBatch)
                                                          │
                                                          ▼
                              Redis (driver:{CODE} hash, history:{CODE} zset, alerts:log)
                                                          │
                                                          ▼
                                          Streamlit dashboard (2s autorefresh)
```

Risk score precomputed in batch (`race_replay_events.parquet`) per CLAUDE.md decision — Spark Streaming routes precomputed risk + applies consecutive-HIGH alert rule via Redis hash counter.

## Quick start

```powershell
pip install -r requirements.txt

# 1. Infra
docker-compose up -d

# 2. Batch (run once per race)
python ingestion/ingest_fastf1_laps.py
python features/build_lap_features.py
python features/build_degradation_baselines.py

# 3. Streaming (3 terminals)
# T1
python streaming/spark_degradation_consumer.py
# T2
python streaming/kafka_lap_replay_producer.py --race "2024 Bahrain Grand Prix" --speed 20
# T3
streamlit run dashboard/app.py
```

Dashboard: http://localhost:8501.

## Configuration

All knobs in `config.py`:

| Key | Purpose |
|-----|---------|
| `DEMO_SEASON`, `DEMO_RACE`, `DEMO_SESSION` | Default race for ingest + replay |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` | Kafka endpoint + topic |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB` | Redis endpoint |
| `CLEAN_LAP_MIN_LAP_NUMBER`, `TRACK_STATUS_EXCLUDE` | Clean-lap definition |
| `STINT_MIN_CLEAN_LAPS` | INSUFFICIENT_DATA threshold |
| `TYRE_AGE_BUCKETS` | Tyre-age bucketing |
| `RISK_WEIGHT_PACE/SLOPE/FIELD` | Risk score component weights (0.4/0.4/0.2) |
| `RISK_THRESHOLD_MEDIUM/HIGH` | Status thresholds (0.40 / 0.70) |
| `ALERT_HIGH_CONSECUTIVE`, `ALERT_FALLBACK_RISK` | Alert rules (2 consecutive HIGH or risk≥0.80) |
| `SPARK_KAFKA_PACKAGE` | Spark-Kafka connector coordinates |

## Layout

| Folder | Role |
|--------|------|
| `ingestion/` | FastF1 → Parquet (`ingest_fastf1_laps.py`) |
| `features/` | `build_lap_features.py`, `build_degradation_baselines.py` |
| `streaming/` | `kafka_lap_replay_producer.py`, `spark_degradation_consumer.py` |
| `dashboard/` | `app.py` (Streamlit) |
| `utils/` | Spark session, logging, time helpers |
| `data/raw/` | FastF1 lap parquet |
| `data/processed/` | Lap features, baselines, replay events parquet |
| `data/cache/` | FastF1 disk cache |
| `data/checkpoints/` | Spark Streaming checkpoints |
| `docs/` | Architecture notes, changelog |

## Stack

- Python 3.11.9, PySpark 3.5.5, FastF1, pandas, pyarrow, numpy
- kafka-python, redis-py, streamlit, plotly, streamlit-autorefresh
- Docker Compose: `apache/kafka:3.7.1` (KRaft single-node), `redis:7.4-alpine`

## Producer / consumer notes

- Producer: one Kafka message per driver-lap, key=driver_code. Sleeps `90/speed` seconds between lap groups (real lap ≈ 90s).
- Consumer: `startingOffsets=latest` — start consumer BEFORE producer, or restart producer to see fresh msgs.
- Topic auto-created on first produce (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`).

## Verifying Redis state

```powershell
docker exec -it f1_redis redis-cli
> KEYS driver:*
> HGETALL driver:VER
> ZRANGE history:VER 0 -1 WITHSCORES
> LRANGE alerts:log 0 9
> GET race:current
```

## Spec / state

- Authoritative spec: `F1_Tyre_Degradation_AI_Implementation_Prompt.md`
- Project memory: `CLAUDE.md` (architecture decisions)
- Resume point: `STATE.md`
- Phase history: `docs/CHANGELOG.md`
