# Changelog

All phase boundaries for the F1 Tyre Degradation Monitoring project.

## 2026-05-01 — Phase 9: docs

- README expanded (architecture diagram, config table, Redis verification commands).
- `docs/CHANGELOG.md` created (this file).

## 2026-05-01 — Phase 8: Streamlit dashboard

- `dashboard/app.py`: 2s autorefresh via streamlit-autorefresh.
- Header: race name, last lap, driver count, last update.
- Driver risk cards grid (5 cols, sorted desc by risk_score, color-coded border per status).
- Driver dropdown → 4 plotly charts: lap_time, risk_score (HIGH/MED hlines), pace_delta, tyre_age.
- Alerts table (last 50).
- Connection error banner if Redis down.

## 2026-05-01 — Phase 7: Spark Structured Streaming consumer

- `streaming/spark_degradation_consumer.py`: readStream from Kafka, parse JSON with EVENT_SCHEMA, foreachBatch → redis-py pipeline.
- Per-row writes: HSET `driver:{CODE}` (15 fields incl. consecutive_high), ZADD `history:{CODE}` (score=lap_number), LPUSH+LTRIM(200) `alerts:log`, SET `race:current`.
- Alert rule: HIGH × 2 consecutive clean laps OR risk≥0.80 fallback.
- Verified e2e via redis-cli: 20 driver hashes, alerts firing for both rules.

## 2026-05-01 — Phase 6: Kafka producer

- `streaming/kafka_lap_replay_producer.py`: argparse (--race/--speed/--bootstrap/--topic/--max-laps).
- Filter race, sort (lap_number, driver_code), group by lap, send one JSON per driver-lap with key=driver_code.
- Sleep `TYPICAL_LAP_SECONDS(90)/speed` between laps. NaN→null.
- Verified: 100 msgs at speed=100.

## 2026-05-01 — Phase 5: Docker stack

- Switched bitnami/kafka:3.7 → apache/kafka:3.7.1 (bitnami pulled 3.7 tag from Docker Hub).
- KRaft single-node, advertised localhost:9092.
- Redis 7.4-alpine with appendonly.

## 2026-05-01 — Phase 4: degradation baselines + replay events

- `features/build_degradation_baselines.py`.
- `degradation_slope_3/5`: rolling regression slope (cov/var) of lap_time vs stint_lap_number on clean laps only.
- compound_age_baselines on (year, race_round, compound, tyre_age_bucket): median/std pace_delta + slope_5 + sample_count.
- Field baselines per (race_name, lap_number).
- 4-tier fallback (specific → race+compound → compound+age → global) gated on sample_count ≥ 5.
- Risk: pace/slope/field z-clip [0,1] → weighted 0.4/0.4/0.2 → clip.
- Status: INSUFFICIENT_DATA if clean_laps_in_stint_so_far < 3, else thresholds 0.40/0.70.
- Verified: 1129 events, 10 baseline groups, distribution LOW 543 / MED 224 / INSUF 187 / HIGH 175.

## 2026-05-01 — Phase 3: lap features

- `utils/spark.py` SparkSession factory.
- `features/build_lap_features.py`: clean_lap (lap≥2, not pit, is_accurate, track_status excludes 4/5/6/7 via rlike), stint_lap_number, clean_laps_in_stint_so_far, tyre_age_bucket.
- Rolling pace (rolling_3/5_lap_avg, previous_5_lap_avg, pace_delta) — window per (driver, race, stint), no future leakage, clean-only via `when`.
- Field median per (race, lap) via percentile_approx + field_delta.
- Verified: 1024/1129 clean (90.7%), HUL S1 SOFT only stint with <3 clean.

## 2026-05-01 — Phase 2: FastF1 ingestion

- `ingestion/ingest_fastf1_laps.py`: argparse-based with config defaults.
- `utils/logging.py`, `utils/time_utils.py` (`timedelta_to_seconds`, `constructor_key`).
- `session.load(laps=True, telemetry=False, weather=False, messages=False)` for speed.
- Verified: 1129 rows, 20 drivers, SOFT+HARD only at Bahrain 2024.

## 2026-05-01 — Phase 1: skeleton

- `README.md`, `requirements.txt`, `docker-compose.yml`, `config.py`, `.gitignore`.
- Folder structure per spec.
- Architecture decisions locked in `CLAUDE.md`.
