# Project Decisions — F1 Tyre Degradation Monitoring

Consolidated record of architecture, data, and implementation decisions. ADR-style: each entry = decision + rationale + impact.

---

## 1. Architecture

### 1.1 Lambda-style split: batch + streaming

- **Batch (PySpark):** ingest → lap features → degradation slopes → baselines → risk score → `race_replay_events.parquet`.
- **Streaming (Kafka + Spark Structured Streaming):** producer replays parquet events → consumer routes to Redis.
- **Why:** Demo stability, reproducible risk math, simple grading. Streaming-only would couple correctness to wall-clock timing and complicate windowed regression.

### 1.2 Risk score precomputed in batch (not in streaming)

- **Decision:** Risk components and final score baked into `race_replay_events.parquet` during batch. Spark Streaming consumer parses + writes; it does NOT recompute risk.
- **Why:** Spec line 1032 explicitly allows it for first demo. Avoids stateful streaming joins against baselines + history.
- **Impact:** Streaming consumer is small + reliable. Trade-off: cannot ingest new races without re-running batch.

### 1.3 Kafka event = one per driver-lap (not raw telemetry)

- **Decision:** One JSON message per driver-lap, key = driver_code.
- **Why:** Spec line 581. Telemetry-row stream would burst at ~10–50 Hz × 20 drivers; lap-grain shows streaming architecture without flooding the demo.
- **Impact:** ~1100 messages per race. Trivial to debug and visualize.

---

## 2. Data definitions

### 2.1 Clean lap rules (`clean_lap` boolean)

A lap counts as clean iff ALL hold:

| Rule | Source |
|------|--------|
| `lap_number >= 2` | `CLEAN_LAP_MIN_LAP_NUMBER` (lap 1 = race start anomaly) |
| `lap_time_sec` not null | drop missing telemetry |
| `pit_in_time` is null | exclude inlap |
| `pit_out_time` is null | exclude outlap |
| `is_accurate == true` | FastF1 quality flag |
| `track_status` excludes `4,5,6,7` | `TRACK_STATUS_EXCLUDE` (SC, red, VSC deploy, VSC) |

- **TrackStatus mapping:** 1=clear, 2=yellow, 4=Safety Car, 5=red, 6=VSC deploying, 7=VSC. Yellow flags allowed (too common to drop).
- **Implementation:** `track_status` field can be a multi-char string ("12" = both 1 and 2). Use `rlike("[4567]")` to match any excluded char.

### 2.2 Stint-level features

- `stint_lap_number` = `row_number()` over (driver_code, race_name, stint) ordered by lap_number. Includes non-clean laps so it stays continuous.
- `clean_laps_in_stint_so_far` = cumulative sum of `clean_lap` cast to int over the same partition (inclusive of current row).
- `tyre_age_bucket` = bucketed `tyre_life` per `TYRE_AGE_BUCKETS = [(0,5),(6,10),(11,15),(16,20),(21,999)]`. Last bucket label `21+`.

### 2.3 Rolling pace (no future leakage)

- Window: per (driver_code, race_name, stint), order by lap_number.
- Clean-only via `when(clean_lap, lap_time_sec)` so non-clean rows contribute null and are ignored by `avg`.
- `rolling_3_lap_avg` = avg over `rowsBetween(-2, 0)` (current + 2 prior).
- `rolling_5_lap_avg` = avg over `rowsBetween(-4, 0)`.
- `previous_5_lap_avg` = avg over `rowsBetween(-5, -1)` (excludes current — used for `pace_delta` baseline).
- `pace_delta` = `lap_time_sec - previous_5_lap_avg`.

### 2.4 Field median lap time

- `field_median_lap_time` per (race_name, lap_number) via `percentile_approx(when(clean_lap, lap_time_sec), 0.5)`.
- `field_delta` = driver `lap_time_sec` - `field_median_lap_time`.

### 2.5 Degradation slopes (rolling regression)

- `degradation_slope_3` and `degradation_slope_5`: linear regression slope of `lap_time_sec` vs `stint_lap_number` over last N clean laps in current stint.
- Computed on a clean-only sub-frame, then joined back so non-clean rows have null slope.
- Slope formula: `cov(x,y) / var(x)` evaluated as `(avg(xy) - avg(x)avg(y)) / (avg(x²) - avg(x)²)` over `rowsBetween(-(N-1), 0)`.
- `degradation_slope_5` is the canonical slope used for risk and baselines.

---

## 3. Baselines

### 3.1 Group hierarchy + fallback

Most-specific baseline group: `(year, race_round, compound, tyre_age_bucket)`. If `sample_count < MIN_BASELINE_SAMPLES (= 5)`, fall back in order:

1. `(year, race_round, compound)` — race + compound only
2. `(compound, tyre_age_bucket)` — across races
3. global clean-lap baseline

- **Why:** Spec lines 499–504. Single-race demo will hit fallback for sparse buckets (e.g., HARD 21+).
- **Implementation:** All four levels precomputed and joined; final pick via nested `coalesce(when(sample_count >= 5, …))`.

### 3.2 Baseline metrics per group

- `median_lap_time_sec`, `median_pace_delta`, `median_degradation_slope` (medians via `percentile_approx` for outlier robustness).
- `std_pace_delta`, `std_degradation_slope` (population std, used for z-score normalization).
- `sample_count` (gates fallback).

---

## 4. Risk score

### 4.1 Component normalization (z-score, clipped)

- `pace_component = clip(compound_baseline_delta / baseline_std_pace_delta, 0, 1)`
- `slope_component = clip(compound_baseline_slope / baseline_std_degradation_slope, 0, 1)`
- `field_component = clip(field_delta / field_std_pace_delta, 0, 1)`
- Each clipped to `[0, 1]`. If std == 0 → component = 0.

### 4.2 Weighted sum

```
risk_score = clip(0.4 * pace_component + 0.4 * slope_component + 0.2 * field_component, 0, 1)
```

- **Why these weights:** CLAUDE.md decision. Pace + slope dominate (driver-specific signals). Field is contextual (track conditions). Spec line 526–530 suggests this exact mix.
- **Why not ML:** Spec line 514. Transparent + explainable for grading.

### 4.3 Status mapping

| Status | Rule |
|--------|------|
| `INSUFFICIENT_DATA` | `clean_laps_in_stint_so_far < 3` (`STINT_MIN_CLEAN_LAPS`) |
| `LOW` | `risk_score < 0.40` |
| `MEDIUM` | `0.40 <= risk_score < 0.70` |
| `HIGH` | `risk_score >= 0.70` |

---

## 5. Alerts

Two rules, OR-combined:

1. **Consecutive HIGH:** `clean_lap AND risk_status == HIGH` for ≥ `ALERT_HIGH_CONSECUTIVE (= 2)` consecutive clean laps.
   - State stored in Redis hash `driver:{CODE}` field `consecutive_high`. Reset to 0 on any non-(clean+HIGH) lap.
2. **Fallback:** `risk_score >= 0.80` (`ALERT_FALLBACK_RISK`) on a single clean lap.

- **Why:** Spec lines 567–574. Consecutive rule reduces noise; fallback catches sudden severe degradation.
- **Storage:** `LPUSH alerts:log <json>` then `LTRIM alerts:log 0 199` (cap at 200).

---

## 6. Infrastructure

### 6.1 Kafka image: `apache/kafka:3.7.1` (KRaft single-node)

- **Original choice:** `bitnami/kafka:3.7`.
- **Switched to:** `apache/kafka:3.7.1` because Bitnami pulled the `3.7` tag from Docker Hub mid-project (manifest 404).
- **KRaft mode:** No Zookeeper. Single broker + controller (NodeId=1, ports 9092 client / 9093 controller). `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`.
- **Env-var prefix difference:** apache image uses `KAFKA_*`, bitnami used `KAFKA_CFG_*`. Console-consumer path: `/opt/kafka/bin/...` (was `/opt/bitnami/kafka/bin/...`).

### 6.2 Redis: `redis:7.4-alpine`

- AOF persistence (`--appendonly yes`).
- DB 0. `decode_responses=True` everywhere.
- Keys:
  - `driver:{CODE}` — hash, latest state per driver.
  - `history:{CODE}` — sorted set, score = `lap_number`, member = JSON snapshot.
  - `alerts:log` — list, LPUSH + LTRIM(200).
  - `race:current` — string, JSON `{race_name, year, last_lap, updated_at}`.

### 6.3 Spark-Kafka connector

- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5` matched to PySpark 3.5.5.
- Wired via `spark.jars.packages` from `config.SPARK_KAFKA_PACKAGE`. First consumer run downloads ~30 MB of jars.

### 6.4 Windows host caveat

- Spark needs `winutils.exe` + `HADOOP_HOME` for native IO. Alternative: run Spark in Docker if winutils unavailable.

---

## 7. Streaming consumer details

- `readStream` from Kafka with `startingOffsets=latest` and `failOnDataLoss=false`.
- JSON parsed via explicit `EVENT_SCHEMA` (StructType) — no schema inference, fixed contract with producer.
- `foreachBatch(write_batch_to_redis)` — one Redis pipeline per micro-batch. Drives all writes from the driver process (small data: ~20 rows/lap).
- Pre-read prior `consecutive_high` per driver before pipeline execute (Redis HGET inside the loop, before pipe.hset). Acceptable at 20 reads/batch.
- Checkpoint dir: `data/checkpoints/consumer` (gitignored).

---

## 8. Producer details

- Reads `race_replay_events.parquet`, filters `--race "<YEAR> <NAME>"`, sorts by (lap_number, driver_code).
- Iterates `groupby("lap_number")`; per lap sends one message per driver, then `flush()`, then `time.sleep(90 / speed)`.
- `--speed 20` → real-time-ish (~4.5 s per lap). `--speed 200` → fast smoke test.
- NaN floats serialized as `null` (not `NaN` — invalid JSON).
- Graceful exit code 2 if Kafka unreachable.

---

## 9. Dashboard

- `streamlit-autorefresh` at 2000 ms.
- Reads Redis on every refresh (no caching of state — only `get_redis()` connection cached via `@st.cache_resource`).
- Driver cards sorted descending by `risk_score`. Border color encodes status.
- Driver dropdown loads `history:{CODE}` zset → 4 plotly charts (lap_time, risk_score with HIGH/MED hlines, pace_delta, tyre_age).
- Alerts table shows last 50 from `alerts:log`.

---

## 10. Demo race

- **Primary:** 2024 Bahrain Grand Prix (round 1).
- **Why Bahrain:** Season opener, high tyre wear track, well-known compound choices, complete FastF1 data.
- **Backups:** 2024 Spanish GP, 2024 Hungarian GP. Configurable via `--race` arg or `config.DEMO_RACE`.

---

## 11. Verified numbers (2024 Bahrain)

| Metric | Value |
|--------|-------|
| Total laps ingested | 1129 |
| Drivers | 20 |
| Compounds used | SOFT (338), HARD (791) |
| Clean laps | 1024 / 1129 (90.7%) |
| Stints with <3 clean laps | 1 (HUL stint 1 SOFT) |
| Baseline groups | 10 (5 buckets × 2 compounds) |
| Risk distribution | LOW 543 / MEDIUM 224 / INSUFFICIENT_DATA 187 / HIGH 175 |

---

## 12. Conventions

- **Comments:** Only when WHY non-obvious. No what/how comments. No docstrings repeating identifier names.
- **Tests:** Spec scope only — `clean_lap` flag + risk thresholds. No coverage chasing.
- **Commits:** Conventional Commits, one per phase. No file enumeration in messages, no AI attribution.
- **Docs:** README (run order + config), STATE.md (resume point, <60 lines), CHANGELOG.md (phase log), DECISIONS.md (this file), CLAUDE.md (terse session memory).

---

## 13. Known limitations

- Single race per parquet — switching races requires re-running all batch jobs.
- No backfill for missed Kafka offsets (consumer starts at latest).
- Producer wall-clock pacing only; no event-time semantics in Spark consumer.
- Slope based on stint_lap_number (count) not actual stint elapsed time.
- Telemetry aggregates (speed/throttle/brake) not implemented — spec marks optional.
- `winutils.exe` requirement on Windows host for local PySpark.
