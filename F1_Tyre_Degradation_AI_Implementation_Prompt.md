# AI Implementation Prompt: Real-Time F1 Tyre Degradation and Race Pace Anomaly Monitoring

## Role

You are my senior Python, PySpark, Big Data, and software architecture assistant.

I am starting a new project from scratch and I want you to build it quickly, cleanly, and efficiently. I will use this project for a Master's-level Big Data course, so the implementation must clearly demonstrate batch processing, streaming, NoSQL state management, and a live dashboard.

Do not overengineer. Build the smallest robust version that works end-to-end and is easy to explain in a 15-minute presentation.

---

## Project Title

**F1 Telemetry Anomaly Detection and Predictive Maintenance Pipeline**

---

## Final Project Idea

Build a **near-real-time Formula 1 tyre degradation and race pace anomaly monitoring system**.

The system should process historical F1 lap and telemetry data, compute expected tyre degradation and race pace baselines, replay race events through Kafka, process them with Spark Structured Streaming, store live driver state in Redis, and visualize degradation risk in a Streamlit dashboard.

The project should answer:

> Can we monitor Formula 1 tyre degradation and race pace anomalies in near real time using lap timing, tyre stint, and telemetry data?

---

## Important Context

I previously tried direct mechanical DNF prediction, but it produced poor results because mechanical DNFs are rare, public F1 telemetry does not include internal engine/fault-code data, and pre-failure behavior is noisy.

For this new project, avoid that mistake.

Do **not** make the main project depend on predicting rare mechanical failures.

Instead, build a reliable monitoring system around data FastF1 is better suited for:

- lap times
- sector times
- tyre compound
- tyre life
- stint number
- pit stops
- clean laps
- driver pace
- race pace evolution
- telemetry aggregates such as speed, throttle, brake, RPM, and gear

This should still fit the project title because tyre degradation and abnormal pace loss are forms of anomaly detection and predictive-maintenance-style monitoring.

---

## Environment Constraints

Use:

- Python 3.11.9
- PySpark 3.5.5

Prefer:

- FastF1
- pandas
- pyarrow
- numpy
- scikit-learn only if needed
- kafka-python or confluent-kafka
- redis
- streamlit
- plotly
- Docker Compose for Kafka, Zookeeper, and Redis

Avoid unnecessary complex dependencies.

---

## Core Architecture

Build a Lambda-style Big Data architecture with three layers.

### 1. Batch Layer

Purpose:

> Process historical F1 data and compute lap-level features, tyre degradation baselines, and replay-ready event data.

Flow:

```text
FastF1 data
→ local Parquet storage
→ PySpark batch processing
→ lap/stint feature engineering
→ degradation baseline tables
→ race replay event dataset
```

Expected outputs:

```text
data/raw/
data/processed/lap_features.parquet
data/processed/degradation_baselines.parquet
data/processed/race_replay_events.parquet
```

---

### 2. Speed Layer

Purpose:

> Simulate a live race by replaying historical driver-lap events through Kafka and processing them with Spark Structured Streaming.

Flow:

```text
Kafka producer
→ Kafka topic: f1-lap-events
→ Spark Structured Streaming consumer
→ Redis
```

Spark Streaming should compute live degradation risk and write current driver state to Redis.

---

### 3. Serving Layer

Purpose:

> Show a live dashboard of driver tyre degradation and race pace anomaly risk.

Flow:

```text
Redis
→ Streamlit dashboard
```

Dashboard should show:

- race replay status
- current lap
- driver risk cards
- tyre compound
- tyre age
- lap time
- pace delta
- degradation slope
- risk score
- risk status
- alert log
- selected driver trend chart

---

## Do Not Build This as a Fragile ML Prediction Project

This project should not depend on getting high classifier scores.

The main deliverable is a working Big Data monitoring system.

Use transparent statistical monitoring and baseline comparison first.

ML can be optional, but it should not be required for the final demo.

Good framing:

> This system flags abnormal tyre degradation and race pace loss compared with expected stint behavior.

Bad framing:

> This system guarantees mechanical failure prediction.

---

## Data Source Plan

Use FastF1 as the primary source.

Collect race sessions for selected seasons. Start with a small reliable subset first, then expand.

Recommended implementation order:

1. First make the project work for one race, such as the 2024 Bahrain Grand Prix.
2. Then expand to multiple 2024 races.
3. Then optionally add 2023 for historical baseline comparison.

Do not try to ingest every available race before the pipeline works.

---

## Required Data Fields

The project needs a lap-level dataset with one row per driver-lap.

Target fields:

```text
year
race_round
race_name
session_name
driver_code
driver_number
team
constructor_key
lap_number
lap_time_sec
sector1_sec
sector2_sec
sector3_sec
compound
tyre_life
stint
pit_in_time
pit_out_time
track_status
is_accurate
clean_lap
```

Optional telemetry aggregates:

```text
speed_mean
speed_max
speed_std
throttle_mean
brake_pct
rpm_mean
rpm_std
gear_changes
```

If telemetry aggregation is slow, make it optional and keep the first working version lap-level.

---

## Dataset Issues to Handle From the Start

The previous project failed partly because the target was not well matched to the data. For this new project, be strict about data cleaning.

### 1. Pit Laps

Pit-in and pit-out laps are naturally slow and should not be treated as tyre degradation.

Exclude from clean baseline calculations:

```text
PitInTime is not null
PitOutTime is not null
```

### 2. Lap 1

Lap 1 has standing-start behavior, traffic, cold tyres, and incidents.

Exclude:

```text
LapNumber <= 1
```

### 3. Safety Car / VSC / Red Flag

Safety car and VSC laps are slow because the whole field is controlled.

Exclude or flag laps where TrackStatus indicates abnormal race control.

If TrackStatus is difficult to parse, at least preserve it and document the limitation.

### 4. Missing or Invalid Lap Times

Exclude missing LapTime from clean baseline calculations.

Do not impute lap times for degradation scoring.

### 5. Missing Tyre Data

If Compound, TyreLife, or Stint is missing, do not use that lap for tyre degradation baselines.

### 6. Short Stints

A stint with fewer than 3 clean laps cannot produce a reliable degradation slope.

For early stint laps, return:

```text
risk_status = "INSUFFICIENT_DATA"
```

instead of forcing a risk score.

### 7. Track Evolution and Fuel Burn

Do not claim the project measures pure tyre degradation.

Lap time changes due to fuel burn, track evolution, traffic, weather, and strategy.

Use safer wording:

> race pace degradation and tyre/stint anomaly monitoring

not:

> exact tyre degradation prediction

### 8. Different Tracks and Compounds

Do not use one global threshold for everything.

At minimum, compute baselines by:

```text
race_name / race_round
compound
tyre_age_bucket
```

---

## Clean Lap Definition

Create a `clean_lap` boolean.

Recommended first definition:

```text
clean_lap =
    LapNumber > 1
    AND LapTime is not null
    AND PitInTime is null
    AND PitOutTime is null
    AND Compound is not null
    AND Compound != "UNKNOWN"
    AND TyreLife is not null
    AND lap is not safety car / VSC / red flag if TrackStatus supports it
```

If some fields are unavailable, handle gracefully and document what was skipped.

---

## Feature Engineering Plan

### Lap-Time Features

Create:

```text
lap_time_sec
rolling_3_lap_avg
rolling_5_lap_avg
previous_5_lap_avg
pace_delta
```

Definitions:

```text
rolling_3_lap_avg = average of recent 3 clean laps for same driver/race/stint
rolling_5_lap_avg = average of recent 5 clean laps for same driver/race/stint
previous_5_lap_avg = average of previous 5 clean laps, excluding current lap
pace_delta = current lap time - previous_5_lap_avg
```

Avoid leakage:

- Do not use future laps to compute input features.
- Rolling averages should use current and/or previous laps only.
- For baseline creation, using historical data is okay, but streaming risk should not require future laps.

---

### Tyre/Stint Features

Create:

```text
compound
tyre_life
tyre_age_bucket
stint
stint_lap_number
clean_laps_in_stint_so_far
```

Recommended tyre age buckets:

```text
0-5
6-10
11-15
16-20
21+
```

---

### Degradation Features

Create:

```text
degradation_slope_3
degradation_slope_5
field_median_lap_time
field_delta
driver_vs_field_delta
compound_baseline_delta
compound_baseline_slope
```

Definitions:

```text
degradation_slope_3 = slope of lap_time_sec over last 3 clean laps in current stint
degradation_slope_5 = slope of lap_time_sec over last 5 clean laps in current stint
field_median_lap_time = median clean lap time for the field on the same race lap
field_delta = current driver lap time - field_median_lap_time
driver_vs_field_delta = driver degradation slope - field median degradation slope
```

Use simple, explainable calculations.

---

### Optional Telemetry Aggregates

If feasible, aggregate telemetry per driver-lap:

```text
speed_mean
speed_max
speed_std
throttle_mean
brake_pct
rpm_mean
rpm_std
gear_changes
```

Definition examples:

```text
brake_pct = percentage of telemetry rows in the lap where Brake > 0
gear_changes = number of times nGear changes during the lap
```

These are optional but useful for dashboard context.

Do not let telemetry aggregation block the core project.

---

## Baseline Strategy

Create historical/race-context baselines in PySpark.

### Compound + Tyre Age Baseline

Group by:

```text
year
race_round
compound
tyre_age_bucket
```

Compute:

```text
median_lap_time_sec
median_pace_delta
median_degradation_slope
std_pace_delta
std_degradation_slope
sample_count
```

### Field Baseline

For each race lap, compute:

```text
field_median_lap_time
field_median_pace_delta
field_median_degradation_slope
```

### Minimum Sample Rule

If a baseline group has very few samples, fall back to broader grouping:

1. race + compound + tyre_age_bucket
2. race + compound
3. compound + tyre_age_bucket
4. global clean-lap baseline

Document this fallback clearly.

---

## Risk Score Design

Use a transparent risk score.

Do not make the demo depend on an ML model.

Recommended risk components:

```text
pace_component
slope_component
field_component
```

Conceptual formula:

```text
risk_score =
    0.4 * normalized_pace_delta
  + 0.4 * normalized_degradation_slope
  + 0.2 * normalized_field_relative_loss
```

The exact normalization can be decided based on available data, but it must be explainable.

Example interpretation:

- pace_delta captures how much slower the driver is than their recent baseline.
- degradation_slope captures whether lap times are worsening over the stint.
- field_relative_loss captures whether the driver is losing time compared with the rest of the field.

Clamp final risk_score between 0 and 1.

---

## Risk Status

Use simple dashboard status labels:

```text
INSUFFICIENT_DATA
LOW
MEDIUM
HIGH
```

Suggested thresholds:

```text
INSUFFICIENT_DATA: fewer than 3 clean laps in current stint
LOW: risk_score < 0.40
MEDIUM: 0.40 <= risk_score < 0.70
HIGH: risk_score >= 0.70
```

Alert condition:

```text
HIGH risk for 2 consecutive clean driver-laps
```

Fallback alert condition:

```text
risk_score >= 0.80 for one lap
```

---

## Kafka Simulation Design

Stream one event per driver-lap, not every raw telemetry row.

This makes the demo more stable and easier to understand while still demonstrating streaming architecture.

Kafka topic:

```text
f1-lap-events
```

Kafka message schema:

```json
{
  "year": 2024,
  "race_round": 1,
  "race_name": "Bahrain Grand Prix",
  "driver_code": "ALO",
  "constructor_key": "aston_martin",
  "lap_number": 28,
  "lap_time_sec": 96.842,
  "sector1_sec": 31.1,
  "sector2_sec": 39.2,
  "sector3_sec": 26.5,
  "compound": "MEDIUM",
  "tyre_life": 17,
  "stint": 2,
  "speed_mean": 214.5,
  "throttle_mean": 72.1,
  "brake_pct": 14.8,
  "event_time": "2024-03-02T15:42:00"
}
```

Kafka producer requirements:

- read `data/processed/race_replay_events.parquet`
- filter selected race
- sort events by lap number and driver
- replay at configurable speed
- print progress to terminal
- fail gracefully if Kafka is unavailable

Suggested command:

```text
python streaming/kafka_lap_replay_producer.py --race "2024 Bahrain Grand Prix" --speed 20
```

---

## Spark Structured Streaming Design

Create a Spark consumer that:

1. Reads from Kafka topic `f1-lap-events`.
2. Parses JSON messages.
3. Loads degradation baselines.
4. Computes current risk score.
5. Assigns risk status.
6. Writes latest driver state to Redis.
7. Writes history points to Redis sorted sets.
8. Writes alerts to Redis list.

Redis keys:

```text
driver:{DRIVER_CODE}
history:{DRIVER_CODE}
alerts:log
race:current
```

Driver hash fields:

```text
driver_code
constructor_key
race_name
lap_number
compound
tyre_life
stint
lap_time_sec
pace_delta
degradation_slope
field_delta
risk_score
risk_status
timestamp
```

History sorted set member:

```json
{
  "lap": 28,
  "lap_time_sec": 96.842,
  "risk_score": 0.82,
  "pace_delta": 1.8,
  "tyre_life": 17,
  "compound": "MEDIUM"
}
```

Alert log entry:

```json
{
  "lap": 28,
  "driver_code": "ALO",
  "compound": "MEDIUM",
  "tyre_life": 17,
  "risk_score": 0.82,
  "reason": "Pace delta and degradation slope above expected baseline"
}
```

---

## Streamlit Dashboard Requirements

Create a dashboard that can be used during a 15-minute presentation.

Dashboard sections:

### 1. Header

Show:

```text
Project title
Selected race
Current replay lap
Replay status
Last update time
```

### 2. Driver Risk Cards

Each driver card should show:

```text
Driver
Team/constructor
Compound
Tyre age
Lap time
Pace delta
Risk score
Risk status
```

Use clear colors:

```text
LOW = green
MEDIUM = yellow/orange
HIGH = red
INSUFFICIENT_DATA = gray
```

### 3. Selected Driver Trend

Allow selecting one driver.

Show charts for:

- lap time over race
- risk score over race
- pace delta over race
- tyre age over stint

### 4. Alert Log

Show table:

```text
Lap
Driver
Compound
Tyre Age
Risk Score
Reason
Timestamp
```

### 5. Optional Compound Comparison

Show average degradation trend by compound if easy.

Do not spend too much time on this if the main dashboard is not done.

---

## Required Project Structure

Use a clean, simple folder structure.

Suggested:

```text
f1-tyre-degradation-monitoring/
│
├── README.md
├── requirements.txt
├── docker-compose.yml
├── config.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── cache/
│
├── ingestion/
│   └── ingest_fastf1_laps.py
│
├── features/
│   ├── build_lap_features.py
│   └── build_degradation_baselines.py
│
├── streaming/
│   ├── kafka_lap_replay_producer.py
│   └── spark_degradation_consumer.py
│
├── dashboard/
│   └── app.py
│
├── utils/
│   ├── spark.py
│   ├── redis_client.py
│   ├── logging.py
│   └── time_utils.py
│
├── docs/
│   ├── PROJECT_UNDERSTANDING.md
│   ├── ARCHITECTURE.md
│   ├── DATA_DECISIONS.md
│   └── DEMO_SCRIPT.md
│
└── tests/
    └── optional basic tests
```

You can adjust names if needed, but keep it simple.

---

## Implementation Phases

Build in phases. After each phase, stop and report:

- what was created
- what command to run
- what output should exist
- how to verify it worked

---

### Phase 1: Project Skeleton

Create:

```text
README.md
requirements.txt
docker-compose.yml
config.py
folder structure
```

Do not write complex logic yet.

---

### Phase 2: FastF1 Lap Data Ingestion

Create:

```text
ingestion/ingest_fastf1_laps.py
```

Goal:

- Load one race first.
- Save lap data to Parquet.
- Include tyre compound, tyre life, stint, lap time, pit markers, track status.
- Cache FastF1 data locally.

Output:

```text
data/raw/fastf1_laps.parquet
```

Validation:

- print number of laps
- print drivers
- print missing LapTime count
- print missing Compound/TyreLife count

---

### Phase 3: Lap Feature Builder

Create:

```text
features/build_lap_features.py
```

Goal:

- Create clean_lap flag.
- Convert lap/sector times to seconds.
- Add stint_lap_number.
- Optionally aggregate telemetry per lap.
- Save processed lap features.

Output:

```text
data/processed/lap_features.parquet
```

Validation:

- total rows
- clean lap count
- rows by compound
- rows by driver
- stints with fewer than 3 clean laps

---

### Phase 4: Baseline Builder

Create:

```text
features/build_degradation_baselines.py
```

Goal:

- Compute rolling pace features.
- Compute degradation slopes.
- Compute race/compound/tyre-age baselines.
- Compute risk score for historical replay events.
- Save replay event dataset.

Outputs:

```text
data/processed/degradation_baselines.parquet
data/processed/race_replay_events.parquet
```

Validation:

- show top 10 high-risk laps
- show sample driver stint with risk score progression
- verify risk resets or drops after pit stops if possible

---

### Phase 5: Kafka + Redis Docker Setup

Create:

```text
docker-compose.yml
```

Services:

- zookeeper
- kafka
- redis

Validation:

```text
docker-compose up -d
```

Then verify:

- Kafka is running
- Redis is running
- no port conflicts

Use common local ports:

```text
Kafka: 9092
Redis: 6379
```

---

### Phase 6: Kafka Producer

Create:

```text
streaming/kafka_lap_replay_producer.py
```

Goal:

- Read race replay events.
- Filter by race.
- Send JSON messages to Kafka.
- Allow configurable replay speed.

Validation:

- producer prints sent lap/driver events
- Kafka topic receives messages

---

### Phase 7: Spark Streaming Consumer

Create:

```text
streaming/spark_degradation_consumer.py
```

Goal:

- Read Kafka stream.
- Parse messages.
- Compute or pass through risk score.
- Write to Redis.
- Create alert log.

Validation:

- Redis keys appear:
  - `driver:*`
  - `history:*`
  - `alerts:log`
- Values update as producer runs.

Important:

If computing risk fully inside streaming is too complex, it is acceptable for the first demo to precompute risk in `race_replay_events.parquet` during batch processing and have Spark Streaming parse and write it live to Redis.

But still keep Spark Streaming in the loop.

---

### Phase 8: Streamlit Dashboard

Create:

```text
dashboard/app.py
```

Goal:

- Read Redis every 1-2 seconds.
- Display driver cards.
- Display selected driver chart.
- Display alert log.

Validation:

- dashboard updates live while producer and consumer run
- risk cards change by lap
- alerts appear

---

### Phase 9: Documentation

Create/update:

```text
docs/PROJECT_UNDERSTANDING.md
docs/ARCHITECTURE.md
docs/DATA_DECISIONS.md
docs/DEMO_SCRIPT.md
README.md
```

Documentation should explain:

- project goal
- why tyre degradation instead of mechanical DNF
- architecture
- data decisions
- clean lap rules
- risk score design
- run instructions
- demo script

---

## Suggested Run Order

Final expected run order:

```text
# 1. Ingest FastF1 lap data
python ingestion/ingest_fastf1_laps.py

# 2. Build lap features
python features/build_lap_features.py

# 3. Build degradation baselines and replay events
python features/build_degradation_baselines.py

# 4. Start services
docker-compose up -d

# 5. Start Spark streaming consumer
python streaming/spark_degradation_consumer.py

# 6. Start Kafka producer
python streaming/kafka_lap_replay_producer.py --race "2024 Bahrain Grand Prix" --speed 20

# 7. Start dashboard
streamlit run dashboard/app.py
```

---

## Demo Requirements

The demo must work reliably.

Recommended demo race:

- Start with 2024 Bahrain Grand Prix or another race where FastF1 tyre/lap data is complete.
- Choose the race after checking data completeness.
- The dashboard should show at least one driver reaching MEDIUM or HIGH risk.

If no natural high-risk event appears, adjust risk threshold carefully and document that thresholds are chosen for monitoring sensitivity.

Do not fake data unless clearly marked as synthetic demo mode.

---

## Report/Presentation Framing

Use this story:

> We built a Lambda-style Big Data pipeline for Formula 1 telemetry anomaly monitoring. The system uses PySpark to process historical lap and tyre data, computes expected race pace and tyre degradation baselines, replays historical race events through Kafka, processes them with Spark Structured Streaming, stores live driver state in Redis, and displays degradation risk in Streamlit. The final system focuses on tyre degradation and race pace anomalies because these are better supported by public FastF1 data than rare mechanical failure prediction.

Emphasize:

- volume: many race laps and optional telemetry aggregates
- velocity: Kafka replay simulates live race events
- variety: lap times, tyre compound, stint, sector times, telemetry summaries
- batch: PySpark baselines
- streaming: Spark Structured Streaming
- NoSQL: Redis
- serving: Streamlit dashboard

---

## What Not to Do

Do not:

- build a fragile rare-failure predictor
- rely on one low-performing ML score
- include pit laps in degradation baselines
- include safety car laps as degradation
- use future laps in input features
- make the dashboard too complex before core streaming works
- process all races before one race works end-to-end
- hide data limitations

---

## Success Criteria

The project is successful if:

1. FastF1 lap data is ingested and stored as Parquet.
2. PySpark creates clean lap features and degradation baselines.
3. Kafka replays driver-lap events.
4. Spark Structured Streaming consumes events.
5. Redis stores live driver risk state.
6. Streamlit dashboard updates in near real time.
7. The system flags high degradation risk based on transparent baseline comparisons.
8. The project can be explained clearly in a 15-minute presentation.

---

## Final Instruction

Start by inspecting the project folder, then build the project phase by phase.

Before implementing each phase, briefly state:

- files to create or modify
- assumptions
- expected output
- validation checks

After implementing each phase, tell me:

- what changed
- what command to run
- how to verify success
- what to do next

Prioritize a working end-to-end demo over unnecessary complexity.
