# CLAUDE.md — F1 Tyre Degradation Monitoring

Project context for Claude Code. Read this first every session. Keep terse.

## Project

Near-real-time F1 tyre degradation + race pace anomaly monitor. Master's Big Data course demo. Lambda architecture: PySpark batch → Kafka replay → Spark Structured Streaming → Redis → Streamlit.

Spec: `F1_Tyre_Degradation_AI_Implementation_Prompt.md` (authoritative). Do not duplicate spec here — only deviations + state.

## Stack

- Python 3.11.9, PySpark 3.5.5
- FastF1, pandas, pyarrow, numpy
- kafka-python OR confluent-kafka, redis-py, streamlit, plotly
- Docker Compose: Kafka (KRaft mode, no Zookeeper), Redis
- Windows host. Spark needs `winutils.exe` + `HADOOP_HOME` OR run in Docker.

## Demo race

Primary: 2024 Bahrain GP. Backups: 2024 Spain, 2024 Hungary.

## Architecture decisions (locked)

- Kafka event = one per driver-lap (not raw telemetry rows).
- Risk precomputed in batch (`race_replay_events.parquet`); Spark Streaming parses + writes Redis. Acceptable per spec line 1032.
- Risk score normalization: z-score vs baseline std, clipped [0,1]. Components weighted 0.4/0.4/0.2 (pace/slope/field).
- TrackStatus parsed: 1=clear, 2=yellow, 4=SC, 5=red, 6=VSC deploy, 7=VSC. Exclude 4,5,6,7 from clean laps.
- Kafka Docker: bitnami/kafka KRaft (skip Zookeeper).
- Spark-Kafka package: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5`.
- Field median slope: precomputed in batch, attached to replay events.
- Alert state: Redis hash `driver:{CODE}` field `consecutive_high` for "HIGH × 2 laps" rule.

## Folder structure

See spec section "Required Project Structure" (line 776). Follow as-is.

## Run order

See spec section "Suggested Run Order" (line 1086).

## Session continuity

**Single source of truth: `STATE.md`** (project root).

Each session:
1. Read `CLAUDE.md` (this file) + `STATE.md` first.
2. Resume at `STATE.md` "Next action".
3. Update `STATE.md` after every meaningful change (file created, phase done, blocker hit).
4. Keep `STATE.md` < 60 lines. If growing, archive completed phases to `docs/CHANGELOG.md`.

Do NOT re-read `F1_Tyre_Degradation_AI_Implementation_Prompt.md` in full each session — grep for needed section.

## Conventions

- No comments in code unless WHY non-obvious.
- No README/docs unless spec phase requires it.
- Commit per phase boundary. Conventional Commits.
- Test critical logic only: `clean_lap` flag, risk score thresholds.
- Validation prints after each phase per spec.
