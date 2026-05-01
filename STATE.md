# STATE — F1 Tyre Degradation

Resume point. Update every session. Keep < 60 lines.

## Current phase

**ALL PHASES DONE.** End-to-end verified: ingest → features → baselines → Kafka → Spark Streaming → Redis → Streamlit.

## Phase status

- [x] Phase 1 — skeleton
- [x] Phase 2 — FastF1 ingestion (1129 rows verified)
- [x] Phase 3 — lap features (1024/1129 clean)
- [x] Phase 4 — baselines + replay events (1129 events, 10 baselines, risk LOW/MED/INSUF/HIGH = 543/224/187/175)
- [x] Phase 5 — Docker stack (apache/kafka:3.7.1 KRaft + redis:7.4-alpine)
- [x] Phase 6 — Kafka producer (verified 100 msgs)
- [x] Phase 7 — Spark Streaming consumer (verified 20 driver hashes + alerts)
- [x] Phase 8 — Streamlit dashboard (verified, no bugs)
- [x] Phase 9 — docs (README expanded, CHANGELOG created)

Detailed phase notes archived in `docs/CHANGELOG.md`.

## Next action

Demo + final commit. Suggested commit groups (Conventional Commits, one per phase):
```bash
git add config.py docker-compose.yml requirements.txt README.md .gitignore && git commit -m "chore: project skeleton"
git add ingestion/ utils/logging.py utils/time_utils.py && git commit -m "feat(ingest): FastF1 lap ingestion"
git add utils/spark.py features/build_lap_features.py && git commit -m "feat(features): lap features + clean_lap + rolling pace"
git add features/build_degradation_baselines.py && git commit -m "feat(features): degradation baselines + risk score"
git add streaming/kafka_lap_replay_producer.py && git commit -m "feat(streaming): Kafka lap replay producer"
git add streaming/spark_degradation_consumer.py && git commit -m "feat(streaming): Spark consumer + Redis writes + alerts"
git add dashboard/ && git commit -m "feat(dashboard): Streamlit live monitor"
git add docs/ STATE.md CLAUDE.md && git commit -m "docs: README + CHANGELOG"
```

Optional follow-ups:
- Add tests for clean_lap + risk threshold (per CLAUDE.md "test critical logic only").
- Try backup race: `python ingestion/ingest_fastf1_laps.py --race "Spanish Grand Prix"` then re-run features.
- Telemetry aggregates (speed/throttle/brake) — spec marks optional.

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
