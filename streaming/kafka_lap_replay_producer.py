"""Phase 6: Kafka producer that replays race events from parquet at configurable speed.

Reads race_replay_events.parquet, filters race, sorts by lap+driver, sleeps between
lap groups based on speed multiplier. One Kafka message per driver-lap.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from utils.logging import get_logger

log = get_logger("producer")

PAYLOAD_FIELDS = [
    "year", "race_round", "race_name", "driver_code", "driver_number", "team",
    "constructor_key", "lap_number", "lap_time_sec", "sector1_sec", "sector2_sec",
    "sector3_sec", "compound", "tyre_life", "stint", "stint_lap_number",
    "clean_lap", "clean_laps_in_stint_so_far", "tyre_age_bucket",
    "rolling_3_lap_avg", "rolling_5_lap_avg", "previous_5_lap_avg", "pace_delta",
    "field_median_lap_time", "field_delta", "degradation_slope_3", "degradation_slope_5",
    "driver_vs_field_delta", "compound_baseline_delta", "compound_baseline_slope",
    "pace_component", "slope_component", "field_component",
    "risk_score", "risk_status",
    "air_temp_c", "track_temp_c", "humidity", "rainfall", "wind_speed",
    "anomaly_score", "is_anomaly",
]

TYPICAL_LAP_SECONDS = 90.0


def _to_jsonable(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (pd.Timestamp,)):
        return v.isoformat()
    if hasattr(v, "item"):
        try:
            v = v.item()
        except Exception:
            pass
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def build_payload(row: pd.Series, event_time: datetime) -> dict:
    payload = {}
    for f in PAYLOAD_FIELDS:
        if f in row.index:
            payload[f] = _to_jsonable(row[f])
    payload["event_time"] = event_time.isoformat()
    return payload


def make_producer(bootstrap: str):
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
            acks="all",
            linger_ms=10,
        )
    except NoBrokersAvailable:
        log.error("Kafka unavailable at %s. Is docker-compose up?", bootstrap)
        sys.exit(2)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay race events to Kafka.")
    p.add_argument("--race", type=str, default=f"{config.DEMO_SEASON} {config.DEMO_RACE}")
    p.add_argument("--input", type=Path, default=config.RACE_REPLAY_EVENTS_PARQUET)
    p.add_argument("--bootstrap", type=str, default=config.KAFKA_BOOTSTRAP_SERVERS)
    p.add_argument("--topic", type=str, default=config.KAFKA_TOPIC)
    p.add_argument("--speed", type=float, default=config.DEFAULT_REPLAY_SPEED)
    p.add_argument("--max-laps", type=int, default=None)
    return p.parse_args()


def filter_race(df: pd.DataFrame, race_arg: str) -> pd.DataFrame:
    parts = race_arg.split(" ", 1)
    if len(parts) == 2 and parts[0].isdigit():
        year = int(parts[0])
        name = parts[1]
        m = (df["year"] == year) & (df["race_name"] == name)
    else:
        m = df["race_name"] == race_arg
    out = df[m].copy()
    if out.empty:
        raise SystemExit(f"No events match race={race_arg!r}")
    return out


def main() -> None:
    args = parse_args()
    log.info("Loading: %s", args.input)
    df = pd.read_parquet(args.input)
    df = filter_race(df, args.race)
    df = df.sort_values(["lap_number", "driver_code"]).reset_index(drop=True)
    log.info("Race: %s | rows: %d | drivers: %d | laps: %d..%d",
             args.race, len(df), df["driver_code"].nunique(),
             int(df["lap_number"].min()), int(df["lap_number"].max()))

    producer = make_producer(args.bootstrap)
    log.info("Connected. topic=%s speed=%sx", args.topic, args.speed)

    sleep_per_lap = TYPICAL_LAP_SECONDS / max(args.speed, 0.001)
    sent = 0
    start = time.time()
    laps_iter = df.groupby("lap_number", sort=True)
    for lap_no, lap_df in laps_iter:
        if args.max_laps is not None and lap_no > args.max_laps:
            break
        now = datetime.now(timezone.utc)
        for _, row in lap_df.iterrows():
            payload = build_payload(row, now)
            key = str(payload["driver_code"])
            producer.send(args.topic, key=key, value=payload)
            sent += 1
        producer.flush()
        elapsed = time.time() - start
        log.info("Lap %d sent: %d drivers (total=%d, elapsed=%.1fs)",
                 lap_no, len(lap_df), sent, elapsed)
        time.sleep(sleep_per_lap)

    producer.flush()
    producer.close()
    log.info("Done. total messages sent: %d", sent)


if __name__ == "__main__":
    main()
