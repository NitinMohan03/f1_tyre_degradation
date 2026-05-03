"""Phase 7: Spark Structured Streaming consumer.

Reads Kafka topic f1-lap-events, parses JSON, writes per-driver state, history,
and alerts to Redis. Risk is precomputed in batch (per architecture decision) so
Spark side only routes + applies the consecutive-HIGH alert rule via Redis hash.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

import config
from utils.logging import get_logger
from utils.spark import get_spark

log = get_logger("consumer")

EVENT_SCHEMA = StructType([
    StructField("year", IntegerType()),
    StructField("race_round", IntegerType()),
    StructField("race_name", StringType()),
    StructField("driver_code", StringType()),
    StructField("driver_number", StringType()),
    StructField("team", StringType()),
    StructField("constructor_key", StringType()),
    StructField("lap_number", LongType()),
    StructField("lap_time_sec", DoubleType()),
    StructField("sector1_sec", DoubleType()),
    StructField("sector2_sec", DoubleType()),
    StructField("sector3_sec", DoubleType()),
    StructField("compound", StringType()),
    StructField("tyre_life", DoubleType()),
    StructField("stint", LongType()),
    StructField("stint_lap_number", LongType()),
    StructField("clean_lap", BooleanType()),
    StructField("clean_laps_in_stint_so_far", LongType()),
    StructField("tyre_age_bucket", StringType()),
    StructField("rolling_3_lap_avg", DoubleType()),
    StructField("rolling_5_lap_avg", DoubleType()),
    StructField("previous_5_lap_avg", DoubleType()),
    StructField("pace_delta", DoubleType()),
    StructField("field_median_lap_time", DoubleType()),
    StructField("field_delta", DoubleType()),
    StructField("degradation_slope_3", DoubleType()),
    StructField("degradation_slope_5", DoubleType()),
    StructField("driver_vs_field_delta", DoubleType()),
    StructField("compound_baseline_delta", DoubleType()),
    StructField("compound_baseline_slope", DoubleType()),
    StructField("pace_component", DoubleType()),
    StructField("slope_component", DoubleType()),
    StructField("field_component", DoubleType()),
    StructField("risk_score", DoubleType()),
    StructField("risk_status", StringType()),
    StructField("air_temp_c", DoubleType()),
    StructField("track_temp_c", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("rainfall", BooleanType()),
    StructField("wind_speed", DoubleType()),
    StructField("anomaly_score", DoubleType()),
    StructField("is_anomaly", BooleanType()),
    StructField("event_time", StringType()),
])


def build_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
    )


def _f(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.6f}"
    return str(v)


def write_batch_to_redis(batch_df: DataFrame, batch_id: int) -> None:
    import redis

    rows = batch_df.collect()
    if not rows:
        return

    r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, decode_responses=True)
    pipe = r.pipeline()
    alert_count = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for row in rows:
        d = row.asDict()
        code = d["driver_code"]
        if not code:
            continue
        driver_key = config.REDIS_KEY_DRIVER.format(driver_code=code)
        history_key = config.REDIS_KEY_HISTORY.format(driver_code=code)

        prior = r.hget(driver_key, "consecutive_high")
        prior_n = int(prior) if prior and prior.isdigit() else 0
        is_high = d.get("risk_status") == "HIGH"
        is_clean = bool(d.get("clean_lap"))
        new_n = prior_n + 1 if (is_high and is_clean) else 0

        hash_fields = {
            "driver_code": _f(code),
            "constructor_key": _f(d.get("constructor_key")),
            "race_name": _f(d.get("race_name")),
            "lap_number": _f(d.get("lap_number")),
            "compound": _f(d.get("compound")),
            "tyre_life": _f(d.get("tyre_life")),
            "stint": _f(d.get("stint")),
            "lap_time_sec": _f(d.get("lap_time_sec")),
            "pace_delta": _f(d.get("pace_delta")),
            "degradation_slope": _f(d.get("degradation_slope_5")),
            "field_delta": _f(d.get("field_delta")),
            "risk_score": _f(d.get("risk_score")),
            "risk_status": _f(d.get("risk_status")),
            "air_temp_c": _f(d.get("air_temp_c")),
            "track_temp_c": _f(d.get("track_temp_c")),
            "humidity": _f(d.get("humidity")),
            "rainfall": _f(d.get("rainfall")),
            "wind_speed": _f(d.get("wind_speed")),
            "anomaly_score": _f(d.get("anomaly_score")),
            "is_anomaly": _f(d.get("is_anomaly")),
            "consecutive_high": str(new_n),
            "timestamp": now_iso,
        }
        pipe.hset(driver_key, mapping=hash_fields)

        history_member = json.dumps({
            "lap": d.get("lap_number"),
            "lap_time_sec": d.get("lap_time_sec"),
            "risk_score": d.get("risk_score"),
            "pace_delta": d.get("pace_delta"),
            "tyre_life": d.get("tyre_life"),
            "compound": d.get("compound"),
            "risk_status": d.get("risk_status"),
        })
        pipe.zadd(history_key, {history_member: float(d.get("lap_number") or 0)})

        risk_score = d.get("risk_score") or 0.0
        consecutive_alert = is_clean and is_high and new_n >= config.ALERT_HIGH_CONSECUTIVE
        fallback_alert = is_clean and (risk_score >= config.ALERT_FALLBACK_RISK)
        ml_only_alert = (
            is_clean
            and bool(d.get("is_anomaly"))
            and not is_high
            and not fallback_alert
        )
        if consecutive_alert or fallback_alert or ml_only_alert:
            if consecutive_alert:
                reason = f"HIGH risk for {new_n} consecutive clean laps"
            elif fallback_alert:
                reason = f"Risk score {risk_score:.2f} >= fallback threshold {config.ALERT_FALLBACK_RISK}"
            else:
                reason = f"ML anomaly (score={d.get('anomaly_score')}) without rule-based HIGH"
            alert = {
                "lap": d.get("lap_number"),
                "driver_code": code,
                "compound": d.get("compound"),
                "tyre_life": d.get("tyre_life"),
                "risk_score": risk_score,
                "reason": reason,
                "timestamp": now_iso,
            }
            pipe.lpush(config.REDIS_KEY_ALERTS, json.dumps(alert))
            pipe.ltrim(config.REDIS_KEY_ALERTS, 0, 199)
            alert_count += 1

    last = rows[-1].asDict()
    pipe.set(
        config.REDIS_KEY_RACE,
        json.dumps({
            "race_name": last.get("race_name"),
            "year": last.get("year"),
            "last_lap": last.get("lap_number"),
            "updated_at": now_iso,
        }),
    )
    pipe.execute()
    log.info("batch=%d rows=%d alerts=%d", batch_id, len(rows), alert_count)


def main() -> None:
    spark = get_spark(
        "f1-degradation-consumer",
        extra_packages=[config.SPARK_KAFKA_PACKAGE],
    )

    events = build_stream(spark)
    query = (
        events.writeStream
        .foreachBatch(write_batch_to_redis)
        .outputMode("append")
        .option("checkpointLocation", str(config.DATA_DIR / "checkpoints" / "consumer"))
        .start()
    )
    log.info("Streaming. topic=%s redis=%s:%d", config.KAFKA_TOPIC, config.REDIS_HOST, config.REDIS_PORT)
    query.awaitTermination()


if __name__ == "__main__":
    main()
