"""Phase 3: lap-level features.

Reads raw FastF1 laps, adds clean_lap flag, stint counters, tyre age bucket,
rolling pace stats (no future leakage), field median + delta. Writes Parquet.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

import config
from utils.logging import get_logger
from utils.spark import get_spark

log = get_logger("lap_features")


def add_clean_lap(df: DataFrame) -> DataFrame:
    excluded_chars = "".join(sorted(config.TRACK_STATUS_EXCLUDE))
    track_status_clean = F.col("track_status").isNull() | (
        ~F.col("track_status").rlike(f"[{excluded_chars}]")
    )
    return df.withColumn(
        "clean_lap",
        (F.col("lap_number") >= F.lit(config.CLEAN_LAP_MIN_LAP_NUMBER))
        & F.col("lap_time_sec").isNotNull()
        & F.col("pit_in_time").isNull()
        & F.col("pit_out_time").isNull()
        & F.coalesce(F.col("is_accurate"), F.lit(False))
        & track_status_clean,
    )


def add_stint_counters(df: DataFrame) -> DataFrame:
    w_stint = Window.partitionBy("year", "race_round", "driver_code", "stint").orderBy("lap_number")
    return (
        df.withColumn("stint_lap_number", F.row_number().over(w_stint))
        .withColumn(
            "clean_laps_in_stint_so_far",
            F.sum(F.col("clean_lap").cast(IntegerType())).over(
                w_stint.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )
    )


def add_tyre_age_bucket(df: DataFrame) -> DataFrame:
    col = F.col("tyre_life")
    expr = None
    for lo, hi in config.TYRE_AGE_BUCKETS:
        label = f"{lo}-{hi}" if hi < 999 else f"{lo}+"
        cond = (col >= F.lit(lo)) & (col <= F.lit(hi))
        expr = F.when(cond, F.lit(label)) if expr is None else expr.when(cond, F.lit(label))
    return df.withColumn("tyre_age_bucket", expr.otherwise(F.lit(None)))


def add_rolling_pace(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("year", "race_round", "driver_code", "stint").orderBy("lap_number")
    clean_time = F.when(F.col("clean_lap"), F.col("lap_time_sec"))
    return (
        df.withColumn("rolling_3_lap_avg", F.avg(clean_time).over(w.rowsBetween(-2, 0)))
        .withColumn("rolling_5_lap_avg", F.avg(clean_time).over(w.rowsBetween(-4, 0)))
        .withColumn("previous_5_lap_avg", F.avg(clean_time).over(w.rowsBetween(-5, -1)))
        .withColumn("pace_delta", F.col("lap_time_sec") - F.col("previous_5_lap_avg"))
    )


def add_field_median(df: DataFrame) -> DataFrame:
    clean_time = F.when(F.col("clean_lap"), F.col("lap_time_sec"))
    field = (
        df.groupBy("year", "race_round", "lap_number")
        .agg(F.percentile_approx(clean_time, 0.5).alias("field_median_lap_time"))
    )
    out = df.join(field, on=["year", "race_round", "lap_number"], how="left")
    return out.withColumn("field_delta", F.col("lap_time_sec") - F.col("field_median_lap_time"))


def build(df: DataFrame) -> DataFrame:
    df = add_clean_lap(df)
    df = add_stint_counters(df)
    df = add_tyre_age_bucket(df)
    df = add_rolling_pace(df)
    df = add_field_median(df)
    return df


def report(df: DataFrame) -> None:
    total = df.count()
    clean = df.filter(F.col("clean_lap")).count()
    log.info("Total rows: %d", total)
    log.info("Clean laps: %d (%.1f%%)", clean, 100.0 * clean / max(total, 1))

    log.info("Rows by compound:")
    df.groupBy("compound").count().orderBy(F.desc("count")).show(truncate=False)

    log.info("Rows by driver:")
    df.groupBy("driver_code").count().orderBy("driver_code").show(40, truncate=False)

    log.info("Stints with < %d clean laps:", config.STINT_MIN_CLEAN_LAPS)
    stint_clean = (
        df.groupBy("driver_code", "race_name", "stint", "compound")
        .agg(F.sum(F.col("clean_lap").cast(IntegerType())).alias("clean_count"))
        .filter(F.col("clean_count") < config.STINT_MIN_CLEAN_LAPS)
        .orderBy("driver_code", "stint")
    )
    stint_clean.show(40, truncate=False)


def _resolve_input_path() -> str:
    partitions = list(config.RAW_LAPS_DIR.glob("year=*/round=*/laps.parquet"))
    if partitions:
        return str(config.RAW_LAPS_DIR)
    if config.RAW_LAPS_PARQUET.exists():
        return str(config.RAW_LAPS_PARQUET)
    raise FileNotFoundError(
        f"No raw lap data found. Expected partitions under {config.RAW_LAPS_DIR} "
        f"or single file at {config.RAW_LAPS_PARQUET}."
    )


def main() -> None:
    spark = get_spark("f1-lap-features")
    in_path = _resolve_input_path()
    log.info("Reading: %s", in_path)
    raw = spark.read.parquet(in_path)

    feats = build(raw).cache()
    report(feats)

    out = str(config.LAP_FEATURES_PARQUET)
    feats.coalesce(1).write.mode("overwrite").parquet(out)
    log.info("Wrote -> %s", out)
    spark.stop()


if __name__ == "__main__":
    main()
