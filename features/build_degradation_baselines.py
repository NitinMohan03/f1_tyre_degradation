"""Phase 4: degradation slopes, baselines, race replay events with risk score.

Reads lap_features.parquet. Adds degradation_slope_3/5 (rolling regression on clean laps),
compound + tyre-age baselines with fallback, field baselines per race lap, and risk score
components. Writes degradation_baselines.parquet + race_replay_events.parquet.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import config
from utils.logging import get_logger
from utils.spark import get_spark

log = get_logger("baselines")

MIN_BASELINE_SAMPLES = 5


def _rolling_slope(df: DataFrame, window: Window, n: int, col: str) -> DataFrame:
    x = F.col("stint_lap_number").cast("double")
    y = F.col("lap_time_sec")
    w = window.rowsBetween(-(n - 1), 0)
    cnt = F.count(y).over(w)
    mean_x = F.avg(x).over(w)
    mean_y = F.avg(y).over(w)
    mean_xy = F.avg(x * y).over(w)
    mean_xx = F.avg(x * x).over(w)
    var_x = mean_xx - mean_x * mean_x
    cov_xy = mean_xy - mean_x * mean_y
    slope = F.when(cnt >= n, F.when(var_x != 0, cov_xy / var_x)).otherwise(F.lit(None))
    return df.withColumn(col, slope)


def add_degradation_slopes(df: DataFrame) -> DataFrame:
    clean = df.filter(F.col("clean_lap"))
    w = Window.partitionBy("year", "race_round", "driver_code", "stint").orderBy("lap_number")
    clean = _rolling_slope(clean, w, 3, "degradation_slope_3")
    clean = _rolling_slope(clean, w, 5, "degradation_slope_5")
    slope_cols = clean.select(
        "year", "race_round", "driver_code", "stint", "lap_number",
        "degradation_slope_3", "degradation_slope_5",
    )
    return df.join(
        slope_cols,
        on=["year", "race_round", "driver_code", "stint", "lap_number"],
        how="left",
    )


def compound_age_baselines(df: DataFrame) -> DataFrame:
    clean = df.filter(F.col("clean_lap"))
    return (
        clean.groupBy("year", "race_round", "compound", "tyre_age_bucket")
        .agg(
            F.percentile_approx("lap_time_sec", 0.5).alias("median_lap_time_sec"),
            F.percentile_approx("pace_delta", 0.5).alias("median_pace_delta"),
            F.percentile_approx("degradation_slope_5", 0.5).alias("median_degradation_slope"),
            F.stddev_pop("pace_delta").alias("std_pace_delta"),
            F.stddev_pop("degradation_slope_5").alias("std_degradation_slope"),
            F.count("*").alias("sample_count"),
        )
    )


def add_field_baselines(df: DataFrame) -> DataFrame:
    clean_slope = F.when(F.col("clean_lap"), F.col("degradation_slope_5"))
    clean_pace = F.when(F.col("clean_lap"), F.col("pace_delta"))
    field = (
        df.groupBy("year", "race_round", "lap_number")
        .agg(
            F.percentile_approx(clean_slope, 0.5).alias("field_median_degradation_slope"),
            F.percentile_approx(clean_pace, 0.5).alias("field_median_pace_delta"),
            F.stddev_pop(clean_slope).alias("field_std_degradation_slope"),
            F.stddev_pop(clean_pace).alias("field_std_pace_delta"),
        )
    )
    out = df.join(field, on=["year", "race_round", "lap_number"], how="left")
    out = out.withColumn(
        "driver_vs_field_delta",
        F.col("degradation_slope_5") - F.col("field_median_degradation_slope"),
    )
    return out


def attach_baselines(df: DataFrame, baselines: DataFrame) -> DataFrame:
    specific = baselines.alias("b1")
    by_race_compound = (
        baselines.groupBy("year", "race_round", "compound")
        .agg(
            F.avg("median_pace_delta").alias("median_pace_delta_rc"),
            F.avg("median_degradation_slope").alias("median_degradation_slope_rc"),
            F.avg("std_pace_delta").alias("std_pace_delta_rc"),
            F.avg("std_degradation_slope").alias("std_degradation_slope_rc"),
            F.sum("sample_count").alias("sample_count_rc"),
        )
    )
    by_compound_age = (
        baselines.groupBy("compound", "tyre_age_bucket")
        .agg(
            F.avg("median_pace_delta").alias("median_pace_delta_ca"),
            F.avg("median_degradation_slope").alias("median_degradation_slope_ca"),
            F.avg("std_pace_delta").alias("std_pace_delta_ca"),
            F.avg("std_degradation_slope").alias("std_degradation_slope_ca"),
            F.sum("sample_count").alias("sample_count_ca"),
        )
    )
    global_b = baselines.agg(
        F.avg("median_pace_delta").alias("median_pace_delta_g"),
        F.avg("median_degradation_slope").alias("median_degradation_slope_g"),
        F.avg("std_pace_delta").alias("std_pace_delta_g"),
        F.avg("std_degradation_slope").alias("std_degradation_slope_g"),
    )

    out = (
        df.join(specific, on=["year", "race_round", "compound", "tyre_age_bucket"], how="left")
        .join(by_race_compound, on=["year", "race_round", "compound"], how="left")
        .join(by_compound_age, on=["compound", "tyre_age_bucket"], how="left")
        .crossJoin(global_b)
    )

    def pick(specific_col: str, rc_col: str, ca_col: str, g_col: str):
        return F.coalesce(
            F.when(F.col("sample_count") >= MIN_BASELINE_SAMPLES, F.col(specific_col)),
            F.when(F.col("sample_count_rc") >= MIN_BASELINE_SAMPLES, F.col(rc_col)),
            F.when(F.col("sample_count_ca") >= MIN_BASELINE_SAMPLES, F.col(ca_col)),
            F.col(g_col),
        )

    out = (
        out.withColumn(
            "baseline_pace_delta",
            pick("median_pace_delta", "median_pace_delta_rc", "median_pace_delta_ca", "median_pace_delta_g"),
        )
        .withColumn(
            "baseline_degradation_slope",
            pick(
                "median_degradation_slope", "median_degradation_slope_rc",
                "median_degradation_slope_ca", "median_degradation_slope_g",
            ),
        )
        .withColumn(
            "baseline_std_pace_delta",
            pick("std_pace_delta", "std_pace_delta_rc", "std_pace_delta_ca", "std_pace_delta_g"),
        )
        .withColumn(
            "baseline_std_degradation_slope",
            pick(
                "std_degradation_slope", "std_degradation_slope_rc",
                "std_degradation_slope_ca", "std_degradation_slope_g",
            ),
        )
        .withColumn("compound_baseline_delta", F.col("pace_delta") - F.col("baseline_pace_delta"))
        .withColumn(
            "compound_baseline_slope",
            F.col("degradation_slope_5") - F.col("baseline_degradation_slope"),
        )
    )

    drop_cols = [
        "median_lap_time_sec", "median_pace_delta", "median_degradation_slope",
        "std_pace_delta", "std_degradation_slope", "sample_count",
        "median_pace_delta_rc", "median_degradation_slope_rc",
        "std_pace_delta_rc", "std_degradation_slope_rc", "sample_count_rc",
        "median_pace_delta_ca", "median_degradation_slope_ca",
        "std_pace_delta_ca", "std_degradation_slope_ca", "sample_count_ca",
        "median_pace_delta_g", "median_degradation_slope_g",
        "std_pace_delta_g", "std_degradation_slope_g",
    ]
    return out.drop(*drop_cols)


def add_risk_score(df: DataFrame) -> DataFrame:
    def z_clip(numerator, std):
        z = F.when(std > 0, numerator / std).otherwise(F.lit(0.0))
        return F.greatest(F.lit(0.0), F.least(F.lit(1.0), z))

    pace_z = z_clip(F.col("compound_baseline_delta"), F.col("baseline_std_pace_delta"))
    slope_z = z_clip(F.col("compound_baseline_slope"), F.col("baseline_std_degradation_slope"))
    field_z = z_clip(F.col("field_delta"), F.col("field_std_pace_delta"))

    risk = (
        F.lit(config.RISK_WEIGHT_PACE) * pace_z
        + F.lit(config.RISK_WEIGHT_SLOPE) * slope_z
        + F.lit(config.RISK_WEIGHT_FIELD) * field_z
    )
    risk = F.greatest(F.lit(0.0), F.least(F.lit(1.0), risk))

    out = (
        df.withColumn("pace_component", pace_z)
        .withColumn("slope_component", slope_z)
        .withColumn("field_component", field_z)
        .withColumn("risk_score", risk)
    )

    insufficient = F.col("clean_laps_in_stint_so_far") < F.lit(config.STINT_MIN_CLEAN_LAPS)
    status = (
        F.when(insufficient, F.lit("INSUFFICIENT_DATA"))
        .when(F.col("risk_score") >= F.lit(config.RISK_THRESHOLD_HIGH), F.lit("HIGH"))
        .when(F.col("risk_score") >= F.lit(config.RISK_THRESHOLD_MEDIUM), F.lit("MEDIUM"))
        .otherwise(F.lit("LOW"))
    )
    return out.withColumn("risk_status", status)


def report(events: DataFrame, baselines: DataFrame) -> None:
    log.info("Replay events rows: %d", events.count())
    log.info("Baselines rows: %d", baselines.count())
    log.info("Risk status distribution:")
    events.groupBy("risk_status").count().orderBy(F.desc("count")).show(truncate=False)
    log.info("Slope sample (first 10 clean laps):")
    (
        events.filter(F.col("clean_lap") & F.col("degradation_slope_5").isNotNull())
        .select(
            "driver_code", "stint", "lap_number", "stint_lap_number",
            "lap_time_sec", "degradation_slope_5", "risk_score", "risk_status",
        )
        .orderBy("driver_code", "lap_number").show(10, truncate=False)
    )


def _score_anomaly(replay_pdf):
    """Add anomaly_score + is_anomaly to a pandas frame using the persisted model.

    Returns the input dataframe with two new columns. If the model file is
    missing or required feature columns are absent, fills nulls and warns.
    """
    import numpy as np

    if not config.ANOMALY_MODEL_PATH.exists():
        log.warning(
            "Anomaly model not found at %s. Skipping anomaly scoring. "
            "Run features/train_anomaly_model.py first.",
            config.ANOMALY_MODEL_PATH,
        )
        replay_pdf["anomaly_score"] = np.nan
        replay_pdf["is_anomaly"] = False
        return replay_pdf

    import joblib
    payload = joblib.load(config.ANOMALY_MODEL_PATH)
    model = payload["model"]
    feature_cols = payload["feature_cols"]

    missing = [c for c in feature_cols if c not in replay_pdf.columns]
    if missing:
        log.warning("Replay frame missing model features %s. Anomaly score=null.", missing)
        replay_pdf["anomaly_score"] = np.nan
        replay_pdf["is_anomaly"] = False
        return replay_pdf

    X = replay_pdf[feature_cols].to_numpy(dtype=np.float64)
    valid_mask = ~np.isnan(X).any(axis=1)
    scores = np.full(len(X), np.nan)
    preds = np.zeros(len(X), dtype=bool)
    if valid_mask.any():
        Xv = X[valid_mask]
        scores[valid_mask] = model.score_samples(Xv)
        preds[valid_mask] = (model.predict(Xv) == -1)
    replay_pdf = replay_pdf.copy()
    replay_pdf["anomaly_score"] = scores
    replay_pdf["is_anomaly"] = preds
    log.info(
        "Anomaly scoring: scored=%d anomalies=%d (%.2f%%)",
        int(valid_mask.sum()), int(preds.sum()),
        100.0 * preds.sum() / max(valid_mask.sum(), 1),
    )
    return replay_pdf


def main() -> None:
    spark = get_spark("f1-degradation-baselines")
    log.info("Reading: %s", config.LAP_FEATURES_PARQUET)
    df = spark.read.parquet(str(config.LAP_FEATURES_PARQUET))

    df = add_degradation_slopes(df).cache()
    baselines = compound_age_baselines(df).cache()
    df = add_field_baselines(df)
    df = attach_baselines(df, baselines)
    df = add_risk_score(df)

    report(df, baselines)

    baselines.coalesce(1).write.mode("overwrite").parquet(str(config.DEGRADATION_BASELINES_PARQUET))

    replay = df.filter(
        (F.col("year") == F.lit(config.DEMO_SEASON))
        & (F.col("race_name") == F.lit(config.DEMO_RACE))
    )
    replay_count = replay.count()
    log.info(
        "Replay events filtered to %s %s: %d rows",
        config.DEMO_SEASON, config.DEMO_RACE, replay_count,
    )
    out_path = Path(str(config.RACE_REPLAY_EVENTS_PARQUET))
    if replay_count == 0:
        log.warning(
            "Replay event set empty. Live pipeline will have no data. "
            "Verify ingestion includes %s %s.",
            config.DEMO_SEASON, config.DEMO_RACE,
        )
        replay.coalesce(1).write.mode("overwrite").parquet(str(out_path))
    else:
        replay_pdf = replay.toPandas()
        replay_pdf = _score_anomaly(replay_pdf)
        if out_path.exists():
            if out_path.is_dir():
                import shutil
                shutil.rmtree(out_path)
            else:
                out_path.unlink()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        replay_pdf.to_parquet(out_path, index=False)

    log.info("Wrote -> %s", config.DEGRADATION_BASELINES_PARQUET)
    log.info("Wrote -> %s", config.RACE_REPLAY_EVENTS_PARQUET)
    spark.stop()


if __name__ == "__main__":
    main()
