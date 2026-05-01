"""Phase 12: train IsolationForest anomaly detector on clean laps.

Reads lap_features.parquet (post Phase 3), filters clean laps, fits
IsolationForest on a small set of degradation-relevant features, persists
the model + the feature schema to models/isoforest_v1.joblib.

The model is later loaded by build_degradation_baselines.py to score every
event (including non-clean laps) so the streaming layer carries a per-lap
anomaly_score + is_anomaly flag.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from utils.logging import get_logger

log = get_logger("anomaly_train")

FEATURE_COLS = [
    "pace_delta",
    "field_delta",
    "tyre_life",
    "track_temp_c",
    "air_temp_c",
    "humidity",
    "wind_speed",
]

DEFAULT_CONTAMINATION = 0.05
DEFAULT_N_ESTIMATORS = 200
DEFAULT_RANDOM_STATE = 42


def _read_lap_features() -> pd.DataFrame:
    parts = list(Path(config.LAP_FEATURES_PARQUET).glob("*.parquet"))
    if not parts:
        raise FileNotFoundError(f"No parquet parts under {config.LAP_FEATURES_PARQUET}")
    frames = [pd.read_parquet(p) for p in parts]
    return pd.concat(frames, ignore_index=True)


def prepare_training_frame(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"lap_features parquet missing required columns: {missing}. "
            f"Re-run ingestion (with weather) and build_lap_features."
        )
    clean = df[df["clean_lap"].fillna(False)].copy()
    clean = clean.dropna(subset=FEATURE_COLS)
    return clean


def train(
    clean: pd.DataFrame,
    contamination: float = DEFAULT_CONTAMINATION,
    n_estimators: int = DEFAULT_N_ESTIMATORS,
    random_state: int = DEFAULT_RANDOM_STATE,
) -> IsolationForest:
    X = clean[FEATURE_COLS].to_numpy(dtype=np.float64)
    log.info("Training IsolationForest: rows=%d cols=%d contamination=%.3f",
             X.shape[0], X.shape[1], contamination)
    model = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        random_state=random_state,
        n_jobs=-1,
    )
    model.fit(X)
    return model


def report(model: IsolationForest, clean: pd.DataFrame) -> None:
    X = clean[FEATURE_COLS].to_numpy(dtype=np.float64)
    scores = model.score_samples(X)
    preds = model.predict(X)
    anomalies = (preds == -1).sum()
    rate = anomalies / max(len(preds), 1)
    log.info("Train rows: %d", len(preds))
    log.info("Anomaly rate: %.2f%% (%d / %d)", rate * 100, anomalies, len(preds))
    log.info("Score percentiles: p1=%.3f p5=%.3f p50=%.3f p95=%.3f p99=%.3f",
             np.percentile(scores, 1), np.percentile(scores, 5),
             np.percentile(scores, 50), np.percentile(scores, 95),
             np.percentile(scores, 99))

    by_compound = (
        clean.assign(is_anomaly=(preds == -1))
        .groupby("compound")["is_anomaly"].agg(["sum", "count"])
    )
    by_compound["rate_pct"] = 100 * by_compound["sum"] / by_compound["count"].clip(lower=1)
    log.info("Anomaly rate by compound:\n%s", by_compound.to_string())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train IsolationForest on clean laps.")
    p.add_argument("--contamination", type=float, default=DEFAULT_CONTAMINATION)
    p.add_argument("--n-estimators", type=int, default=DEFAULT_N_ESTIMATORS)
    p.add_argument("--output", type=Path, default=config.ANOMALY_MODEL_PATH)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    log.info("Reading lap features from %s", config.LAP_FEATURES_PARQUET)
    df = _read_lap_features()
    clean = prepare_training_frame(df)
    log.info("Clean laps for training: %d (of %d total)", len(clean), len(df))
    if len(clean) < 50:
        raise SystemExit("Too few clean laps to train (<50). Aborting.")

    model = train(
        clean,
        contamination=args.contamination,
        n_estimators=args.n_estimators,
    )
    report(model, clean)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "model": model,
        "feature_cols": FEATURE_COLS,
        "contamination": args.contamination,
        "n_estimators": args.n_estimators,
        "trained_rows": int(len(clean)),
    }
    joblib.dump(payload, args.output)
    log.info("Wrote model -> %s", args.output)


if __name__ == "__main__":
    main()
