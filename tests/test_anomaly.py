"""Sanity tests for the persisted IsolationForest anomaly model."""
from __future__ import annotations

import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from features.train_anomaly_model import FEATURE_COLS


@pytest.fixture(scope="module")
def model_payload():
    if not config.ANOMALY_MODEL_PATH.exists():
        pytest.skip(f"Model not trained yet at {config.ANOMALY_MODEL_PATH}")
    return joblib.load(config.ANOMALY_MODEL_PATH)


def test_payload_shape(model_payload):
    assert "model" in model_payload
    assert model_payload["feature_cols"] == FEATURE_COLS
    assert 0.0 < model_payload["contamination"] < 0.5
    assert model_payload["trained_rows"] >= 100


def test_score_range_on_training_data(model_payload):
    parts = list(Path(config.LAP_FEATURES_PARQUET).glob("*.parquet"))
    if not parts:
        pytest.skip("No lap_features parquet present")
    df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    clean = df[df["clean_lap"].fillna(False)].dropna(subset=FEATURE_COLS)
    if clean.empty:
        pytest.skip("No clean laps to score")

    X = clean[FEATURE_COLS].to_numpy(dtype=np.float64)
    model = model_payload["model"]
    scores = model.score_samples(X)
    preds = model.predict(X)

    assert np.isfinite(scores).all()
    assert set(np.unique(preds)).issubset({-1, 1})

    rate = (preds == -1).mean()
    contamination = model_payload["contamination"]
    assert rate <= contamination * 1.5 + 0.02, (
        f"Anomaly rate {rate:.3f} far above contamination {contamination}"
    )


def test_handles_nan_rows_gracefully(model_payload):
    model = model_payload["model"]
    cols = model_payload["feature_cols"]
    bad = np.full((1, len(cols)), np.nan)
    with pytest.raises((ValueError, RuntimeError)):
        model.predict(bad)
