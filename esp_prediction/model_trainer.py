from __future__ import annotations
import pickle
from pathlib import Path

import numpy as np
from sklearn.ensemble import IsolationForest, RandomForestClassifier

FEATURE_COLS = [
    "delta_T", "dp", "phase_imbalance", "vib_total", "load_per_hz",
    "pi_slope_7d", "tm_slope_7d", "dp_slope_7d", "trips_30d", "k_pump", "k_degradation",
]


def train_models(features_df, model_dir: Path):
    model_dir.mkdir(parents=True, exist_ok=True)
    df = features_df.copy()
    X = df[FEATURE_COLS].replace([np.inf, -np.inf], np.nan).fillna(method="ffill").fillna(method="bfill").fillna(0)

    healthy_mask = df["label"] == 0
    iso = IsolationForest(random_state=42, contamination=0.08)
    iso.fit(X.loc[healthy_mask] if healthy_mask.any() else X)

    y = df["label"].isin([1, 3]).astype(int)
    rf = RandomForestClassifier(n_estimators=300, random_state=42, class_weight="balanced")
    rf.fit(X, y)

    with open(model_dir / "isolation_forest.pkl", "wb") as f:
        pickle.dump(iso, f)
    with open(model_dir / "random_forest.pkl", "wb") as f:
        pickle.dump(rf, f)

    return iso, rf
