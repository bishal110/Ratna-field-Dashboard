from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest, RandomForestClassifier

FEATURE_COLS = [
    "delta_T",
    "dp",
    "phase_imbalance",
    "vib_total",
    "load_per_hz",
    "pi_slope_7d",
    "tm_slope_7d",
    "dp_slope_7d",
    "trips_30d",
    "k_pump",
    "k_degradation",
]


def _ensure_feature_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Ensure every required feature exists
    for col in FEATURE_COLS:
        if col not in out.columns:
            out[col] = np.nan

    # Ensure label exists
    if "label" not in out.columns:
        out["label"] = 0

    # Numeric cast safety
    for col in FEATURE_COLS:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["label"] = pd.to_numeric(out["label"], errors="coerce").fillna(0).astype(int)
    return out


def train_models(features_df: pd.DataFrame, model_dir: Path):
    if features_df is None or features_df.empty:
        raise ValueError("features_df is empty. Cannot train models.")

    model_dir.mkdir(parents=True, exist_ok=True)

    df = _ensure_feature_columns(features_df)

    X = (
        df[FEATURE_COLS]
        .replace([np.inf, -np.inf], np.nan)
        .ffill()
        .bfill()
        .fillna(0.0)
    )

    # Healthy = label 0
    healthy_mask = df["label"] == 0

    # Isolation Forest
    iso = IsolationForest(random_state=42, contamination=0.08)
    if healthy_mask.any():
        iso.fit(X.loc[healthy_mask])
    else:
        iso.fit(X)

    # RF target: 1 if failure-like (label 1 or 3), else 0
    y = df["label"].isin([1, 3]).astype(int)

    # If only one class present, create a tiny synthetic opposite class
    # to prevent classifier crash in edge cases.
    if y.nunique() < 2:
        x_row = X.iloc[[0]].copy()
        y_row = pd.Series([1 - int(y.iloc[0])], index=x_row.index)
        X = pd.concat([X, x_row], ignore_index=True)
        y = pd.concat([y, y_row], ignore_index=True)

    rf = RandomForestClassifier(
        n_estimators=300,
        random_state=42,
        class_weight="balanced",
    )
    rf.fit(X, y)

    with open(model_dir / "isolation_forest.pkl", "wb") as f:
        pickle.dump(iso, f)

    with open(model_dir / "random_forest.pkl", "wb") as f:
        pickle.dump(rf, f)

    return iso, rf