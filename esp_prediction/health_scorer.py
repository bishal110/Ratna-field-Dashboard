from __future__ import annotations

import numpy as np
import pandas as pd


def score_health(feature_df: pd.DataFrame, iso_model, rf_model, feature_cols: list[str]) -> pd.DataFrame:
    if feature_df is None or feature_df.empty:
        raise ValueError("feature_df is empty in score_health")

    df = feature_df.copy()

    # Ensure all expected feature columns exist
    for c in feature_cols:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    X = (
        df[feature_cols]
        .replace([np.inf, -np.inf], np.nan)
        .ffill()
        .bfill()
        .fillna(0.0)
    )

    anomaly_raw = iso_model.decision_function(X)
    failure_prob = rf_model.predict_proba(X)[:, 1]

    # Combined 0-100 score
    isolation_score = (anomaly_raw + 1.0) / 2.0 * 50.0
    rf_score = (1.0 - failure_prob) * 50.0
    health_score = isolation_score + rf_score

    df["anomaly_score"] = anomaly_raw
    df["failure_probability"] = failure_prob
    df["health_score"] = np.clip(health_score, 0, 100)

    df["risk_level"] = np.select(
        [df["health_score"] >= 70, df["health_score"] >= 40],
        ["NORMAL", "WARNING"],
        default="CRITICAL",
    )

    return df