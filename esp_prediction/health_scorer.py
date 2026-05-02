from __future__ import annotations

import numpy as np
import pandas as pd


def score_health(feature_df: pd.DataFrame, iso_model, rf_model, feature_cols: list[str]) -> pd.DataFrame:
    df = feature_df.copy()
    X = df[feature_cols].replace([np.inf, -np.inf], np.nan).ffill().bfill().fillna(0)

    anomaly_raw = iso_model.decision_function(X)
    proba = rf_model.predict_proba(X)
    failure_prob = proba[:, 1] if proba.shape[1] > 1 else np.zeros(len(X))

    isolation_score = (anomaly_raw + 1) / 2 * 50
    rf_score = (1 - failure_prob) * 50
    health_score = isolation_score + rf_score

    df["anomaly_score"] = anomaly_raw
    df["failure_probability"] = failure_prob
    df["health_score"] = health_score.clip(0, 100)
    df["risk_level"] = np.select(
        [df["health_score"] >= 70, df["health_score"] >= 40],
        ["NORMAL", "WARNING"],
        default="CRITICAL",
    )
    return df
