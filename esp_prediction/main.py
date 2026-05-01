from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Allow direct execution: python esp_prediction/main.py
if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from esp_prediction.config import MODELS_DIR
from esp_prediction.data_ingestion import run_ingestion
from esp_prediction.feature_engineering import build_daily_features
from esp_prediction.model_trainer import train_models
from esp_prediction.predictor import run_prediction
from esp_prediction.virtual_sensor import apply_virtual_sensors


def _fallback_features_from_esp(esp_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal fallback feature builder to avoid full pipeline stop when upstream
    feature builder returns empty unexpectedly.
    """
    if esp_df is None or esp_df.empty:
        return pd.DataFrame()

    df = esp_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "timestamp" not in df.columns or "well_name" not in df.columns:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "well_name"]).copy()
    if df.empty:
        return pd.DataFrame()

    df["date"] = df["timestamp"].dt.floor("D")

    # Ensure needed numeric cols
    for c in [
        "frequency_hz",
        "pi_psia",
        "pd_psia",
        "ti_c",
        "tm_c",
        "current_ia",
        "current_ib",
        "current_ic",
        "motor_load_pct",
        "vibration_vx",
        "vibration_vy",
        "vibration_vz",
    ]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    agg = df.groupby(["well_name", "date"], as_index=False).agg(
        {
            "frequency_hz": "mean",
            "pi_psia": "mean",
            "pd_psia": "mean",
            "ti_c": "mean",
            "tm_c": "mean",
            "current_ia": "mean",
            "current_ib": "mean",
            "current_ic": "mean",
            "motor_load_pct": "mean",
            "vibration_vx": "mean",
            "vibration_vy": "mean",
            "vibration_vz": "mean",
        }
    )

    agg["delta_T"] = agg["tm_c"] - agg["ti_c"]
    agg["dp"] = agg["pd_psia"] - agg["pi_psia"]

    phase_mean = agg[["current_ia", "current_ib", "current_ic"]].mean(axis=1)
    phase_std = agg[["current_ia", "current_ib", "current_ic"]].std(axis=1)
    agg["phase_imbalance"] = (phase_std / phase_mean) * 100

    agg["vib_total"] = np.sqrt(
        (agg["vibration_vx"] ** 2)
        + (agg["vibration_vy"] ** 2)
        + (agg["vibration_vz"] ** 2)
    )
    agg["load_per_hz"] = agg["motor_load_pct"] / agg["frequency_hz"]
    agg["k_pump"] = agg["dp"] / (agg["frequency_hz"] ** 2)

    # Add expected model columns if absent
    for c in ["pi_slope_7d", "tm_slope_7d", "dp_slope_7d", "trips_30d", "k_degradation"]:
        if c not in agg.columns:
            agg[c] = 0.0

    # Labels from events
    agg["label"] = 0
    if events_df is not None and not events_df.empty:
        ev = events_df.copy()
        ev.columns = [str(c).strip() for c in ev.columns]
        if "well_name" in ev.columns and "stop_dt" in ev.columns and "failure_label" in ev.columns:
            ev["stop_dt"] = pd.to_datetime(ev["stop_dt"], errors="coerce")
            ev["date"] = ev["stop_dt"].dt.floor("D")
            ev["failure_label"] = pd.to_numeric(ev["failure_label"], errors="coerce").fillna(0).astype(int)
            lbl = ev[["well_name", "date", "failure_label"]].drop_duplicates()
            agg = agg.merge(lbl, on=["well_name", "date"], how="left")
            agg["label"] = agg["failure_label"].fillna(0).astype(int)
            agg = agg.drop(columns=["failure_label"], errors="ignore")

    return agg


def run_pipeline():
    # Step 2 ingestion
    esp_df, events_df = run_ingestion()

    if esp_df is None or esp_df.empty:
        raise ValueError("ESP ingestion returned empty data. Cannot continue pipeline.")

    # Step 3 virtual sensors
    esp_virtual = apply_virtual_sensors(esp_df)

    # Step 5 features
    features = build_daily_features(esp_virtual, events_df)

    # Fallback if main feature build returns empty
    if features is None or features.empty:
        print("[WARN] build_daily_features returned empty. Using fallback feature builder.")
        features = _fallback_features_from_esp(esp_virtual, events_df)

    if features is None or features.empty:
        raise ValueError("features_df is empty even after fallback. Check ingestion and columns.")

    print(f"[INFO] Feature rows ready for training: {len(features)}")

    # Train models
    iso_model, rf_model = train_models(features, MODELS_DIR)

    # Score + persist
    scored = run_prediction(iso_model, rf_model)

    print("Pipeline complete. Rows scored:", len(scored))


if __name__ == "__main__":
    run_pipeline()