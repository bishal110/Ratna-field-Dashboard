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


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def _fallback_features_from_esp(esp_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal fallback feature builder to avoid full pipeline stop when upstream
    feature builder returns empty unexpectedly.
    """
    if esp_df is None or esp_df.empty:
        return pd.DataFrame()

    df = esp_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    well_col = _find_col(df, ["well_name", "well", "wellname"])
    ts_col = _find_col(df, ["timestamp", "datetime", "date_time", "time_stamp"])

    if not well_col or not ts_col:
        print(f"[DEBUG] fallback missing core cols. Columns = {list(df.columns)}")
        return pd.DataFrame()

    if well_col != "well_name":
        df = df.rename(columns={well_col: "well_name"})
    if ts_col != "timestamp":
        df = df.rename(columns={ts_col: "timestamp"})

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "well_name"]).copy()
    if df.empty:
        print("[DEBUG] fallback: all rows dropped after timestamp/well filtering")
        return pd.DataFrame()

    df["date"] = df["timestamp"].dt.floor("D")

    # Ensure needed numeric cols
    numeric_needed = [
        "frequency_hz", "pi_psia", "pd_psia", "ti_c", "tm_c",
        "current_ia", "current_ib", "current_ic",
        "motor_load_pct", "vibration_vx", "vibration_vy", "vibration_vz"
    ]
    for c in numeric_needed:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    agg = df.groupby(["well_name", "date"], as_index=False).agg({
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
    })

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

    # Add expected model columns
    for c in ["pi_slope_7d", "tm_slope_7d", "dp_slope_7d", "trips_30d", "k_degradation"]:
        if c not in agg.columns:
            agg[c] = 0.0

    # Labels from events
    agg["label"] = 0
    if events_df is not None and not events_df.empty:
        ev = events_df.copy()
        ev.columns = [str(c).strip() for c in ev.columns]

        wcol = _find_col(ev, ["well_name", "well", "wellname"])
        scol = _find_col(ev, ["stop_dt", "stop_date", "stop_datetime"])
        lcol = _find_col(ev, ["failure_label", "label"])

        if wcol and scol and lcol:
            if wcol != "well_name":
                ev = ev.rename(columns={wcol: "well_name"})
            if scol != "stop_dt":
                ev = ev.rename(columns={scol: "stop_dt"})
            if lcol != "failure_label":
                ev = ev.rename(columns={lcol: "failure_label"})

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

    print(f"[INFO] Ingested ESP rows: {len(esp_df)}")
    print(f"[INFO] Ingested Events rows: {0 if events_df is None else len(events_df)}")
    print(f"[DEBUG] ESP columns: {list(esp_df.columns)}")

    # Step 3 virtual sensors
    try:
        esp_virtual = apply_virtual_sensors(esp_df)
        if esp_virtual is None or esp_virtual.empty:
            print("[WARN] apply_virtual_sensors returned empty. Using raw esp_df for features.")
            esp_virtual = esp_df.copy()
    except Exception as e:
        print(f"[WARN] apply_virtual_sensors failed ({e}). Using raw esp_df for features.")
        esp_virtual = esp_df.copy()

    print(f"[INFO] ESP rows after virtual sensor stage: {len(esp_virtual)}")
    print(f"[DEBUG] ESP virtual columns: {list(esp_virtual.columns)}")

    # Main feature builder
    try:
        features = build_daily_features(esp_virtual, events_df)
    except Exception as e:
        print(f"[WARN] build_daily_features exception: {e}")
        features = pd.DataFrame()

    # Fallback 1: from esp_virtual
    if features is None or features.empty:
        print("[WARN] build_daily_features returned empty. Using fallback builder on esp_virtual.")
        features = _fallback_features_from_esp(esp_virtual, events_df)

    # Fallback 2: from raw esp_df
    if features is None or features.empty:
        print("[WARN] fallback on esp_virtual empty. Retrying fallback on raw esp_df.")
        features = _fallback_features_from_esp(esp_df, events_df)

    if features is None or features.empty:
        raise ValueError(
            "features_df is empty after all fallbacks. "
            "Please share [DEBUG] ESP columns + first 5 rows."
        )

    print(f"[INFO] Feature rows ready for training: {len(features)}")
    print(f"[DEBUG] Feature columns: {list(features.columns)}")

    # Train models
    iso_model, rf_model = train_models(features, MODELS_DIR)

    # Score + persist
    scored = run_prediction(iso_model, rf_model)

    print("Pipeline complete. Rows scored:", len(scored))


if __name__ == "__main__":
    run_pipeline()