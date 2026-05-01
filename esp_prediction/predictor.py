from __future__ import annotations

import sqlite3
import numpy as np
import pandas as pd

from esp_prediction.config import DB_PATH, TABLE_NAMES
from esp_prediction.feature_engineering import build_daily_features
from esp_prediction.health_scorer import score_health
from esp_prediction.model_trainer import FEATURE_COLS
from esp_prediction.occ_detector import detect_occ
from esp_prediction.virtual_sensor import apply_virtual_sensors


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def _ensure_feature_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in FEATURE_COLS:
        if c not in out.columns:
            out[c] = np.nan
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _fallback_features_from_esp(esp_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    if esp_df is None or esp_df.empty:
        return pd.DataFrame()

    df = esp_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    well_col = _find_col(df, ["well_name", "well", "wellname"])
    ts_col = _find_col(df, ["timestamp", "datetime", "date_time", "time_stamp"])
    if not well_col or not ts_col:
        return pd.DataFrame()

    if well_col != "well_name":
        df = df.rename(columns={well_col: "well_name"})
    if ts_col != "timestamp":
        df = df.rename(columns={ts_col: "timestamp"})

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "well_name"]).copy()
    if df.empty:
        return pd.DataFrame()

    df["date"] = df["timestamp"].dt.floor("D")

    needed = [
        "frequency_hz", "pi_psia", "pd_psia", "ti_c", "tm_c",
        "current_ia", "current_ib", "current_ic",
        "motor_load_pct", "vibration_vx", "vibration_vy", "vibration_vz",
        "choke_size_in", "remarks", "header_pressure_bar",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = np.nan if c != "remarks" else ""
        if c != "remarks":
            df[c] = pd.to_numeric(df[c], errors="coerce")

    feat = df.groupby(["well_name", "date"], as_index=False).agg(
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
            "choke_size_in": "last",
            "remarks": "last",
            "header_pressure_bar": "mean",
        }
    )

    feat["delta_T"] = feat["tm_c"] - feat["ti_c"]
    feat["dp"] = feat["pd_psia"] - feat["pi_psia"]
    phase_mean = feat[["current_ia", "current_ib", "current_ic"]].mean(axis=1)
    phase_std = feat[["current_ia", "current_ib", "current_ic"]].std(axis=1)
    feat["phase_imbalance"] = (phase_std / phase_mean) * 100
    feat["vib_total"] = np.sqrt(
        feat["vibration_vx"] ** 2 + feat["vibration_vy"] ** 2 + feat["vibration_vz"] ** 2
    )
    feat["load_per_hz"] = feat["motor_load_pct"] / feat["frequency_hz"]
    feat["k_pump"] = feat["dp"] / (feat["frequency_hz"] ** 2)

    # placeholders for model features
    for c in ["pi_slope_7d", "tm_slope_7d", "dp_slope_7d", "trips_30d", "k_degradation"]:
        feat[c] = 0.0

    # label not required for prediction, but keep for consistency
    feat["label"] = 0

    # optional event-based trips
    if events_df is not None and not events_df.empty:
        ev = events_df.copy()
        ev.columns = [str(c).strip() for c in ev.columns]
        if "stop_dt" in ev.columns and "well_name" in ev.columns and "failure_label" in ev.columns:
            ev["stop_dt"] = pd.to_datetime(ev["stop_dt"], errors="coerce")
            ev["date"] = ev["stop_dt"].dt.floor("D")
            ev["failure_label"] = pd.to_numeric(ev["failure_label"], errors="coerce").fillna(0).astype(int)

            l1 = ev[ev["failure_label"] == 1][["well_name", "date"]].copy()
            l1["trip"] = 1
            feat = feat.merge(l1, on=["well_name", "date"], how="left")
            feat["trip"] = feat["trip"].fillna(0)
            feat["trips_30d"] = (
                feat.groupby("well_name")["trip"].rolling(30, min_periods=1).sum().reset_index(level=0, drop=True)
            )
            feat = feat.drop(columns=["trip"], errors="ignore")

    return feat


def load_input_tables():
    with sqlite3.connect(DB_PATH) as conn:
        esp = pd.read_sql(f"SELECT * FROM {TABLE_NAMES['esp_raw']}", conn)
        events = pd.read_sql(f"SELECT * FROM {TABLE_NAMES['esp_events']}", conn)

        try:
            prod = pd.read_sql("SELECT date, well_name, liquid_rate_bpd FROM oil_production", conn)
        except Exception:
            prod = pd.DataFrame(columns=["date", "well_name", "liquid_rate_bpd"])

    if "timestamp" in esp.columns:
        esp["timestamp"] = pd.to_datetime(esp["timestamp"], errors="coerce")
    return esp, events, prod


def persist_scores(scored_df: pd.DataFrame):
    out = scored_df.copy()
    out["created_at"] = pd.Timestamp.utcnow()
    out["dominant_alert"] = out.get("risk_level", "UNKNOWN")
    out = out.rename(columns={"k_degradation": "k_degradation_pct"})

    # Ensure expected output fields exist
    output_cols = [
        "date",
        "well_name",
        "health_score",
        "risk_level",
        "anomaly_score",
        "failure_probability",
        "occ_active",
        "occ_type",
        "occ_description",
        "delta_T",
        "dp",
        "phase_imbalance",
        "k_degradation_pct",
        "dominant_alert",
        "created_at",
    ]
    for c in output_cols:
        if c not in out.columns:
            out[c] = None

    with sqlite3.connect(DB_PATH) as conn:
        out[output_cols].to_sql(TABLE_NAMES["esp_health_scores"], conn, if_exists="replace", index=False)


def run_prediction(iso_model, rf_model):
    esp, events, prod = load_input_tables()

    # Step 1: virtual sensor stage
    try:
        esp_v = apply_virtual_sensors(esp)
        if esp_v is None or esp_v.empty:
            esp_v = esp.copy()
    except Exception:
        esp_v = esp.copy()

    # Step 2: feature engineering (primary)
    try:
        feat = build_daily_features(esp_v, events, prod)
    except Exception:
        feat = pd.DataFrame()

    # Step 3: fallback if primary feature build fails/empty
    if feat is None or feat.empty:
        feat = _fallback_features_from_esp(esp_v, events)

    if feat is None or feat.empty:
        feat = _fallback_features_from_esp(esp, events)

    if feat is None or feat.empty:
        raise ValueError("Prediction feature set is empty after fallback.")

    # Ensure model feature columns always exist
    feat = _ensure_feature_cols(feat)

    # OCC layer
    feat_occ = detect_occ(feat, events)
    feat_occ = _ensure_feature_cols(feat_occ)

    # Score
    scored = score_health(feat_occ, iso_model, rf_model, FEATURE_COLS)

    # Suppress health score during OCC active
    if "occ_active" in scored.columns:
        mask = scored["occ_active"] == True
        scored.loc[mask, "health_score"] = None
        scored.loc[mask, "risk_level"] = "RECALIBRATING"

    persist_scores(scored)
    return scored