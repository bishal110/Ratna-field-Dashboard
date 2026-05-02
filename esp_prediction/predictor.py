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


def _ensure_feature_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in FEATURE_COLS:
        if c not in out.columns:
            out[c] = np.nan
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


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

    esp_v = apply_virtual_sensors(esp)
    if esp_v is None or esp_v.empty:
        esp_v = esp.copy()

    feat = build_daily_features(esp_v, events, prod)
    if feat is None or feat.empty:
        raise ValueError("Prediction feature set is empty. Run ingestion and Step 2/3 checks.")

    feat = _ensure_feature_cols(feat)

    feat_occ = detect_occ(feat, events)
    feat_occ = _ensure_feature_cols(feat_occ)

    scored = score_health(feat_occ, iso_model, rf_model, FEATURE_COLS)

    if "occ_active" in scored.columns:
        mask = scored["occ_active"] == True
        scored.loc[mask, "health_score"] = None
        scored.loc[mask, "risk_level"] = "RECALIBRATING"

    persist_scores(scored)
    return scored
