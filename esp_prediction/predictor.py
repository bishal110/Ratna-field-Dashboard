from __future__ import annotations

import sqlite3
import pandas as pd

from esp_prediction.config import DB_PATH, TABLE_NAMES
from esp_prediction.feature_engineering import build_daily_features
from esp_prediction.health_scorer import score_health
from esp_prediction.model_trainer import FEATURE_COLS
from esp_prediction.occ_detector import detect_occ
from esp_prediction.virtual_sensor import apply_virtual_sensors


def load_input_tables():
    with sqlite3.connect(DB_PATH) as conn:
        esp = pd.read_sql(f"SELECT * FROM {TABLE_NAMES['esp_raw']}", conn)
        events = pd.read_sql(f"SELECT * FROM {TABLE_NAMES['esp_events']}", conn)
        try:
            prod = pd.read_sql("SELECT date, well_name, liquid_rate_bpd FROM oil_production", conn)
        except Exception:
            prod = pd.DataFrame(columns=["date", "well_name", "liquid_rate_bpd"])
    esp["timestamp"] = pd.to_datetime(esp["timestamp"], errors="coerce")
    return esp, events, prod


def persist_scores(scored_df: pd.DataFrame):
    cols = ["date", "well_name", "health_score", "risk_level", "anomaly_score", "failure_probability", "occ_active", "occ_type", "occ_description", "delta_T", "dp", "phase_imbalance", "k_degradation"]
    out = scored_df.copy()
    out["created_at"] = pd.Timestamp.utcnow()
    out["dominant_alert"] = out["risk_level"]
    out = out.rename(columns={"k_degradation": "k_degradation_pct"})
    cols = [c for c in cols if c in out.columns]
    cols += ["k_degradation_pct", "dominant_alert", "created_at"]

    with sqlite3.connect(DB_PATH) as conn:
        out[cols].to_sql(TABLE_NAMES["esp_health_scores"], conn, if_exists="replace", index=False)


def run_prediction(iso_model, rf_model):
    esp, events, prod = load_input_tables()
    esp_v = apply_virtual_sensors(esp)
    feat = build_daily_features(esp_v, events, prod)
    feat_occ = detect_occ(feat, events)

    scored = score_health(feat_occ, iso_model, rf_model, FEATURE_COLS)
    scored.loc[scored["occ_active"] == True, "health_score"] = None
    scored.loc[scored["occ_active"] == True, "risk_level"] = "RECALIBRATING"
    persist_scores(scored)
    return scored
