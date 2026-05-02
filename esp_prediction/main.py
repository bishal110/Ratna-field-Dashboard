from __future__ import annotations

from esp_prediction.config import MODELS_DIR
from esp_prediction.data_ingestion import run_ingestion
from esp_prediction.model_trainer import train_models
from esp_prediction.predictor import run_prediction
from esp_prediction.feature_engineering import build_daily_features
from esp_prediction.virtual_sensor import apply_virtual_sensors


def run_pipeline():
    esp_df, events_df = run_ingestion()
    esp_virtual = apply_virtual_sensors(esp_df)
    features = build_daily_features(esp_virtual, events_df)
    iso, rf = train_models(features, MODELS_DIR)
    scored = run_prediction(iso, rf)
    print("Pipeline complete. Rows scored:", len(scored))


if __name__ == "__main__":
    run_pipeline()
