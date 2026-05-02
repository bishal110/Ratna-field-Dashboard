from __future__ import annotations
import numpy as np
import pandas as pd

from esp_prediction.config import BASELINE_HEALTHY_DAYS


def _rolling_slope(series: pd.Series, window: int = 7) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce").values.astype(float)
    out = np.full(len(vals), np.nan)
    x = np.arange(window)
    for i in range(window - 1, len(vals)):
        y = vals[i - window + 1 : i + 1]
        if np.isnan(y).any():
            continue
        out[i] = np.polyfit(x, y, 1)[0]
    return pd.Series(out, index=series.index)


def build_daily_features(esp_df: pd.DataFrame, events_df: pd.DataFrame, prod_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if esp_df.empty:
        return esp_df
    df = esp_df.copy()
    df["date"] = pd.to_datetime(df["timestamp"]).dt.floor("D")

    agg = df.groupby(["well_name", "date"], as_index=False).agg({
        "frequency_hz": "mean", "pi_psia": "mean", "pd_psia": "mean", "ti_c": "mean", "tm_c": "mean",
        "current_ia": "mean", "current_ib": "mean", "current_ic": "mean", "motor_load_pct": "mean",
        "vibration_vx": "mean", "vibration_vy": "mean", "vibration_vz": "mean", "choke_size_in": "last",
        "remarks": "last", "header_pressure_bar": "mean",
    })

    if prod_df is not None and not prod_df.empty:
        p = prod_df.copy()
        p["date"] = pd.to_datetime(p["date"])
        p = p.groupby(["well_name", "date"], as_index=False)["liquid_rate_bpd"].mean().rename(columns={"liquid_rate_bpd": "flow_rate_bpd"})
        agg = agg.merge(p, on=["well_name", "date"], how="left")
    else:
        agg["flow_rate_bpd"] = np.nan

    for col in ["vibration_vx", "vibration_vy", "vibration_vz", "current_ia", "current_ib", "current_ic"]:
        agg[col] = pd.to_numeric(agg[col], errors="coerce")

    agg["delta_T"] = agg["tm_c"] - agg["ti_c"]
    agg["dp"] = agg["pd_psia"] - agg["pi_psia"]
    agg["phase_imbalance"] = agg[["current_ia", "current_ib", "current_ic"]].std(axis=1) / agg[["current_ia", "current_ib", "current_ic"]].mean(axis=1) * 100
    agg["vib_total"] = np.sqrt((agg["vibration_vx"] ** 2) + (agg["vibration_vy"] ** 2) + (agg["vibration_vz"] ** 2))
    agg["load_per_hz"] = agg["motor_load_pct"] / agg["frequency_hz"]
    agg["k_pump"] = agg["dp"] / (agg["frequency_hz"] ** 2)

    out = []
    for _, g in agg.groupby("well_name"):
        g = g.sort_values("date").copy()
        g["pi_slope_7d"] = _rolling_slope(g["pi_psia"], 7)
        g["tm_slope_7d"] = _rolling_slope(g["tm_c"], 7)
        g["dp_slope_7d"] = _rolling_slope(g["dp"], 7)

        kbase_window = g.dropna(subset=["k_pump"]).head(BASELINE_HEALTHY_DAYS)
        k_base = kbase_window["k_pump"].mean() if not kbase_window.empty else np.nan
        g["k_baseline"] = k_base
        g["k_degradation"] = (k_base - g["k_pump"]) / k_base * 100 if pd.notna(k_base) else np.nan
        out.append(g)
    feat = pd.concat(out, ignore_index=True)

    if events_df is not None and not events_df.empty:
        ev = events_df.copy()
        ev["stop_dt"] = pd.to_datetime(ev["stop_dt"])
        ev["date"] = ev["stop_dt"].dt.floor("D")
        l1 = ev[ev["failure_label"] == 1][["well_name", "date"]].copy()
        l1["trip"] = 1
        feat = feat.merge(l1, on=["well_name", "date"], how="left")
        feat["trip"] = feat["trip"].fillna(0)
        feat["trips_30d"] = feat.groupby("well_name")["trip"].rolling(30, min_periods=1).sum().reset_index(level=0, drop=True)
    else:
        feat["trips_30d"] = 0

    feat["label"] = 0
    if events_df is not None and not events_df.empty:
        ev2 = events_df.copy(); ev2["date"] = pd.to_datetime(ev2["stop_dt"]).dt.floor("D")
        m = ev2[["well_name", "date", "failure_label"]].drop_duplicates()
        feat = feat.merge(m, on=["well_name", "date"], how="left")
        feat["label"] = feat["failure_label"].fillna(0).astype(int)
        feat.drop(columns=["failure_label"], inplace=True)

    return feat
