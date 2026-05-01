from __future__ import annotations

import numpy as np
import pandas as pd

from esp_prediction.config import BASELINE_HEALTHY_DAYS


def _rolling_slope(series: pd.Series, window: int = 7) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce").values
    out = np.full(len(vals), np.nan)
    x = np.arange(window)

    for i in range(window - 1, len(vals)):
        y = vals[i - window + 1 : i + 1]
        if np.isnan(y).any():
            continue
        out[i] = np.polyfit(x, y, 1)[0]

    return pd.Series(out, index=series.index)


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names and ensure required columns exist.
    Handles case/space variations from sqlite/pandas pipelines.
    """
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    # Build lowercase map for robust lookup
    lc_map = {c.lower(): c for c in out.columns}

    def pick(*names: str):
        for n in names:
            if n in out.columns:
                return n
            if n.lower() in lc_map:
                return lc_map[n.lower()]
        return None

    rename_map = {}

    c = pick("well_name", "well", "wellname")
    if c and c != "well_name":
        rename_map[c] = "well_name"

    c = pick("timestamp", "time_stamp", "datetime", "date_time")
    if c and c != "timestamp":
        rename_map[c] = "timestamp"

    c = pick("frequency_hz", "frequency", "hz")
    if c and c != "frequency_hz":
        rename_map[c] = "frequency_hz"

    c = pick("pi_psia", "pi")
    if c and c != "pi_psia":
        rename_map[c] = "pi_psia"

    c = pick("pd_psia", "pd")
    if c and c != "pd_psia":
        rename_map[c] = "pd_psia"

    c = pick("ti_c", "ti")
    if c and c != "ti_c":
        rename_map[c] = "ti_c"

    c = pick("tm_c", "tm")
    if c and c != "tm_c":
        rename_map[c] = "tm_c"

    c = pick("current_ia", "ia")
    if c and c != "current_ia":
        rename_map[c] = "current_ia"

    c = pick("current_ib", "ib")
    if c and c != "current_ib":
        rename_map[c] = "current_ib"

    c = pick("current_ic", "ic")
    if c and c != "current_ic":
        rename_map[c] = "current_ic"

    c = pick("motor_load_pct", "motor_load")
    if c and c != "motor_load_pct":
        rename_map[c] = "motor_load_pct"

    c = pick("vibration_vx", "vx")
    if c and c != "vibration_vx":
        rename_map[c] = "vibration_vx"

    c = pick("vibration_vy", "vy")
    if c and c != "vibration_vy":
        rename_map[c] = "vibration_vy"

    c = pick("vibration_vz", "vz")
    if c and c != "vibration_vz":
        rename_map[c] = "vibration_vz"

    c = pick("choke_size_in", "choke_size")
    if c and c != "choke_size_in":
        rename_map[c] = "choke_size_in"

    c = pick("remarks", "comment", "comments")
    if c and c != "remarks":
        rename_map[c] = "remarks"

    c = pick("header_pressure_bar", "production_header_pressure_bar", "header_pressure")
    if c and c != "header_pressure_bar":
        rename_map[c] = "header_pressure_bar"

    out = out.rename(columns=rename_map)

    # Ensure required columns exist (fill with NaN/blank if missing)
    must_cols = [
        "well_name",
        "timestamp",
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
        "choke_size_in",
        "remarks",
        "header_pressure_bar",
    ]
    for col in must_cols:
        if col not in out.columns:
            out[col] = np.nan if col != "remarks" else ""

    return out


def build_daily_features(
    esp_df: pd.DataFrame,
    events_df: pd.DataFrame | None,
    prod_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if esp_df is None or esp_df.empty:
        return pd.DataFrame()

    df = _standardize_columns(esp_df)

    # Final guard
    if "well_name" not in df.columns:
        raise KeyError("well_name not found in esp_df even after standardization")
    if "timestamp" not in df.columns:
        raise KeyError("timestamp not found in esp_df even after standardization")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "well_name"]).copy()
    df["date"] = df["timestamp"].dt.floor("D")

    # Numeric normalization
    num_cols = [
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
        "choke_size_in",
        "header_pressure_bar",
    ]
    for c in num_cols:
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
            "choke_size_in": "last",
            "remarks": "last",
            "header_pressure_bar": "mean",
        }
    )

    # Optional production join
    if prod_df is not None and not prod_df.empty:
        p = prod_df.copy()
        p.columns = [str(c).strip() for c in p.columns]
        if "date" in p.columns and "well_name" in p.columns and "liquid_rate_bpd" in p.columns:
            p["date"] = pd.to_datetime(p["date"], errors="coerce")
            p["liquid_rate_bpd"] = pd.to_numeric(p["liquid_rate_bpd"], errors="coerce")
            p = (
                p.groupby(["well_name", "date"], as_index=False)["liquid_rate_bpd"]
                .mean()
                .rename(columns={"liquid_rate_bpd": "flow_rate_bpd"})
            )
            agg = agg.merge(p, on=["well_name", "date"], how="left")
        else:
            agg["flow_rate_bpd"] = np.nan
    else:
        agg["flow_rate_bpd"] = np.nan

    # Derived features
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

    out = []
    for _, g in agg.groupby("well_name"):
        g = g.sort_values("date").copy()
        g["pi_slope_7d"] = _rolling_slope(g["pi_psia"], 7)
        g["tm_slope_7d"] = _rolling_slope(g["tm_c"], 7)
        g["dp_slope_7d"] = _rolling_slope(g["dp"], 7)

        kbase_window = g.dropna(subset=["k_pump"]).head(BASELINE_HEALTHY_DAYS)
        k_base = kbase_window["k_pump"].mean() if not kbase_window.empty else np.nan
        g["k_baseline"] = k_base
        g["k_degradation"] = (
            (k_base - g["k_pump"]) / k_base * 100 if pd.notna(k_base) else np.nan
        )
        out.append(g)

    feat = pd.concat(out, ignore_index=True) if out else pd.DataFrame()

    # Event features + labels
    feat["trips_30d"] = 0.0
    feat["label"] = 0

    if events_df is not None and not events_df.empty and not feat.empty:
        ev = events_df.copy()
        ev.columns = [str(c).strip() for c in ev.columns]

        if "stop_dt" in ev.columns and "well_name" in ev.columns:
            ev["stop_dt"] = pd.to_datetime(ev["stop_dt"], errors="coerce")
            ev["date"] = ev["stop_dt"].dt.floor("D")
            if "failure_label" in ev.columns:
                ev["failure_label"] = pd.to_numeric(ev["failure_label"], errors="coerce").fillna(0).astype(int)
            else:
                ev["failure_label"] = 0

            # trips_30d from label 1
            l1 = ev[ev["failure_label"] == 1][["well_name", "date"]].copy()
            l1["trip"] = 1
            feat = feat.merge(l1, on=["well_name", "date"], how="left")
            feat["trip"] = feat["trip"].fillna(0)
            feat["trips_30d"] = (
                feat.groupby("well_name")["trip"]
                .rolling(30, min_periods=1)
                .sum()
                .reset_index(level=0, drop=True)
            )
            feat = feat.drop(columns=["trip"], errors="ignore")

            # daily label from event table
            m = ev[["well_name", "date", "failure_label"]].drop_duplicates()
            feat = feat.merge(m, on=["well_name", "date"], how="left")
            feat["label"] = feat["failure_label"].fillna(0).astype(int)
            feat = feat.drop(columns=["failure_label"], errors="ignore")

    return feat