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
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    lc_map = {c.lower(): c for c in out.columns}

    def pick(*names: str):
        for n in names:
            if n in out.columns:
                return n
            if n.lower() in lc_map:
                return lc_map[n.lower()]
        return None

    rename_map = {}
    mapped = {
        "well_name": pick("well_name", "well", "wellname"),
        "timestamp": pick("timestamp", "time_stamp", "datetime", "date_time"),
        "frequency_hz": pick("frequency_hz", "frequency", "hz"),
        "pi_psia": pick("pi_psia", "pi"),
        "pd_psia": pick("pd_psia", "pd"),
        "ti_c": pick("ti_c", "ti"),
        "tm_c": pick("tm_c", "tm"),
        "current_ia": pick("current_ia", "ia"),
        "current_ib": pick("current_ib", "ib"),
        "current_ic": pick("current_ic", "ic"),
        "motor_load_pct": pick("motor_load_pct", "motor_load"),
        "vibration_vx": pick("vibration_vx", "vx"),
        "vibration_vy": pick("vibration_vy", "vy"),
        "vibration_vz": pick("vibration_vz", "vz"),
        "choke_size_in": pick("choke_size_in", "choke_size"),
        "remarks": pick("remarks", "comment", "comments"),
        "header_pressure_bar": pick("header_pressure_bar", "production_header_pressure_bar", "header_pressure"),
    }
    for target, source in mapped.items():
        if source and source != target:
            rename_map[source] = target
    out = out.rename(columns=rename_map)

    must_cols = [
        "well_name", "timestamp", "frequency_hz", "pi_psia", "pd_psia", "ti_c", "tm_c",
        "current_ia", "current_ib", "current_ic", "motor_load_pct", "vibration_vx", "vibration_vy",
        "vibration_vz", "choke_size_in", "remarks", "header_pressure_bar",
    ]
    for col in must_cols:
        if col not in out.columns:
            out[col] = np.nan if col != "remarks" else ""
    return out


def build_daily_features(esp_df: pd.DataFrame, events_df: pd.DataFrame, prod_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if esp_df is None or esp_df.empty:
        return pd.DataFrame()

    df = _standardize_columns(esp_df)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "well_name"]).copy()
    if df.empty:
        return pd.DataFrame()

    df["date"] = df["timestamp"].dt.floor("D")

    numeric_cols = [
        "frequency_hz", "pi_psia", "pd_psia", "ti_c", "tm_c", "current_ia", "current_ib", "current_ic",
        "motor_load_pct", "vibration_vx", "vibration_vy", "vibration_vz", "choke_size_in", "header_pressure_bar",
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    agg = df.groupby(["well_name", "date"], as_index=False).agg({
        "frequency_hz": "mean", "pi_psia": "mean", "pd_psia": "mean", "ti_c": "mean", "tm_c": "mean",
        "current_ia": "mean", "current_ib": "mean", "current_ic": "mean", "motor_load_pct": "mean",
        "vibration_vx": "mean", "vibration_vy": "mean", "vibration_vz": "mean", "choke_size_in": "last",
        "remarks": "last", "header_pressure_bar": "mean",
    })

    if prod_df is not None and not prod_df.empty:
        p = prod_df.copy()
        p.columns = [str(c).strip() for c in p.columns]
        if {"date", "well_name", "liquid_rate_bpd"}.issubset(set(p.columns)):
            p["date"] = pd.to_datetime(p["date"], errors="coerce")
            p["liquid_rate_bpd"] = pd.to_numeric(p["liquid_rate_bpd"], errors="coerce")
            p = p.groupby(["well_name", "date"], as_index=False)["liquid_rate_bpd"].mean().rename(columns={"liquid_rate_bpd": "flow_rate_bpd"})
            agg = agg.merge(p, on=["well_name", "date"], how="left")
        else:
            agg["flow_rate_bpd"] = np.nan
    else:
        agg["flow_rate_bpd"] = np.nan

    agg["delta_T"] = agg["tm_c"] - agg["ti_c"]
    agg["dp"] = agg["pd_psia"] - agg["pi_psia"]
    phase_mean = agg[["current_ia", "current_ib", "current_ic"]].mean(axis=1)
    phase_std = agg[["current_ia", "current_ib", "current_ic"]].std(axis=1)
    agg["phase_imbalance"] = (phase_std / phase_mean) * 100
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

    feat = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    if feat.empty:
        return feat

    feat["trips_30d"] = 0.0
    feat["label"] = 0

    if events_df is not None and not events_df.empty:
        ev = events_df.copy()
        ev.columns = [str(c).strip() for c in ev.columns]
        if {"well_name", "stop_dt"}.issubset(set(ev.columns)):
            ev["stop_dt"] = pd.to_datetime(ev["stop_dt"], errors="coerce")
            ev["date"] = ev["stop_dt"].dt.floor("D")
            if "failure_label" not in ev.columns:
                ev["failure_label"] = 0
            ev["failure_label"] = pd.to_numeric(ev["failure_label"], errors="coerce").fillna(0).astype(int)

            l1 = ev[ev["failure_label"] == 1][["well_name", "date"]].copy()
            l1["trip"] = 1
            feat = feat.merge(l1, on=["well_name", "date"], how="left")
            feat["trip"] = feat["trip"].fillna(0)
            feat["trips_30d"] = feat.groupby("well_name")["trip"].rolling(30, min_periods=1).sum().reset_index(level=0, drop=True)
            feat = feat.drop(columns=["trip"], errors="ignore")

            m = ev[["well_name", "date", "failure_label"]].drop_duplicates()
            feat = feat.merge(m, on=["well_name", "date"], how="left")
            feat["label"] = feat["failure_label"].fillna(0).astype(int)
            feat = feat.drop(columns=["failure_label"], errors="ignore")

    return feat