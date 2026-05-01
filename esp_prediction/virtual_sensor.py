from __future__ import annotations

import numpy as np
import pandas as pd

from esp_prediction.config import BASELINE_HEALTHY_DAYS, CONFIDENCE_WEIGHTS


def _fit_k_pump(df: pd.DataFrame) -> float:
    healthy = df.dropna(subset=["pi_psia", "pd_psia", "frequency_hz"]).copy()
    if healthy.empty:
        return np.nan

    start = healthy["timestamp"].min()
    cutoff = start + pd.Timedelta(days=BASELINE_HEALTHY_DAYS)
    window = healthy[healthy["timestamp"] <= cutoff]
    if window.empty:
        window = healthy

    k = (window["pd_psia"] - window["pi_psia"]) / (window["frequency_hz"] ** 2)
    return float(k.replace([np.inf, -np.inf], np.nan).dropna().mean())


def _fit_delta_t_ref(df: pd.DataFrame) -> float:
    base = df.dropna(subset=["tm_c", "ti_c"])
    if base.empty:
        return np.nan
    return float((base["tm_c"] - base["ti_c"]).mean())


def _ensure_core_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    lc_map = {c.lower(): c for c in out.columns}

    if "well_name" not in out.columns and "well_name" in lc_map:
        out = out.rename(columns={lc_map["well_name"]: "well_name"})
    if "timestamp" not in out.columns:
        for cand in ["timestamp", "datetime", "date_time", "time_stamp"]:
            if cand in lc_map:
                out = out.rename(columns={lc_map[cand]: "timestamp"})
                break

    if "well_name" not in out.columns:
        raise KeyError("well_name column missing in apply_virtual_sensors input")
    if "timestamp" not in out.columns:
        raise KeyError("timestamp column missing in apply_virtual_sensors input")

    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["timestamp", "well_name"]).copy()
    return out


def apply_virtual_sensors(esp_df: pd.DataFrame) -> pd.DataFrame:
    if esp_df is None or esp_df.empty:
        return pd.DataFrame()

    out = _ensure_core_columns(esp_df)
    out = out.sort_values(["well_name", "timestamp"]).reset_index(drop=True)

    for col in ["pi_psia", "tm_c", "current_ia", "current_ib", "current_ic"]:
        if col not in out.columns:
            out[col] = np.nan
        out[f"{col}_source"] = "measured"
        out[f"{col}_confidence"] = CONFIDENCE_WEIGHTS["measured"]

    for col in ["pd_psia", "frequency_hz", "ti_c", "motor_load_pct", "current_ia", "current_ib", "current_ic"]:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")

    processed_groups = []

    for _, g in out.groupby("well_name", dropna=False):
        g = g.copy()

        k_pump = _fit_k_pump(g)
        delta_t_ref = _fit_delta_t_ref(g)

        mask_pi = g["pi_psia"].isna() & g["pd_psia"].notna() & g["frequency_hz"].notna() & pd.notna(k_pump)
        g.loc[mask_pi, "pi_psia"] = g.loc[mask_pi, "pd_psia"] - (k_pump * (g.loc[mask_pi, "frequency_hz"] ** 2))
        g.loc[mask_pi, "pi_psia_source"] = "virtual"
        g.loc[mask_pi, "pi_psia_confidence"] = CONFIDENCE_WEIGHTS["virtual_strong"]

        mask_tm = g["tm_c"].isna() & g["ti_c"].notna() & g["motor_load_pct"].notna() & g["frequency_hz"].notna() & pd.notna(delta_t_ref)
        g.loc[mask_tm, "tm_c"] = (
            g.loc[mask_tm, "ti_c"]
            + (g.loc[mask_tm, "motor_load_pct"] / 100.0)
            * (g.loc[mask_tm, "frequency_hz"] / 50.0)
            * delta_t_ref
        )
        g.loc[mask_tm, "tm_c_source"] = "virtual"
        g.loc[mask_tm, "tm_c_confidence"] = CONFIDENCE_WEIGHTS["virtual_strong"]

        phase_map = {
            "current_ia": ["current_ib", "current_ic"],
            "current_ib": ["current_ia", "current_ic"],
            "current_ic": ["current_ia", "current_ib"],
        }
        for phase, others in phase_map.items():
            m = g[phase].isna() & g[others[0]].notna() & g[others[1]].notna()
            g.loc[m, phase] = g.loc[m, others].mean(axis=1)
            g.loc[m, f"{phase}_source"] = f"virtual_{phase}"
            g.loc[m, f"{phase}_confidence"] = CONFIDENCE_WEIGHTS["virtual_weak"]

        processed_groups.append(g)

    final_df = pd.concat(processed_groups, ignore_index=True)
    final_df = final_df.sort_values(["well_name", "timestamp"]).reset_index(drop=True)
    return final_df