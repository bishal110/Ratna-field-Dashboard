from __future__ import annotations

import pandas as pd
import numpy as np

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


def apply_virtual_sensors(esp_df: pd.DataFrame) -> pd.DataFrame:
    if esp_df.empty:
        return esp_df
    out = esp_df.copy().sort_values(["well_name", "timestamp"])

    for col in ["pi_psia", "tm_c", "current_ia", "current_ib", "current_ic"]:
        out[f"{col}_source"] = "measured"
        out[f"{col}_confidence"] = CONFIDENCE_WEIGHTS["measured"]

    def _process(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
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

        for phase, others in {
            "current_ia": ["current_ib", "current_ic"],
            "current_ib": ["current_ia", "current_ic"],
            "current_ic": ["current_ia", "current_ib"],
        }.items():
            m = g[phase].isna() & g[others[0]].notna() & g[others[1]].notna()
            g.loc[m, phase] = g.loc[m, others].mean(axis=1)
            g.loc[m, f"{phase}_source"] = f"virtual_{phase}"
            g.loc[m, f"{phase}_confidence"] = CONFIDENCE_WEIGHTS["virtual_weak"]

        return g

    out = out.groupby("well_name", group_keys=False).apply(_process)
    return out.reset_index(drop=True)
