from __future__ import annotations

import pandas as pd

from esp_prediction.config import (
    OCC_INTERVENTION_KEYWORDS,
    OCC_PIGGING_KEYWORDS,
    OCC_STABILIZATION_DAYS,
    OCC_THRESHOLDS,
)


def _to_num(v):
    return pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]


def detect_occ(daily_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df is None or daily_df.empty:
        return pd.DataFrame() if daily_df is None else daily_df

    df = daily_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "well_name" not in df.columns or "date" not in df.columns:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["well_name", "date"]).sort_values(["well_name", "date"]).copy()

    df["occ_active"] = False
    df["occ_type"] = None
    df["occ_description"] = None

    events_dates = set()
    if events_df is not None and not events_df.empty:
        ev = events_df.copy()
        ev.columns = [str(c).strip() for c in ev.columns]
        start_col = "start_dt" if "start_dt" in ev.columns else ("stop_dt" if "stop_dt" in ev.columns else None)
        if start_col:
            events_dates = set(pd.to_datetime(ev[start_col], errors="coerce").dropna().dt.date.tolist())

    for _, g in df.groupby("well_name", sort=False):
        active_until = None
        active_type = None
        idxs = g.index.tolist()

        for i, idx in enumerate(idxs):
            row = df.loc[idx]
            occ_type = None
            desc = None

            if i > 0:
                prev = df.loc[idxs[i - 1]]

                row_choke = row.get("choke_size_in")
                prev_choke = prev.get("choke_size_in")
                if pd.notna(row_choke) and pd.notna(prev_choke) and row_choke != prev_choke:
                    occ_type = "OCC_CHOKE"
                    desc = f"Choke changed {prev_choke} -> {row_choke}"
                else:
                    row_f = _to_num(row.get("frequency_hz"))
                    prev_f = _to_num(prev.get("frequency_hz"))
                    if pd.notna(row_f) and pd.notna(prev_f) and abs(row_f - prev_f) > OCC_THRESHOLDS["frequency_delta_hz"]:
                        occ_type = "OCC_FREQUENCY"
                        desc = "Frequency step change"
                    else:
                        row_q = _to_num(row.get("flow_rate_bpd"))
                        prev_q = _to_num(prev.get("flow_rate_bpd"))
                        if pd.notna(row_q) and pd.notna(prev_q) and prev_q != 0 and abs(row_q - prev_q) / abs(prev_q) > OCC_THRESHOLDS["flowrate_delta_ratio"]:
                            occ_type = "OCC_FLOWRATE"
                            desc = "Flowrate changed > threshold"
                        else:
                            row_bp = _to_num(row.get("header_pressure_bar"))
                            prev_bp = _to_num(prev.get("header_pressure_bar"))
                            if pd.notna(row_bp) and pd.notna(prev_bp) and abs(row_bp - prev_bp) > OCC_THRESHOLDS["backpressure_delta_bar"]:
                                occ_type = "OCC_BACKPRESSURE"
                                desc = "Backpressure changed > threshold"

            if occ_type is None and row["date"].date() in events_dates:
                occ_type = "OCC_RESTART"
                desc = "Restart event detected"

            remarks = str(row.get("remarks", "")).lower()
            if occ_type is None and any(k.lower() in remarks for k in OCC_INTERVENTION_KEYWORDS):
                occ_type = "OCC_INTERVENTION"
                desc = "Intervention remark detected"

            if occ_type is None and any(k.lower() in remarks for k in OCC_PIGGING_KEYWORDS):
                occ_type = "OCC_PIGGING"
                desc = "Pigging remark detected"

            if occ_type:
                days = OCC_STABILIZATION_DAYS.get(occ_type, 1)
                active_until = row["date"] + pd.Timedelta(days=days)
                active_type = occ_type
                df.at[idx, "occ_description"] = desc

            if active_until is not None and row["date"] <= active_until:
                df.at[idx, "occ_active"] = True
                df.at[idx, "occ_type"] = active_type

    return df