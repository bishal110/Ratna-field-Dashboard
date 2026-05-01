from __future__ import annotations

import pandas as pd

from esp_prediction.config import OCC_THRESHOLDS, OCC_STABILIZATION_DAYS, OCC_INTERVENTION_KEYWORDS, OCC_PIGGING_KEYWORDS


def detect_occ(daily_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return daily_df
    df = daily_df.sort_values(["well_name", "date"]).copy()
    df["occ_active"] = False
    df["occ_type"] = None
    df["occ_description"] = None

    events_dates = set(pd.to_datetime(events_df.get("start_dt", pd.Series(dtype="datetime64[ns]")).dropna()).dt.date.tolist()) if not events_df.empty else set()

    for well, g in df.groupby("well_name"):
        active_until = None
        active_type = None
        idxs = g.index.tolist()
        for i, idx in enumerate(idxs):
            row = df.loc[idx]
            if i == 0:
                continue
            prev = df.loc[idxs[i - 1]]
            occ_type = None
            desc = None
            if row.get("choke_size_in") != prev.get("choke_size_in") and pd.notna(row.get("choke_size_in")) and pd.notna(prev.get("choke_size_in")):
                occ_type = "OCC_CHOKE"; desc = f"Choke changed {prev.get('choke_size_in')} -> {row.get('choke_size_in')}"
            elif abs((row.get("frequency_hz") or 0) - (prev.get("frequency_hz") or 0)) > OCC_THRESHOLDS["frequency_delta_hz"]:
                occ_type = "OCC_FREQUENCY"; desc = "Frequency step change"
            elif pd.notna(row.get("flow_rate_bpd")) and pd.notna(prev.get("flow_rate_bpd")) and prev.get("flow_rate_bpd") != 0 and abs(row.get("flow_rate_bpd")-prev.get("flow_rate_bpd"))/abs(prev.get("flow_rate_bpd")) > OCC_THRESHOLDS["flowrate_delta_ratio"]:
                occ_type = "OCC_FLOWRATE"; desc = "Flowrate changed >15%"
            elif pd.notna(row.get("header_pressure_bar")) and pd.notna(prev.get("header_pressure_bar")) and abs(row.get("header_pressure_bar")-prev.get("header_pressure_bar")) > OCC_THRESHOLDS["backpressure_delta_bar"]:
                occ_type = "OCC_BACKPRESSURE"; desc = "Backpressure changed >3 bar"
            elif row["date"].date() in events_dates:
                occ_type = "OCC_RESTART"; desc = "Restart event detected"
            elif any(k in str(row.get("remarks", "")).lower() for k in OCC_INTERVENTION_KEYWORDS):
                occ_type = "OCC_INTERVENTION"; desc = "Intervention remark detected"
            elif any(k in str(row.get("remarks", "")).lower() for k in OCC_PIGGING_KEYWORDS):
                occ_type = "OCC_PIGGING"; desc = "Pigging remark detected"

            if occ_type:
                days = OCC_STABILIZATION_DAYS[occ_type]
                active_until = row["date"] + pd.Timedelta(days=days)
                active_type = occ_type
                df.at[idx, "occ_description"] = desc

            if active_until is not None and row["date"] <= active_until:
                df.at[idx, "occ_active"] = True
                df.at[idx, "occ_type"] = active_type

    return df
