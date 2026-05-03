"""Step 2: Ingest and clean R9A ESP + Start/Stop data into SQLite.

Outputs:
- esp_raw_r9a
- esp_events_r9a
"""

# updated new and pushed again

from __future__ import annotations

import re
import sqlite3
import sys
from datetime import datetime as _dt_cls
from datetime import time as _dt_time
from fractions import Fraction
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from esp_prediction.config import (
    CHOKE_PARSE_SETTINGS,
    DATE_PARSE_SETTINGS,
    DB_PATH,
    ESP_COLUMN_ALIASES,
    EVENT_COLUMN_ALIASES,
    FAILURE_KEYWORDS,
    LOGGING,
    R9A_FILE_CONFIG,
    RAW_DATA_REPO_FALLBACK,
    SHEET_MATCH_RULES,
    TABLE_NAMES,
    TEXT_AS_NAN_TOKENS,
    WELL_NAME_NORMALIZATION,
    CANONICAL_WELLS,
    CONFIRMED_FAILURE_OVERRIDES,
)


def _normalize_name(text: str) -> str:
    return " ".join(str(text).strip().lower().replace("_", " ").split())


def _normalize_well_name(raw: str) -> Optional[str]:
    if raw is None:
        return None
    cleaned = str(raw).strip().upper().replace(" ", "")
    return WELL_NAME_NORMALIZATION.get(cleaned)


def find_r9a_file() -> Path:
    folder = Path(R9A_FILE_CONFIG["folder"])
    if not folder.exists():
        folder = Path(RAW_DATA_REPO_FALLBACK)

    all_xlsx = [p for p in folder.glob(f"*{R9A_FILE_CONFIG['extension']}") if p.is_file()]
    keywords = [k.lower() for k in R9A_FILE_CONFIG["keywords"]]
    matches = [p for p in all_xlsx if any(k in p.name.lower() for k in keywords)]

    if matches:
        chosen = max(matches, key=lambda p: p.stat().st_mtime)
    else:
        fallback = folder / R9A_FILE_CONFIG["fallback_exact"]
        if fallback.exists():
            chosen = fallback
        else:
            raise FileNotFoundError(R9A_FILE_CONFIG["error_if_missing"])

    if LOGGING.get("print_discovered_file", True):
        print(f"[INFO] R9A file found: {chosen}")
    return chosen




def _parse_time_component(tval) -> Optional[tuple]:
    """Return (hour, minute) from various time value formats, or None."""
    try:
        if pd.isna(tval):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(tval, _dt_time):
        return (tval.hour, tval.minute)
    txt = re.sub(r"\s*[Hh][Rr][Ss]\.?\s*$", "", str(tval).strip()).strip()
    if not txt:
        return None
    # Pure digits: "1158" -> (11, 58), "855" -> (8, 55), "14" -> (14, 0)
    m = re.match(r"^(\d+)$", txt)
    if m:
        n = int(m.group(1))
        if n < 24:
            return (n, 0)
        if n < 2400:
            return (n // 100, n % 100)
    # HH:MM with optional spaces around colon
    m = re.match(r"^(\d{1,2})\s*:\s*(\d{2})", txt)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return None


def _parse_date_component(dval) -> Optional[pd.Timestamp]:
    """Return normalized date Timestamp, handling datetime objects, Excel serials, and text."""
    try:
        if pd.isna(dval):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(dval, pd.Timestamp):
        return dval.normalize()
    if isinstance(dval, _dt_cls):
        return pd.Timestamp(dval).normalize()
    n = pd.to_numeric(dval, errors="coerce")
    if pd.notna(n):
        return (pd.Timestamp("1899-12-30") + pd.Timedelta(days=float(n))).normalize()
    txt = str(dval).strip()
    try:
        return pd.to_datetime(txt, dayfirst=True, errors="raise").normalize()
    except Exception:
        return None


def _coerce_datetime(date_series: pd.Series, time_series: pd.Series | None = None) -> pd.Series:
    """Robust datetime parser: handles Excel serial dates, datetime objects, and mixed text formats
    including time strings like '1158 hrs', '10:25 Hrs', datetime.time objects, etc."""
    results = []
    for i in range(len(date_series)):
        dt = _parse_date_component(date_series.iloc[i])
        if dt is None:
            results.append(pd.NaT)
            continue
        if time_series is not None:
            hm = _parse_time_component(time_series.iloc[i])
            if hm is not None:
                dt = dt.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
        results.append(dt)
    return pd.Series(results, dtype="datetime64[ns]")


def _find_alias_column(columns: List[str], aliases: List[str]) -> Optional[str]:
    normalized_map = {_normalize_name(c): c for c in columns}
    for alias in aliases:
        key = _normalize_name(alias)
        if key in normalized_map:
            return normalized_map[key]
    for alias in aliases:
        alias_key = _normalize_name(alias)
        for key, original in normalized_map.items():
            if alias_key in key:
                return original
    return None


def _split_triplet(val) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if pd.isna(val):
        return (None, None, None)
    txt = str(val).strip()
    parts = [p.strip() for p in txt.split("/")]
    if len(parts) != 3:
        return (None, None, None)
    out = []
    for p in parts:
        out.append(pd.to_numeric(p, errors="coerce"))
    return tuple(out)


def _parse_choke(val):
    if pd.isna(val):
        return None
    txt = str(val).strip()
    if txt in TEXT_AS_NAN_TOKENS:
        return None
    try:
        if CHOKE_PARSE_SETTINGS.get("fraction_separator", "/") in txt:
            return float(Fraction(txt))
        return float(txt)
    except Exception:
        return pd.to_numeric(txt, errors="coerce")


def _classify_failure(reason_text: str) -> int:
    text = str(reason_text or "").lower()
    for label in (3, 2, 1):
        if any(k.lower() in text for k in FAILURE_KEYWORDS[label]):
            return label
    return 0


def _detect_header_row(preview_df: pd.DataFrame, anchor_terms: List[str], all_alias_terms: List[str] | None = None) -> int:
    """Return the row with the most alias matches — avoids false matches in config rows."""
    scorers = {_normalize_name(a) for a in (all_alias_terms if all_alias_terms else anchor_terms)
               if len(_normalize_name(a)) > 2}
    max_rows = min(len(preview_df), 120)
    best_row, best_score = 0, -1
    for i in range(max_rows):
        row_vals = [_normalize_name(v) for v in preview_df.iloc[i].tolist()]
        score = sum(
            1 for term in scorers
            if any(term == cell or (len(term) > 3 and term in cell) for cell in row_vals)
        )
        if score > best_score:
            best_score = score
            best_row = i
    return best_row


def _resolve_sheet_for_well(sheet_names: List[str], well: str, kind: str) -> Optional[str]:
    patterns = SHEET_MATCH_RULES[kind][well]
    best_sheet: Optional[str] = None
    best_score = -1.0
    for s in sheet_names:
        s_norm = _normalize_name(s)
        matched = [_normalize_name(p) for p in patterns if _normalize_name(p) in s_norm]
        if not matched:
            continue
        count = len(matched)
        # Specificity: fraction of sheet name covered by matched patterns — favours shorter, more specific sheets
        specificity = sum(len(m) for m in matched) / max(len(s_norm), 1)
        score = count * 10.0 + specificity
        if score > best_score:
            best_score = score
            best_sheet = s
    return best_sheet


def _load_esp_sheet(xls: pd.ExcelFile, sheet: str, well_name: str, warnings: List[str]) -> pd.DataFrame:
    preview = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=80)
    _esp_all = [a for aliases in ESP_COLUMN_ALIASES.values() for a in aliases]
    header_row = _detect_header_row(preview, ESP_COLUMN_ALIASES["date"], _esp_all)
    raw = pd.read_excel(xls, sheet_name=sheet, header=header_row)
    raw.columns = [str(c).strip() for c in raw.columns]

    mapped: Dict[str, str] = {}
    for out_col, aliases in ESP_COLUMN_ALIASES.items():
        col = _find_alias_column(list(raw.columns), aliases)
        if col:
            mapped[out_col] = col

    if "date" not in mapped:
        warnings.append(f"[{well_name}] Missing critical column 'date' in {sheet}")
        return pd.DataFrame()

    df = pd.DataFrame()
    # Forward-fill date: some sheets only put the date on the first reading of the day
    date_raw = raw[mapped["date"]].ffill()
    time_raw = raw[mapped["time"]] if "time" in mapped else None
    dt = _coerce_datetime(date_raw, time_raw)
    df["timestamp"] = dt
    df["well_name"] = _normalize_well_name(well_name) or well_name

    numeric_cols = [
        "frequency_hz", "esm_active_current_amps", "total_esm_current_amps",
        "pi_psia", "pd_psia", "ti_c", "tm_c", "header_pressure_bar",
        "fthp_kgcm2", "fat_c", "motor_load_pct",
    ]
    for c in numeric_cols:
        src = mapped.get(c)
        df[c] = pd.to_numeric(raw[src], errors="coerce") if src else np.nan

    vib_src = mapped.get("vibration_xyz")
    if vib_src:
        vib = raw[vib_src].apply(_split_triplet)
        df[["vibration_vx", "vibration_vy", "vibration_vz"]] = pd.DataFrame(vib.tolist(), index=df.index)
    else:
        df[["vibration_vx", "vibration_vy", "vibration_vz"]] = np.nan

    cur_src = mapped.get("current_ia_ib_ic")
    if cur_src:
        cur = raw[cur_src].apply(_split_triplet)
        df[["current_ia", "current_ib", "current_ic"]] = pd.DataFrame(cur.tolist(), index=df.index)
    else:
        df[["current_ia", "current_ib", "current_ic"]] = np.nan

    abc_src = mapped.get("abc_sec_pressure_kgcm2")
    if abc_src:
        abc = raw[abc_src].apply(_split_triplet)
        df[["sec_pressure_a", "sec_pressure_b", "sec_pressure_c"]] = pd.DataFrame(abc.tolist(), index=df.index)
    else:
        df[["sec_pressure_a", "sec_pressure_b", "sec_pressure_c"]] = np.nan

    choke_src = mapped.get("choke_size_in")
    df["choke_size_in"] = raw[choke_src].apply(_parse_choke) if choke_src else np.nan

    rem_src = mapped.get("remarks")
    df["remarks"] = raw[rem_src].astype(str).fillna("") if rem_src else ""

    df = df.dropna(subset=["timestamp"]).copy()
    return df


def _load_event_sheet(xls: pd.ExcelFile, sheet: str, well_name: str, warnings: List[str]) -> pd.DataFrame:
    preview = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=80)
    _ev_all = [a for aliases in EVENT_COLUMN_ALIASES.values() for a in aliases]
    header_row = _detect_header_row(preview, EVENT_COLUMN_ALIASES["stop_dt"], _ev_all)
    raw = pd.read_excel(xls, sheet_name=sheet, header=header_row)
    raw.columns = [str(c).strip() for c in raw.columns]

    mapped = {}
    for out_col, aliases in EVENT_COLUMN_ALIASES.items():
        col = _find_alias_column(list(raw.columns), aliases)
        if col:
            mapped[out_col] = col

    if "stop_dt" not in mapped:
        warnings.append(f"[{well_name}] Missing stop datetime column in {sheet}")
        return pd.DataFrame()

    ev = pd.DataFrame()
    # Some sheets split date and time into separate columns; combine them if a stop_time col exists
    stop_time_col = raw[mapped["stop_time"]] if "stop_time" in mapped else None
    ev["stop_dt"] = _coerce_datetime(raw[mapped["stop_dt"]], stop_time_col)
    # Assign well_name AFTER stop_dt so the index exists and scalar broadcasts correctly
    ev["well_name"] = _normalize_well_name(well_name) or well_name
    ev["start_dt"] = _coerce_datetime(raw[mapped.get("start_dt")], None) if "start_dt" in mapped else pd.NaT
    ev["run_hours"] = pd.to_numeric(raw[mapped.get("run_hours")], errors="coerce") if "run_hours" in mapped else None
    ev["shutdown_hours"] = pd.to_numeric(raw[mapped.get("shutdown_hours")], errors="coerce") if "shutdown_hours" in mapped else None
    ev["reason_text"] = raw[mapped.get("reason_text")].astype(str).fillna("") if "reason_text" in mapped else ""

    ev = ev.dropna(subset=["stop_dt"]).copy()
    ev["failure_label"] = ev["reason_text"].apply(_classify_failure)

    for override in CONFIRMED_FAILURE_OVERRIDES:
        mask = (
            (ev["well_name"] == override["well_name"])
            & (ev["stop_dt"].dt.date.astype(str) == override["event_date"])
        )
        ev.loc[mask, "failure_label"] = override["force_label"]

    ev["duration_hrs"] = ev["shutdown_hours"].fillna(ev["run_hours"])
    return ev


def run_ingestion() -> Tuple[pd.DataFrame, pd.DataFrame]:
    warnings: List[str] = []
    file_path = find_r9a_file()
    xls = pd.ExcelFile(file_path)
    sheet_names = xls.sheet_names

    esp_frames = []
    event_frames = []

    for well in CANONICAL_WELLS:
        esp_sheet = _resolve_sheet_for_well(sheet_names, well, "esp_parameter_sheets")
        if esp_sheet:
            esp_frames.append(_load_esp_sheet(xls, esp_sheet, well, warnings))
        else:
            warnings.append(f"ESP sheet not found for {well}")

        event_sheet = _resolve_sheet_for_well(sheet_names, well, "start_stop_sheets")
        if event_sheet:
            event_frames.append(_load_event_sheet(xls, event_sheet, well, warnings))
        else:
            warnings.append(f"Event sheet not found for {well}")

    esp_df = pd.concat(esp_frames, ignore_index=True) if esp_frames else pd.DataFrame()
    events_df = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()

    if not esp_df.empty:
        esp_df = esp_df.sort_values(["well_name", "timestamp"]).reset_index(drop=True)

    if not events_df.empty:
        events_df = events_df.sort_values(["well_name", "stop_dt"]).reset_index(drop=True)

    if esp_df.empty and len(esp_df.columns) == 0:
        esp_df = pd.DataFrame(columns=[
            "timestamp", "well_name", "frequency_hz", "esm_active_current_amps", "total_esm_current_amps",
            "pi_psia", "pd_psia", "ti_c", "tm_c", "vibration_vx", "vibration_vy", "vibration_vz",
            "current_ia", "current_ib", "current_ic", "sec_pressure_a", "sec_pressure_b", "sec_pressure_c",
            "header_pressure_bar", "fthp_kgcm2", "fat_c", "motor_load_pct", "choke_size_in", "remarks",
        ])
    if events_df.empty and len(events_df.columns) == 0:
        events_df = pd.DataFrame(columns=[
            "well_name", "stop_dt", "start_dt", "run_hours", "shutdown_hours", "reason_text", "failure_label", "duration_hrs"
        ])

    with sqlite3.connect(DB_PATH) as conn:
        esp_df.to_sql(TABLE_NAMES["esp_raw"], conn, if_exists="replace", index=False)
        events_df.to_sql(TABLE_NAMES["esp_events"], conn, if_exists="replace", index=False)

    print("\n=== STEP 2 INGESTION SUMMARY ===")
    print(f"File used: {file_path}")
    if not esp_df.empty:
        print("Rows per well (esp_raw_r9a):")
        print(esp_df.groupby("well_name").size().to_string())
        print(f"ESP date range: {esp_df['timestamp'].min()} -> {esp_df['timestamp'].max()}")
    else:
        print("No ESP rows ingested.")

    if not events_df.empty:
        print("Label distribution (esp_events_r9a):")
        print(events_df["failure_label"].value_counts(dropna=False).sort_index().to_string())
        print(f"Events date range: {events_df['stop_dt'].min()} -> {events_df['stop_dt'].max()}")
    else:
        print("No event rows ingested.")

    if warnings:
        print("Parsing warnings:")
        for w in warnings[: LOGGING.get("max_warning_samples", 20)]:
            print(f" - {w}")

    return esp_df, events_df


if __name__ == "__main__":
    run_ingestion()
