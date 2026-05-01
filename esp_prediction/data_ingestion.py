"""Step 2: Ingest and clean R9A ESP + Start/Stop data into SQLite.

Outputs:
- esp_raw_r9a
- esp_events_r9a
"""

# updated new and pushed again

from __future__ import annotations

import sqlite3
import sys
from fractions import Fraction
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
    return " ".join(str(text).strip().lower().replace("_", " ").replace("\n", " ").split())


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


def _coerce_datetime(date_series: pd.Series, time_series: pd.Series | None = None) -> pd.Series:
    """Robust datetime parser handling Excel serial dates + mixed text formats."""
    dnum = pd.to_numeric(date_series, errors="coerce")
    excel_dt = pd.to_datetime(dnum, unit="D", origin="1899-12-30", errors="coerce")

    dtext = date_series.astype(str).str.strip()
    if time_series is not None:
        ttext = time_series.astype(str).str.strip()
    else:
        ttext = "00:00:00"

    # Keep fallback parse, but avoid noisy warning spam by suppressing invalid pieces naturally
    mixed = pd.to_datetime(dtext + " " + ttext, **DATE_PARSE_SETTINGS)
    return excel_dt.fillna(mixed)


def _find_alias_column(columns: List[str], aliases: List[str]) -> Optional[str]:
    normalized_map = {_normalize_name(c): c for c in columns}

    # exact normalized match
    for alias in aliases:
        key = _normalize_name(alias)
        if key in normalized_map:
            return normalized_map[key]

    # contains match
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
    out = [pd.to_numeric(p, errors="coerce") for p in parts]
    return tuple(out)


def _parse_choke(val):
    if pd.isna(val):
        return None
    txt = str(val).strip().replace('"', "")
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


def _detect_header_row(preview_df: pd.DataFrame, anchor_terms: List[str]) -> int:
    """Find header row by scanning for known anchor terms in a row."""
    anchors = {_normalize_name(a) for a in anchor_terms}
    max_rows = min(len(preview_df), 120)

    for i in range(max_rows):
        row_vals = [_normalize_name(v) for v in preview_df.iloc[i].tolist()]
        # exact token hit
        if any(a in row_vals for a in anchors):
            return i
        # substring hit
        if any(any(a in cell for a in anchors) for cell in row_vals):
            return i

    return 0


def _resolve_sheet_for_well(sheet_names: List[str], well: str, kind: str) -> Optional[str]:
    patterns = [_normalize_name(p) for p in SHEET_MATCH_RULES[kind][well]]
    well_token = _normalize_name(well).replace("#", "")

    # strict pass
    for s in sheet_names:
        s_norm = _normalize_name(s)

        if kind == "esp_parameter_sheets":
            # exclude summary/start-stop for ESP data tabs
            if "summary" in s_norm or "start stop" in s_norm or "start_stop" in s_norm:
                continue
            # require well token in tab
            if well_token not in s_norm:
                continue

        if kind == "start_stop_sheets":
            # must be a start-stop tab and must contain well token
            if ("start stop" not in s_norm and "start_stop" not in s_norm):
                continue
            if well_token not in s_norm:
                continue

        if any(p in s_norm for p in patterns):
            return s

    # fallback pass (still guarded)
    for s in sheet_names:
        s_norm = _normalize_name(s)

        if kind == "esp_parameter_sheets":
            if "summary" in s_norm or "start stop" in s_norm or "start_stop" in s_norm:
                continue
            if well_token in s_norm:
                return s

        if kind == "start_stop_sheets":
            if ("start stop" in s_norm or "start_stop" in s_norm) and well_token in s_norm:
                return s

    return None


def _load_esp_sheet(xls: pd.ExcelFile, sheet: str, well_name: str, warnings: List[str]) -> pd.DataFrame:
    preview = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=120)
    header_row = _detect_header_row(preview, ESP_COLUMN_ALIASES["date"])

    raw = pd.read_excel(xls, sheet_name=sheet, header=header_row)
    raw.columns = [str(c).strip() for c in raw.columns]

    mapped: Dict[str, str] = {}
    for out_col, aliases in ESP_COLUMN_ALIASES.items():
        col = _find_alias_column(list(raw.columns), aliases)
        if col:
            mapped[out_col] = col

    # IMPORTANT: only date is mandatory; time can be absent
    missing_critical = [k for k in ["date"] if k not in mapped]
    if missing_critical:
        warnings.append(f"[{well_name}] Missing critical columns in {sheet}: {missing_critical}")
        return pd.DataFrame()

    df = pd.DataFrame()
    dt = _coerce_datetime(raw[mapped["date"]], raw[mapped["time"]] if "time" in mapped else None)

    df["timestamp"] = dt
    df["well_name"] = _normalize_well_name(well_name) or well_name

    numeric_cols = [
        "frequency_hz", "esm_active_current_amps", "total_esm_current_amps",
        "pi_psia", "pd_psia", "ti_c", "tm_c", "header_pressure_bar",
        "fthp_kgcm2", "fat_c", "motor_load_pct",
    ]
    for c in numeric_cols:
        src = mapped.get(c)
        df[c] = pd.to_numeric(raw[src], errors="coerce") if src else None

    vib_src = mapped.get("vibration_xyz")
    if vib_src:
        vib = raw[vib_src].apply(_split_triplet)
        df[["vibration_vx", "vibration_vy", "vibration_vz"]] = pd.DataFrame(vib.tolist(), index=df.index)
    else:
        df[["vibration_vx", "vibration_vy", "vibration_vz"]] = None

    cur_src = mapped.get("current_ia_ib_ic")
    if cur_src:
        cur = raw[cur_src].apply(_split_triplet)
        df[["current_ia", "current_ib", "current_ic"]] = pd.DataFrame(cur.tolist(), index=df.index)
    else:
        df[["current_ia", "current_ib", "current_ic"]] = None

    abc_src = mapped.get("abc_sec_pressure_kgcm2")
    if abc_src:
        abc = raw[abc_src].apply(_split_triplet)
        df[["sec_pressure_a", "sec_pressure_b", "sec_pressure_c"]] = pd.DataFrame(abc.tolist(), index=df.index)
    else:
        df[["sec_pressure_a", "sec_pressure_b", "sec_pressure_c"]] = None

    choke_src = mapped.get("choke_size_in")
    df["choke_size_in"] = raw[choke_src].apply(_parse_choke) if choke_src else None

    rem_src = mapped.get("remarks")
    df["remarks"] = raw[rem_src].astype(str).fillna("") if rem_src else ""

    df = df.dropna(subset=["timestamp"]).copy()
    return df


def _load_event_sheet(xls: pd.ExcelFile, sheet: str, well_name: str, warnings: List[str]) -> pd.DataFrame:
    preview = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=120)
    header_row = _detect_header_row(preview, EVENT_COLUMN_ALIASES["stop_dt"])

    raw = pd.read_excel(xls, sheet_name=sheet, header=header_row)
    raw.columns = [str(c).strip() for c in raw.columns]

    mapped = {}
    for out_col, aliases in EVENT_COLUMN_ALIASES.items():
        col = _find_alias_column(list(raw.columns), aliases)
        if col:
            mapped[out_col] = col

    if "stop_dt" not in mapped:
        fallback_stop = _find_alias_column(list(raw.columns), ["Stop date", "Stop Date", "Stop"])
        if fallback_stop:
            mapped["stop_dt"] = fallback_stop
        else:
            warnings.append(f"[{well_name}] Missing stop datetime column in {sheet}")
            return pd.DataFrame()

    ev = pd.DataFrame()
    ev["well_name"] = _normalize_well_name(well_name) or well_name
    ev["stop_dt"] = _coerce_datetime(raw[mapped["stop_dt"]], None)
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

    # Guard against zero-column DataFrame create-table SQL errors
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

    # Final sanitize columns
    esp_df.columns = [str(c).strip() if str(c).strip() else "unnamed_col" for c in esp_df.columns]
    events_df.columns = [str(c).strip() if str(c).strip() else "unnamed_col" for c in events_df.columns]

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
        for w in warnings[: LOGGING.get("max_warning_samples", 50)]:
            print(f" - {w}")

    return esp_df, events_df


if __name__ == "__main__":
    run_ingestion()