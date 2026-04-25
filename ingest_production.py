import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from database import get_connection

RAW_FOLDER = os.path.join(os.path.dirname(__file__), "data", "raw")

VALID_PLATFORMS = ['R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A']

def find_production_file():
    for f in os.listdir(RAW_FOLDER):
        if "production" in f.lower() or "oil" in f.lower():
            return os.path.join(RAW_FOLDER, f)
    return None

def extract_date_from_filename(filename):
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', filename)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"
    return datetime.today().strftime('%Y-%m-%d')

def normalize_well_name(well_name):
    """Remove leading zero from single digit platform number. R07A → R7A"""
    if not well_name:
        return well_name
    return re.sub(r'R0(\d)A', r'R\1A', well_name)

def is_valid_well_name(well_name):
    """
    Valid well names must:
    1. Contain '#' character
    2. Not be purely numeric
    3. Not be empty/NaN/Total/Well Name
    """
    if not well_name or well_name in ['nan', 'None', 'Well Name', 'Total', '']:
        return False
    try:
        float(well_name)
        return False  # Pure number = serial number, not well name
    except ValueError:
        pass
    if '#' not in well_name:
        return False
    return True

def derive_platform_from_well_name(well_name):
    """
    Derive platform from well name — more reliable than merged cell detection.
    
    This solves the problem where Excel merged cells don't align with
    the first well of each platform section, causing wrong platform assignment.
    
    R7A#xx  → R-7A
    R9A#xx  → R-9A
    R10A#xx → R-10A
    R12A#xx → R-12A
    R12B#xx → R-12B
    R13A#xx → R-13A
    """
    if not well_name:
        return None

    # Normalize first — remove leading zeros
    well_clean = re.sub(r'R0(\d)A', r'R\1A', well_name.upper())

    platform_map = {
        'R7A':  'R-7A',
        'R9A':  'R-9A',
        'R10A': 'R-10A',
        'R12A': 'R-12A',
        'R12B': 'R-12B',
        'R13A': 'R-13A',
    }

    for prefix, platform in platform_map.items():
        if well_clean.startswith(prefix):
            return platform

    return None

def ingest_oil_production(df_sheet, date, conn):
    inserted = 0
    skipped = 0
    current_platform = None

    for idx, row in df_sheet.iterrows():

        # ── STOP AT TOTAL ROW ─────────────────────────────────────────────
        col_b = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''
        col_c = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
        if 'Total' in col_b or 'Total' in col_c:
            print(f"   Reached Total row at index {idx} — stopping")
            break

        # ── WELL NAME EXTRACTION ──────────────────────────────────────────
        well_name_raw = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None

        if not is_valid_well_name(well_name_raw):
            continue

        # ── PLATFORM DETECTION ────────────────────────────────────────────
        # Method 1: Column A (merged cells — unreliable at section boundaries)
        platform_val = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        if platform_val in VALID_PLATFORMS:
            current_platform = platform_val

        # Method 2: Derive from well name (ALWAYS overrides column A)
        # This is more reliable because well name never lies
        derived = derive_platform_from_well_name(well_name_raw)
        if derived:
            current_platform = derived

        if not current_platform:
            continue

        # ── NORMALIZE WELL NAME ───────────────────────────────────────────
        well_name = normalize_well_name(well_name_raw)

        # ── DATA EXTRACTION ───────────────────────────────────────────────
        try:
            liquid_rate = pd.to_numeric(row.iloc[3], errors='coerce')
            oil_rate    = pd.to_numeric(row.iloc[4], errors='coerce')
            prod_loss   = pd.to_numeric(row.iloc[5], errors='coerce')
            well_status = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else None
            remarks     = str(row.iloc[7]).strip() if pd.notna(row.iloc[7]) else None

            if well_status in ['nan', 'None', '-']: well_status = None
            if remarks in ['nan', 'None', '-']:     remarks = None

            conn.execute("""
                INSERT OR IGNORE INTO oil_production
                (date, platform, well_name, liquid_rate_bpd, oil_rate_bpd,
                production_loss_bbl, well_status, remarks)
                VALUES (?,?,?,?,?,?,?,?)
            """, (date, current_platform, well_name, liquid_rate,
                  oil_rate, prod_loss, well_status, remarks))
            inserted += 1

        except Exception as e:
            skipped += 1

    return inserted, skipped

def ingest_water_injection(df_sheet, date, conn):
    inserted = 0
    skipped = 0
    current_platform = None
    current_header_pressure = None

    for idx, row in df_sheet.iterrows():

        platform_val = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        if platform_val in VALID_PLATFORMS:
            current_platform = platform_val
            hp = pd.to_numeric(row.iloc[1], errors='coerce')
            if pd.notna(hp):
                current_header_pressure = float(hp)

        well_name_raw = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
        if not is_valid_well_name(well_name_raw):
            continue

        # Derive platform from well name for reliability
        derived = derive_platform_from_well_name(well_name_raw)
        if derived:
            current_platform = derived

        well_name = normalize_well_name(well_name_raw)

        if not current_platform:
            continue

        try:
            choke      = str(row.iloc[3]).strip() if pd.notna(row.iloc[3]) else None
            ithp       = pd.to_numeric(row.iloc[4], errors='coerce')
            status     = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else None
            flow_sm3   = pd.to_numeric(row.iloc[6], errors='coerce')
            flow_bph   = pd.to_numeric(row.iloc[7], errors='coerce')
            inj_hours  = pd.to_numeric(row.iloc[8], errors='coerce')
            cumulative = pd.to_numeric(row.iloc[9], errors='coerce')
            planned    = pd.to_numeric(row.iloc[10], errors='coerce')

            if choke in ['nan', 'None']:   choke = None
            if status in ['nan', 'None']:  status = None

            conn.execute("""
                INSERT OR IGNORE INTO water_injection
                (date, platform, well_name, header_pressure_ksc, choke_size,
                ithp, status, flow_rate_sm3hr, flow_rate_bpd,
                injecting_hours, cumulative_flow_bbl, planned_wi_bpd)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (date, current_platform, well_name, current_header_pressure,
                  choke, ithp, status, flow_sm3, flow_bph,
                  inj_hours, cumulative, planned))
            inserted += 1

        except Exception as e:
            skipped += 1

    return inserted, skipped

def ingest_water_injection_base(df, conn):
    inserted = 0
    skipped = 0

    for idx, row in df.iterrows():

        date_val = row.iloc[0]
        if pd.isna(date_val):
            continue

        try:
            date = pd.to_datetime(date_val, dayfirst=True, errors='coerce')
            if pd.isna(date):
                continue
            date_str = date.strftime('%Y-%m-%d')

            well_name_raw = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None
            if not is_valid_well_name(well_name_raw):
                continue

            well_name = normalize_well_name(well_name_raw)
            platform  = derive_platform_from_well_name(well_name_raw)

            choke      = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
            ithp       = pd.to_numeric(row.iloc[3], errors='coerce')
            status     = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else None
            flow_sm3   = pd.to_numeric(row.iloc[5], errors='coerce')
            flow_bph   = pd.to_numeric(row.iloc[6], errors='coerce')
            inj_hours  = pd.to_numeric(row.iloc[7], errors='coerce')
            cumulative = pd.to_numeric(row.iloc[8], errors='coerce')
            planned    = pd.to_numeric(row.iloc[9], errors='coerce')

            if choke in ['nan', 'None']:  choke = None
            if status in ['nan', 'None']: status = None

            conn.execute("""
                INSERT OR IGNORE INTO water_injection
                (date, platform, well_name, choke_size, ithp, status,
                flow_rate_sm3hr, flow_rate_bpd, injecting_hours,
                cumulative_flow_bbl, planned_wi_bpd)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (date_str, platform, well_name, choke, ithp, status,
                  flow_sm3, flow_bph, inj_hours, cumulative, planned))
            inserted += 1

        except Exception as e:
            skipped += 1

    return inserted, skipped

def ingest_production():
    filepath = find_production_file()
    if not filepath:
        print("❌ No production file found in data/raw folder!")
        return

    print(f"📂 Reading production file: {os.path.basename(filepath)}")
    date = extract_date_from_filename(os.path.basename(filepath))
    print(f"   Date extracted: {date}")

    xl = pd.ExcelFile(filepath)
    print(f"   Sheets found: {xl.sheet_names}")

    conn = get_connection()
    total_inserted = 0
    total_skipped  = 0

    for sheet in xl.sheet_names:
        sheet_lower = sheet.lower().strip()
        df = pd.read_excel(filepath, sheet_name=sheet, header=None)

        if 'overall' in sheet_lower or 'oil' in sheet_lower or 'production' in sheet_lower:
            if 'water' not in sheet_lower and 'injection' not in sheet_lower:
                print(f"\n   Processing oil production sheet: '{sheet}'")
                ins, skip = ingest_oil_production(df, date, conn)
                print(f"   ✅ Inserted: {ins}, Skipped: {skip}")
                total_inserted += ins
                total_skipped  += skip

        elif 'water' in sheet_lower or 'injection' in sheet_lower or 'wi' in sheet_lower:
            if 'base' in sheet_lower:
                print(f"\n   Processing water injection BASE sheet: '{sheet}'")
                ins, skip = ingest_water_injection_base(df, conn)
            else:
                print(f"\n   Processing water injection sheet: '{sheet}'")
                ins, skip = ingest_water_injection(df, date, conn)
            print(f"   ✅ Inserted: {ins}, Skipped: {skip}")
            total_inserted += ins
            total_skipped  += skip

        else:
            print(f"\n   ⏭️  Skipping sheet: '{sheet}'")

    conn.commit()
    conn.close()

    print(f"\n✅ Production ingestion complete!")
    print(f"   Total records inserted: {total_inserted}")
    print(f"   Total records skipped:  {total_skipped}")

if __name__ == "__main__":
    ingest_production()