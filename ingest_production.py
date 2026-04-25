import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from database import get_connection

RAW_FOLDER = os.path.join(os.path.dirname(__file__), "data", "raw")

# ── VALID PLATFORMS ───────────────────────────────────────────────────────────
# Only these 6 platforms exist in Ratna field
# Any other text in platform column is a label, summary or pipeline description
# and must be rejected
VALID_PLATFORMS = ['R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A']

def find_production_file():
    """Find the production Excel file in data/raw folder"""
    for f in os.listdir(RAW_FOLDER):
        if "production" in f.lower() or "oil" in f.lower():
            return os.path.join(RAW_FOLDER, f)
    return None

def extract_date_from_filename(filename):
    """
    Extract date from filename like 'Ratna Oil Production 18.04.2026 1800hrs'
    Returns date string in YYYY-MM-DD format
    """
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', filename)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"
    # If no date found in filename, use today
    return datetime.today().strftime('%Y-%m-%d')

def normalize_well_name(well_name):
    """
    Normalize well name to remove inconsistencies.
    
    Problem: Same well written as R07A#4H and R7A#4H
    Rule: Remove leading zero from single digit platform numbers
    
    Examples:
    R07A#4H  → R7A#4H   (leading zero removed)
    R07A#5   → R7A#5    (leading zero removed)
    R10A#01  → R10A#01  (unchanged, 10 is already 2 digits)
    R12A#02Z → R12A#02Z (unchanged)
    R13A#01  → R13A#01  (unchanged)
    """
    if not well_name:
        return well_name
    # Pattern: R + leading zero + single digit + A + rest
    # Replace R0XA with RXA where X is single digit
    normalized = re.sub(r'R0(\d)A', r'R\1A', well_name)
    return normalized

def is_valid_well_name(well_name):
    """
    Check if a string is a real well name.
    
    Valid well names in Ratna field always:
    1. Contain '#' character (e.g. R7A#01, R12A#02Z, R07A#4H)
    2. Are NOT purely numeric (serial numbers 1,2,3 are not well names)
    3. Are NOT empty or NaN
    
    Invalid examples that must be rejected:
    - '1', '2', '13', '25' → serial numbers (column B bleeds into column C)
    - 'Total' → summary row
    - 'Well Name' → header row
    - 'R-10A (R-10A -> HRA new Line)' → pipeline description
    - 'nan', 'None' → empty cells
    """
    if not well_name or well_name in ['nan', 'None', 'Well Name', 'Total', '']:
        return False
    
    # Reject purely numeric values (serial numbers)
    try:
        float(well_name)
        return False  # It's a number, not a well name
    except ValueError:
        pass  # Good, not a pure number
    
    # Must contain '#' — all real Ratna well names have this
    if '#' not in well_name:
        return False
    
    return True

def ingest_oil_production(df_sheet, date, conn):
    """
    Process oil production sheet and insert into database.
    
    The sheet structure:
    - Column A (index 0): Platform name (merged cells) — only valid if in VALID_PLATFORMS
    - Column B (index 1): Serial number — we IGNORE this, it's not a well name
    - Column C (index 2): Well name — R7A#01 format
    - Column D (index 3): PVT Comp. Total Liquid Rate (bbl/d)
    - Column E (index 4): PVT Comp. Oil Rate (bbl/d)  
    - Column F (index 5): Total Production Loss (bbl)
    - Column G (index 6): Well Flowing/Non-Flowing status
    - Column H (index 7): Remarks
    
    Stop processing when we hit the 'Total' row (row 35 in Excel)
    Everything below Total is MLP, diesel, ESP summary — not needed
    """
    inserted = 0
    skipped = 0
    current_platform = None

    for idx, row in df_sheet.iterrows():
        
        # ── PLATFORM DETECTION ────────────────────────────────────────────────
        # Column A has platform names in merged cells
        # When pandas reads merged cells, only the first row has the value
        # subsequent rows show NaN — so we track current_platform
        platform_val = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        
        # ONLY update platform if it exactly matches a known platform name
        # This rejects pipeline labels like "R-10A (R-10A -> HRA new Line)"
        if platform_val in VALID_PLATFORMS:
            current_platform = platform_val
        
        # ── STOP AT TOTAL ROW ─────────────────────────────────────────────────
        # Row 35 in Excel has "Total" in column B or C
        # Everything below is summary data we don't need
        col_b = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''
        col_c = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
        if 'Total' in col_b or 'Total' in col_c:
            print(f"   Reached Total row at index {idx} — stopping oil production ingestion")
            break
        
        # ── WELL NAME EXTRACTION AND VALIDATION ───────────────────────────────
        # Well name is in Column C (index 2)
        # Column B (index 1) has serial numbers — we skip that
        well_name_raw = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
        
        # Validate well name
        if not is_valid_well_name(well_name_raw):
            continue
        
        # Normalize well name (remove leading zeros)
        well_name = normalize_well_name(well_name_raw)
        
        # Skip if no valid platform detected yet
        if not current_platform:
            continue

        # ── DATA EXTRACTION ───────────────────────────────────────────────────
        try:
            liquid_rate = pd.to_numeric(row.iloc[3], errors='coerce')
            oil_rate    = pd.to_numeric(row.iloc[4], errors='coerce')
            prod_loss   = pd.to_numeric(row.iloc[5], errors='coerce')
            well_status = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else None
            remarks     = str(row.iloc[7]).strip() if pd.notna(row.iloc[7]) else None

            # Clean up 'nan' strings
            if well_status in ['nan', 'None', '-']: well_status = None
            if remarks in ['nan', 'None', '-']:     remarks = None

            # ── DATABASE INSERT ───────────────────────────────────────────────
            # INSERT OR IGNORE means if same date+well already exists, skip it
            # This prevents duplicates when re-running ingestion
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
    """
    Process water injection summary sheet.
    
    Structure:
    - Column A (index 0): Platform name
    - Column B (index 1): Header pressure (KSC) — platform level
    - Column C (index 2): Well name
    - Column D (index 3): Choke size
    - Column E (index 4): ITHP
    - Column F (index 5): Injecting/Non-Injecting status
    - Column G (index 6): Flow rate (sm3/hr)
    - Column H (index 7): Flow rate (bbl/hr)
    - Column I (index 8): Injecting hours
    - Column J (index 9): Cumulative flow (bbl)
    - Column K (index 10): Planned WI (bpd)
    """
    inserted = 0
    skipped = 0
    current_platform = None
    current_header_pressure = None

    for idx, row in df_sheet.iterrows():
        
        # Platform detection — same logic as oil production
        platform_val = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        if platform_val in VALID_PLATFORMS:
            current_platform = platform_val
            # Header pressure is at platform level, in column B
            hp = pd.to_numeric(row.iloc[1], errors='coerce')
            if pd.notna(hp):
                current_header_pressure = float(hp)

        # Well name validation
        well_name_raw = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
        if not is_valid_well_name(well_name_raw):
            continue
        
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
    """
    Process historical water injection base sheet.
    
    This sheet has years of historical data in a clean tabular format:
    - Column A (index 0): Date
    - Column B (index 1): Well name
    - Column C (index 2): Choke size
    - Column D (index 3): ITHP
    - Column E (index 4): Status
    - Column F (index 5): Flow rate (sm3/hr)
    - Column G (index 6): Flow rate (bbl/hr) 
    - Column H (index 7): Injecting hours
    - Column I (index 8): Cumulative flow (bbl)
    - Column J (index 9): Planned WI (bpd)
    
    Platform is derived from well name since it's not a separate column here
    """
    inserted = 0
    skipped = 0

    for idx, row in df.iterrows():
        
        # Date parsing — skip rows with no date
        date_val = row.iloc[0]
        if pd.isna(date_val):
            continue

        try:
            date = pd.to_datetime(date_val, dayfirst=True, errors='coerce')
            if pd.isna(date):
                continue
            date_str = date.strftime('%Y-%m-%d')

            # Well name
            well_name_raw = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None
            if not is_valid_well_name(well_name_raw):
                continue
            
            well_name = normalize_well_name(well_name_raw)

            # Derive platform from well name
            # Well name like R_9A#3 → platform R-9A
            # We look for the platform pattern in the well name
            platform = None
            for p in VALID_PLATFORMS:
                # Convert platform format R-9A to match well name format R_9A or R9A
                p_clean = p.replace('-', '').replace('_', '').upper()
                w_clean = well_name.replace('_', '').replace('-', '').upper()
                if w_clean.startswith(p_clean[:3]):
                    platform = p
                    break

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
    """
    Main function — finds production file, reads all sheets,
    routes each sheet to correct processing function
    """
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
    total_skipped = 0

    for sheet in xl.sheet_names:
        sheet_lower = sheet.lower().strip()
        
        # Read sheet without header — we handle headers manually
        df = pd.read_excel(filepath, sheet_name=sheet, header=None)

        if 'overall' in sheet_lower or 'oil' in sheet_lower or 'production' in sheet_lower:
            if 'water' not in sheet_lower and 'injection' not in sheet_lower:
                print(f"\n   Processing oil production sheet: '{sheet}'")
                ins, skip = ingest_oil_production(df, date, conn)
                print(f"   ✅ Inserted: {ins}, Skipped: {skip}")
                total_inserted += ins
                total_skipped += skip

        elif 'water' in sheet_lower or 'injection' in sheet_lower or 'wi' in sheet_lower:
            if 'base' in sheet_lower:
                print(f"\n   Processing water injection BASE sheet: '{sheet}'")
                ins, skip = ingest_water_injection_base(df, conn)
            else:
                print(f"\n   Processing water injection sheet: '{sheet}'")
                ins, skip = ingest_water_injection(df, date, conn)
            print(f"   ✅ Inserted: {ins}, Skipped: {skip}")
            total_inserted += ins
            total_skipped += skip
        
        else:
            print(f"\n   ⏭️  Skipping sheet: '{sheet}' (not recognized)")

    conn.commit()
    conn.close()

    print(f"\n✅ Production ingestion complete!")
    print(f"   Total records inserted: {total_inserted}")
    print(f"   Total records skipped:  {total_skipped}")

if __name__ == "__main__":
    ingest_production()