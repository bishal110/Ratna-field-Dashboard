import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from database import get_connection

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — tweak here without touching logic below
# ══════════════════════════════════════════════════════════════════════════════

CONFIG = {
    # Root folder containing yearly subfolders
    # Can be absolute path or relative to project
    # Example: 'C:/Users/bisha/Production Data'
    # For now points to data/raw for single file testing
    'production_root': os.path.join(
        os.path.dirname(__file__), "data", "raw"),

    # Only ingest files matching these time patterns (0600hrs files)
    # Case insensitive — add more variants if needed
    '0600_patterns': [
        r'0600\s*hrs',
        r'0600\s*hours',
        r'06:00\s*hrs',
        r'06:00\s*hours',
        r'600\s*hrs',
        r'600\s*hours',
        r'\b0600\b',
        r'\b06hrs\b',
        r'morning',
    ],

    # Skip files matching these patterns (1800hrs files)
    '1800_patterns': [
        r'1800\s*hrs',
        r'1800\s*hours',
        r'18:00\s*hrs',
        r'18:00\s*hours',
        r'1800',
        r'evening',
        r'afternoon',
    ],

    # Valid platforms — reject anything else
    'valid_platforms': [
        'R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A'
    ],

    # Month folder name patterns — maps to month number
    # Case insensitive matching
    'month_patterns': {
        1:  ['jan', 'january'],
        2:  ['feb', 'february'],
        3:  ['mar', 'march'],
        4:  ['apr', 'april'],
        5:  ['may'],
        6:  ['jun', 'june'],
        7:  ['jul', 'july'],
        8:  ['aug', 'august'],
        9:  ['sep', 'sept', 'september'],
        10: ['oct', 'october'],
        11: ['nov', 'november'],
        12: ['dec', 'december'],
    },
}

# ══════════════════════════════════════════════════════════════════════════════
# FILE DISCOVERY
# ══════════════════════════════════════════════════════════════════════════════

def is_0600_file(filename):
    """
    Check if filename represents a 0600hrs (full day) file.
    Case insensitive. Handles all variants:
    0600hrs, 600hrs, 0600HOURS, 06:00hrs, morning etc.
    """
    fname = filename.lower()
    for pattern in CONFIG['0600_patterns']:
        if re.search(pattern, fname, re.IGNORECASE):
            return True
    return False

def is_1800_file(filename):
    """
    Check if filename represents a 1800hrs (half day) file.
    These should be skipped.
    """
    fname = filename.lower()
    for pattern in CONFIG['1800_patterns']:
        if re.search(pattern, fname, re.IGNORECASE):
            return True
    return False

def detect_month_from_folder(folder_name):
    """
    Detect month number from folder name.
    Handles: 1.Jan, 1.January, 1.jan, 01.January, Jan, January etc.
    Returns month number (1-12) or None if not detected.
    """
    folder_lower = folder_name.lower()

    for month_num, patterns in CONFIG['month_patterns'].items():
        for pattern in patterns:
            if pattern in folder_lower:
                return month_num
    return None

def extract_date_from_filename(filename):
    """
    Dynamically extract date from filename.
    Handles multiple formats:
    - DD.MM.YYYY  (most common)
    - DD-MM-YYYY
    - YYYY-MM-DD
    - DD/MM/YYYY
    - D.M.YYYY
    Returns date string as YYYY-MM-DD or None if not found.
    """
    # Try DD.MM.YYYY or DD-MM-YYYY or DD/MM/YYYY
    match = re.search(
        r'(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})',
        filename
    )
    if match:
        part1, part2, part3 = match.groups()
        # part3 is always year (4 digits)
        # Determine if part1 is day or month
        # DD.MM.YYYY → day first
        day   = int(part1)
        month = int(part2)
        year  = int(part3)

        if 1 <= day <= 31 and 1 <= month <= 12:
            try:
                date = datetime(year, month, day)
                return date.strftime('%Y-%m-%d')
            except ValueError:
                pass

    # Try YYYY-MM-DD
    match = re.search(r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})', filename)
    if match:
        year, month, day = match.groups()
        try:
            date = datetime(int(year), int(month), int(day))
            return date.strftime('%Y-%m-%d')
        except ValueError:
            pass

    return None

def discover_production_files(root_folder):
    """
    Walk through the entire folder structure and find all valid
    0600hrs production files.

    Structure handled:
    root/
      YYYY/           ← year folder
        N.Month/      ← month folder (1.Jan, 2.February etc)
          *.xlsx      ← daily files

    Also handles flat structure (files directly in root)
    for backward compatibility with current single-file setup.

    Returns list of dicts:
    {
      'filepath': full path to file,
      'filename': just the filename,
      'date':     extracted date string YYYY-MM-DD,
      'year':     year int,
      'month':    month int,
    }
    """
    discovered = []
    skipped    = []

    # Walk entire directory tree
    for dirpath, dirnames, filenames in os.walk(root_folder):

        # Sort dirnames so years and months process in order
        dirnames.sort()

        for filename in sorted(filenames):

            # Only process Excel files
            if not filename.endswith(('.xlsx', '.xls')):
                continue

            # Skip temp files Excel creates
            if filename.startswith('~$'):
                continue

            filepath = os.path.join(dirpath, filename)

            # Check if it's a 1800hrs file → skip
            if is_1800_file(filename):
                skipped.append(filename)
                continue

            # Check if it's a 0600hrs file → process
            # Also process files with no time pattern
            # (older files may not have time in name)
            is_valid = is_0600_file(filename)

            # If no clear time pattern in filename,
            # check if there's a matching 1800 file in same folder
            # If yes → this might be the 0600 file (process it)
            # If no → process it (single file per day, no time suffix)
            if not is_valid and not is_1800_file(filename):
                # No time indicator — check if production file
                if any(keyword in filename.lower() for keyword in
                       ['production', 'oil', 'ratna']):
                    is_valid = True

            if not is_valid:
                continue

            # Extract date from filename
            date_str = extract_date_from_filename(filename)

            # If date not in filename, try to get from folder structure
            if not date_str:
                # Try to get year from parent folder name
                parts = dirpath.replace('\\', '/').split('/')
                year  = None
                month = None

                for part in parts:
                    if re.match(r'^\d{4}$', part):
                        year = int(part)
                    month_detected = detect_month_from_folder(part)
                    if month_detected:
                        month = month_detected

                if year:
                    date_str = f"{year}-{month or 1:02d}-01"

            if not date_str:
                # Can't determine date — skip
                continue

            # Parse year and month from date
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                year     = date_obj.year
                month    = date_obj.month
            except:
                continue

            discovered.append({
                'filepath': filepath,
                'filename': filename,
                'date':     date_str,
                'year':     year,
                'month':    month,
            })

    # Sort by date
    discovered.sort(key=lambda x: x['date'])

    print(f"\n   📁 File Discovery Summary:")
    print(f"      Found:   {len(discovered)} valid 0600hrs files")
    print(f"      Skipped: {len(skipped)} 1800hrs files")
    if discovered:
        print(f"      Date range: {discovered[0]['date']} "
              f"to {discovered[-1]['date']}")

    return discovered

# ══════════════════════════════════════════════════════════════════════════════
# WELL NAME UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def normalize_well_name(well_name):
    """
    Remove leading zeros from single digit platform numbers.
    R07A#4H → R7A#4H
    R07A#5  → R7A#5
    R10A#01 → R10A#01 (unchanged)
    """
    if not well_name:
        return well_name
    return re.sub(r'R0(\d)A', r'R\1A', well_name)

def is_valid_well_name(well_name):
    """
    Valid well names must:
    1. Contain '#' character
    2. Not be purely numeric (serial numbers)
    3. Not be empty/NaN/Total/Well Name
    """
    if not well_name or well_name in [
            'nan', 'None', 'Well Name', 'Total', '']:
        return False
    try:
        float(well_name)
        return False  # Pure number = serial number
    except ValueError:
        pass
    if '#' not in well_name:
        return False
    return True

def derive_platform_from_well_name(well_name):
    """
    Derive platform from well name.
    More reliable than merged cell detection in Excel.
    R7A#xx  → R-7A
    R10A#xx → R-10A etc.
    """
    if not well_name:
        return None
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

# ══════════════════════════════════════════════════════════════════════════════
# SHEET PROCESSORS
# ══════════════════════════════════════════════════════════════════════════════

def ingest_oil_production(df_sheet, date, conn):
    """
    Process oil production sheet.

    Columns:
    A(0): Platform (merged cells)
    B(1): Serial number — IGNORE
    C(2): Well name
    D(3): PVT Comp. Total Liquid Rate (bbl/d)
    E(4): PVT Comp. Oil Rate (bbl/d)
    F(5): Total Production Loss (bbl)
    G(6): Well Flowing/Non-Flowing status
    H(7): Remarks

    Stops at Total row — ignores everything below
    """
    inserted = 0
    skipped  = 0
    current_platform = None

    for idx, row in df_sheet.iterrows():

        # Stop at Total row
        col_b = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''
        col_c = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
        if 'Total' in col_b or 'Total' in col_c:
            break

        # Well name extraction
        well_name_raw = str(row.iloc[2]).strip() if pd.notna(
            row.iloc[2]) else None

        if not is_valid_well_name(well_name_raw):
            continue

        # Platform detection — column A first, well name second
        platform_val = str(row.iloc[0]).strip() if pd.notna(
            row.iloc[0]) else ''
        if platform_val in CONFIG['valid_platforms']:
            current_platform = platform_val

        # Well name always overrides (handles merged cell misalignment)
        derived = derive_platform_from_well_name(well_name_raw)
        if derived:
            current_platform = derived

        if not current_platform:
            continue

        well_name = normalize_well_name(well_name_raw)

        try:
            liquid_rate = pd.to_numeric(row.iloc[3], errors='coerce')
            oil_rate    = pd.to_numeric(row.iloc[4], errors='coerce')
            prod_loss   = pd.to_numeric(row.iloc[5], errors='coerce')
            well_status = str(row.iloc[6]).strip() if pd.notna(
                row.iloc[6]) else None
            remarks     = str(row.iloc[7]).strip() if pd.notna(
                row.iloc[7]) else None

            if well_status in ['nan', 'None', '-']: well_status = None
            if remarks     in ['nan', 'None', '-']: remarks     = None

            conn.execute("""
                INSERT OR IGNORE INTO oil_production
                (date, platform, well_name, liquid_rate_bpd, oil_rate_bpd,
                production_loss_bbl, well_status, remarks)
                VALUES (?,?,?,?,?,?,?,?)
            """, (date, current_platform, well_name, liquid_rate,
                  oil_rate, prod_loss, well_status, remarks))
            inserted += 1

        except Exception:
            skipped += 1

    return inserted, skipped

def ingest_water_injection(df_sheet, date, conn):
    """
    Process water injection summary sheet.

    Columns:
    A(0):  Platform
    B(1):  Header pressure (KSC)
    C(2):  Well name
    D(3):  Choke size
    E(4):  ITHP
    F(5):  Status
    G(6):  Flow rate (sm3/hr)
    H(7):  Flow rate (bbl/hr)
    I(8):  Injecting hours
    J(9):  Cumulative flow (bbl)
    K(10): Planned WI (bpd)
    """
    inserted = 0
    skipped  = 0
    current_platform       = None
    current_header_pressure = None

    for idx, row in df_sheet.iterrows():

        platform_val = str(row.iloc[0]).strip() if pd.notna(
            row.iloc[0]) else ''
        if platform_val in CONFIG['valid_platforms']:
            current_platform = platform_val
            hp = pd.to_numeric(row.iloc[1], errors='coerce')
            if pd.notna(hp):
                current_header_pressure = float(hp)

        well_name_raw = str(row.iloc[2]).strip() if pd.notna(
            row.iloc[2]) else None
        if not is_valid_well_name(well_name_raw):
            continue

        derived = derive_platform_from_well_name(well_name_raw)
        if derived:
            current_platform = derived

        well_name = normalize_well_name(well_name_raw)

        if not current_platform:
            continue

        try:
            choke      = str(row.iloc[3]).strip() if pd.notna(
                row.iloc[3]) else None
            ithp       = pd.to_numeric(row.iloc[4], errors='coerce')
            status     = str(row.iloc[5]).strip() if pd.notna(
                row.iloc[5]) else None
            flow_sm3   = pd.to_numeric(row.iloc[6], errors='coerce')
            flow_bph   = pd.to_numeric(row.iloc[7], errors='coerce')
            inj_hours  = pd.to_numeric(row.iloc[8], errors='coerce')
            cumulative = pd.to_numeric(row.iloc[9], errors='coerce')
            planned    = pd.to_numeric(row.iloc[10], errors='coerce')

            if choke  in ['nan', 'None']: choke  = None
            if status in ['nan', 'None']: status = None

            conn.execute("""
                INSERT OR IGNORE INTO water_injection
                (date, platform, well_name, header_pressure_ksc,
                choke_size, ithp, status, flow_rate_sm3hr,
                flow_rate_bpd, injecting_hours,
                cumulative_flow_bbl, planned_wi_bpd)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (date, current_platform, well_name,
                  current_header_pressure, choke, ithp, status,
                  flow_sm3, flow_bph, inj_hours, cumulative, planned))
            inserted += 1

        except Exception:
            skipped += 1

    return inserted, skipped

def ingest_water_injection_base(df, conn):
    """
    Process historical water injection base sheet.

    Columns:
    A(0): Date
    B(1): Well name
    C(2): Choke size
    D(3): ITHP
    E(4): Status
    F(5): Flow rate (sm3/hr)
    G(6): Flow rate (bbl/hr)
    H(7): Injecting hours
    I(8): Cumulative flow (bbl)
    J(9): Planned WI (bpd)
    """
    inserted = 0
    skipped  = 0

    for idx, row in df.iterrows():
        date_val = row.iloc[0]
        if pd.isna(date_val):
            continue

        try:
            date = pd.to_datetime(date_val, dayfirst=True, errors='coerce')
            if pd.isna(date):
                continue
            date_str = date.strftime('%Y-%m-%d')

            well_name_raw = str(row.iloc[1]).strip() if pd.notna(
                row.iloc[1]) else None
            if not is_valid_well_name(well_name_raw):
                continue

            well_name = normalize_well_name(well_name_raw)
            platform  = derive_platform_from_well_name(well_name_raw)

            choke      = str(row.iloc[2]).strip() if pd.notna(
                row.iloc[2]) else None
            ithp       = pd.to_numeric(row.iloc[3], errors='coerce')
            status     = str(row.iloc[4]).strip() if pd.notna(
                row.iloc[4]) else None
            flow_sm3   = pd.to_numeric(row.iloc[5], errors='coerce')
            flow_bph   = pd.to_numeric(row.iloc[6], errors='coerce')
            inj_hours  = pd.to_numeric(row.iloc[7], errors='coerce')
            cumulative = pd.to_numeric(row.iloc[8], errors='coerce')
            planned    = pd.to_numeric(row.iloc[9], errors='coerce')

            if choke  in ['nan', 'None']: choke  = None
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

        except Exception:
            skipped += 1

    return inserted, skipped

def process_single_file(file_info, conn):
    """
    Process one production Excel file.
    Routes each sheet to correct processor.
    Returns total inserted and skipped counts.
    """
    filepath = file_info['filepath']
    filename = file_info['filename']
    date     = file_info['date']

    total_inserted = 0
    total_skipped  = 0

    try:
        xl = pd.ExcelFile(filepath)
    except Exception as e:
        print(f"      ❌ Cannot open file: {e}")
        return 0, 0

    for sheet in xl.sheet_names:
        sheet_lower = sheet.lower().strip()
        df = pd.read_excel(filepath, sheet_name=sheet, header=None)

        # Oil production sheet
        if ('overall' in sheet_lower or
                'oil' in sheet_lower or
                'production' in sheet_lower):
            if 'water' not in sheet_lower and 'injection' not in sheet_lower:
                ins, skip = ingest_oil_production(df, date, conn)
                total_inserted += ins
                total_skipped  += skip

        # Water injection sheets
        elif ('water' in sheet_lower or
              'injection' in sheet_lower or
              'wi' in sheet_lower):
            if 'base' in sheet_lower:
                ins, skip = ingest_water_injection_base(df, conn)
            else:
                ins, skip = ingest_water_injection(df, date, conn)
            total_inserted += ins
            total_skipped  += skip

    return total_inserted, total_skipped

# ══════════════════════════════════════════════════════════════════════════════
# MAIN INGESTION FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

def ingest_production():
    """
    Main production ingestion function.

    Steps:
    1. Discover all valid 0600hrs files in folder structure
    2. Sort by date (oldest first)
    3. Process each file
    4. Skip duplicates automatically (INSERT OR IGNORE)
    5. Report summary
    """
    root = CONFIG['production_root']
    print(f"\n📂 Scanning: {root}")

    # Discover all valid files
    files = discover_production_files(root)

    if not files:
        print("❌ No valid production files found!")
        print(f"   Check that files are in: {root}")
        print(f"   And filenames contain 0600hrs or similar")
        return

    print(f"\n🔄 Processing {len(files)} files...\n")

    conn           = get_connection()
    total_inserted = 0
    total_skipped  = 0
    files_ok       = 0
    files_error    = 0

    # Group by year for cleaner output
    current_year = None

    for file_info in files:

        if file_info['year'] != current_year:
            current_year = file_info['year']
            print(f"\n  📅 Year: {current_year}")
            print(f"  {'─'*55}")

        ins, skip = process_single_file(file_info, conn)

        if ins > 0 or skip >= 0:
            status = "✅" if ins > 0 else "⚠️ "
            print(f"  {status} {file_info['date']} | "
                  f"{file_info['filename'][:45]:<45} | "
                  f"Inserted: {ins:>4} | Skipped: {skip:>4}")
            total_inserted += ins
            total_skipped  += skip
            files_ok       += 1
        else:
            files_error += 1

    conn.commit()
    conn.close()

    # Final summary
    print(f"\n{'═'*65}")
    print(f"✅ PRODUCTION INGESTION COMPLETE")
    print(f"   Files processed:       {files_ok}")
    print(f"   Files with errors:     {files_error}")
    print(f"   Total records inserted:{total_inserted:>8,}")
    print(f"   Total records skipped: {total_skipped:>8,}")
    print(f"{'═'*65}\n")

    # Show date range now in database
    conn = get_connection()
    result = conn.execute("""
        SELECT MIN(date), MAX(date), COUNT(DISTINCT date)
        FROM oil_production
    """).fetchone()
    conn.close()

    if result and result[0]:
        print(f"📊 Database now contains:")
        print(f"   Date range: {result[0]} to {result[1]}")
        print(f"   Total days: {result[2]}")

if __name__ == "__main__":
    ingest_production()