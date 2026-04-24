import pandas as pd
import sqlite3
import os
from datetime import datetime
from database import get_connection

RAW_FOLDER = os.path.join(os.path.dirname(__file__), "data", "raw")

PLATFORM_LIST = ['R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A']

def find_production_file():
    for f in os.listdir(RAW_FOLDER):
        if "production" in f.lower() or "oil" in f.lower():
            return os.path.join(RAW_FOLDER, f)
    return None

def extract_date_from_filename(filename):
    """Try to extract date from filename like 'Ratna Oil Production 18.04.2026 1800hrs'"""
    import re
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', filename)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"
    return datetime.today().strftime('%Y-%m-%d')

def ingest_oil_production(df_sheet, date, conn):
    """Process oil production sheet"""
    inserted = 0
    skipped = 0
    current_platform = None

    for idx, row in df_sheet.iterrows():
        # Detect platform from merged cell column
        platform_val = str(row.iloc[0]).strip()
        if any(p in platform_val for p in PLATFORM_LIST):
            current_platform = platform_val

        well_name = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
        if not well_name or well_name in ['nan', 'Well Name', 'Total']:
            continue
        if not any(c.isdigit() for c in well_name):
            continue

        try:
            liquid_rate = pd.to_numeric(row.iloc[3], errors='coerce')
            oil_rate = pd.to_numeric(row.iloc[4], errors='coerce')
            prod_loss = pd.to_numeric(row.iloc[5], errors='coerce')
            well_status = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else None
            remarks = str(row.iloc[7]).strip() if pd.notna(row.iloc[7]) else None

            if well_status in ['nan', 'None']:
                well_status = None
            if remarks in ['nan', 'None']:
                remarks = None

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
    """Process water injection sheet"""
    inserted = 0
    skipped = 0
    current_platform = None
    current_header_pressure = None

    for idx, row in df_sheet.iterrows():
        platform_val = str(row.iloc[0]).strip()
        if any(p in platform_val for p in PLATFORM_LIST):
            current_platform = platform_val
            hp = pd.to_numeric(row.iloc[1], errors='coerce')
            if pd.notna(hp):
                current_header_pressure = hp

        well_name = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
        if not well_name or well_name in ['nan', 'Well Name', 'Total']:
            continue
        if not any(c.isdigit() for c in well_name):
            continue

        try:
            choke = str(row.iloc[3]).strip() if pd.notna(row.iloc[3]) else None
            ithp = pd.to_numeric(row.iloc[4], errors='coerce')
            status = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else None
            flow_sm3 = pd.to_numeric(row.iloc[6], errors='coerce')
            flow_bph = pd.to_numeric(row.iloc[7], errors='coerce')
            inj_hours = pd.to_numeric(row.iloc[8], errors='coerce')
            cumulative = pd.to_numeric(row.iloc[9], errors='coerce')
            planned = pd.to_numeric(row.iloc[10], errors='coerce')

            if status in ['nan', 'None']:
                status = None

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
    total_skipped = 0

    for sheet in xl.sheet_names:
        sheet_lower = sheet.lower()
        df = pd.read_excel(filepath, sheet_name=sheet, header=None)

        if 'oil' in sheet_lower or 'overall' in sheet_lower or 'production' in sheet_lower:
            print(f"\n   Processing oil production sheet: '{sheet}'")
            ins, skip = ingest_oil_production(df, date, conn)
            print(f"   ✅ Inserted: {ins}, Skipped: {skip}")
            total_inserted += ins
            total_skipped += skip

        elif 'water' in sheet_lower or 'injection' in sheet_lower or 'wi' in sheet_lower:
            if 'base' in sheet_lower:
                print(f"\n   Processing water injection base sheet: '{sheet}'")
                ins, skip = ingest_water_injection_base(df, conn)
            else:
                print(f"\n   Processing water injection sheet: '{sheet}'")
                ins, skip = ingest_water_injection(df, date, conn)
            print(f"   ✅ Inserted: {ins}, Skipped: {skip}")
            total_inserted += ins
            total_skipped += skip

    conn.commit()
    conn.close()

    print(f"\n✅ Production ingestion complete!")
    print(f"   Total records inserted: {total_inserted}")
    print(f"   Total records skipped: {total_skipped}")

def ingest_water_injection_base(df, conn):
    """Process historical water injection base sheet"""
    inserted = 0
    skipped = 0

    for idx, row in df.iterrows():
        date_val = row.iloc[0]
        if pd.isna(date_val):
            continue

        try:
            date = pd.to_datetime(date_val, errors='coerce')
            if pd.isna(date):
                continue
            date_str = date.strftime('%Y-%m-%d')

            well_name = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None
            if not well_name or well_name in ['nan', 'Well Name']:
                continue

            choke = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
            ithp = pd.to_numeric(row.iloc[3], errors='coerce')
            status = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else None
            flow_sm3 = pd.to_numeric(row.iloc[5], errors='coerce')
            flow_bph = pd.to_numeric(row.iloc[6], errors='coerce')
            inj_hours = pd.to_numeric(row.iloc[7], errors='coerce')
            cumulative = pd.to_numeric(row.iloc[8], errors='coerce')
            planned = pd.to_numeric(row.iloc[9], errors='coerce')

            platform = None
            for p in PLATFORM_LIST:
                if p.replace('-', '').replace(' ', '').lower() in well_name.lower():
                    platform = p
                    break

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

if __name__ == "__main__":
    ingest_production()