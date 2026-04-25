import pandas as pd
import os
import re
from database import get_connection

RAW_FOLDER = os.path.join(os.path.dirname(__file__), "data", "raw")

def find_avalon_file():
    """Find Avalon export file in data/raw folder"""
    for f in os.listdir(RAW_FOLDER):
        if "avalon" in f.lower():
            return os.path.join(RAW_FOLDER, f)
    return None

def clean_well_name(raw_name):
    """
    Extract well name from Avalon parameter string.

    Avalon format: ONGC.NH.Ratna Field.R7A.R7A1.Motor Temperature 1
    Structure:     [company].[region].[field].[platform].[well].[parameter]

    We split by '.' and take index 4 (5th element) = well name

    Examples:
    ONGC.NH.Ratna Field.R7A.R7A1.Motor Temperature 1   → R7A1
    ONGC.NH.Ratna Field.R10A.R10A2.VFD Output Frequency → R10A2
    ONGC.NH.Ratna Field.R12A.R12A02Z.Pump Intake Pressure → R12A02Z

    For privacy-masked files (like sample file with 'well2' naming):
    Falls back to extracting after 'well' keyword
    """
    try:
        parts = str(raw_name).strip().split('.')

        # Real Avalon format — 6+ parts separated by dots
        if len(parts) >= 6:
            return parts[4].strip()  # Index 4 = well name

        # Fallback for sample/masked files
        # Handles format like 'BGOIL well2Motor Temperature 1'
        match = re.search(r'\bwell(\w+)', raw_name, re.IGNORECASE)
        if match:
            # Extract just the well identifier, not the parameter part
            # 'well2Motor' → we want 'well2', not 'well2Motor'
            # Split on capital letters to find where well ID ends
            raw_id = match.group(0)  # e.g. 'well2Motor'
            # Take only up to first capital after the number
            id_match = re.match(r'(well\d+)', raw_id, re.IGNORECASE)
            if id_match:
                return id_match.group(1)
            return raw_id

    except Exception:
        pass

    return str(raw_name).strip()

def parse_parameter_name(raw_name):
    """
    Extract parameter type from Avalon parameter string.

    Real Avalon format: ONGC.NH.Ratna Field.R7A.R7A1.Motor Temperature 1
    Parameter is everything after the 5th dot (6th part onwards)

    Sample/masked format: BGOIL well2Motor Temperature 1
    Parameter extracted by removing asset and well prefix

    Maps parameter string to database column name.
    """
    try:
        parts = str(raw_name).strip().split('.')

        # Real Avalon format — parameter is 6th part onwards
        if len(parts) >= 6:
            param_str = '.'.join(parts[5:]).strip()
        else:
            # Fallback for sample files
            # Remove asset prefix (BGOIL, RATNA etc) and well identifier
            # 'BGOIL well2Motor Temperature 1' → 'Motor Temperature 1'
            param_str = re.sub(
                r'^[\w\s]+well\w+\s*',  # Remove everything up to and including well ID
                '',
                str(raw_name).strip(),
                flags=re.IGNORECASE
            ).strip()
            if not param_str:
                param_str = raw_name.strip()

    except Exception:
        param_str = str(raw_name).strip()

    # ── PARAMETER MAP ─────────────────────────────────────────────────────────
    # Maps Avalon parameter names to database column names
    # Add new parameters here if Avalon adds more in future
    param_map = {
        "Motor Temperature 1":      "motor_temp_1_c",
        "Motor Temperature":        "motor_temp_1_c",
        "VFD Output Frequency":     "vfd_output_frequency_hz",
        "Pump Discharge Pressure":  "pump_discharge_pressure_psi",
        "Pump Intake Pressure":     "pump_intake_pressure_psi",
        "Motor load":               "motor_load_pct",
        "Motor Load":               "motor_load_pct",
        "Motor Current (Average)":  "motor_current_avg_amp",
        "Motor Current A":          "motor_current_a_amp",
        "Motor Current B":          "motor_current_b_amp",
        "Motor Current C":          "motor_current_c_amp",
        "Pump Intake Temperature":  "pump_intake_temp_c",
        "Vibration X":              "vibration_x",
        "Vibration Y":              "vibration_y",
    }

    for key, col in param_map.items():
        if key.lower() in param_str.lower():
            return col

    return None

def ingest_avalon():
    """
    Main function — reads Avalon ESP export and loads into database.

    Avalon exports data in LONG format:
    Each row = one parameter, one timestamp, one value
    
    We need to PIVOT this to WIDE format:
    Each row = one well, one timestamp, ALL parameters as columns

    Steps:
    1. Read file (CSV or Excel)
    2. Extract well name and parameter from parameter column
    3. Pivot long → wide
    4. Flag dead sensors (zero values on pressure/temp)
    5. Insert into database
    """
    filepath = find_avalon_file()
    if not filepath:
        print("❌ No Avalon file found in data/raw folder!")
        return

    print(f"📂 Reading Avalon file: {os.path.basename(filepath)}")

    # ── READ FILE ─────────────────────────────────────────────────────────────
    try:
        if filepath.lower().endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return

    print(f"   Rows found: {len(df)}")
    print(f"   Columns found: {list(df.columns)}")

    # ── STANDARDIZE COLUMN NAMES ──────────────────────────────────────────────
    # Avalon exports may have slightly different column names
    # We standardize to: parameter, timestamp, value, quality, quality_text, uom
    df.columns = ['parameter', 'timestamp', 'value',
                  'quality', 'quality_text', 'uom'] + list(df.columns[6:])

    # Remove header rows that got included in data
    df = df[df['parameter'] != 'parameters'].copy()
    df = df[df['timestamp'] != 'Time (Asia/Calcutta)'].copy()

    # ── PARSE TIMESTAMP ───────────────────────────────────────────────────────
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)  # Remove timezone
    df = df.dropna(subset=['timestamp'])

    # ── PARSE VALUE ───────────────────────────────────────────────────────────
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    # ── EXTRACT WELL NAME AND PARAMETER ──────────────────────────────────────
    df['well_name']  = df['parameter'].apply(clean_well_name)
    df['param_col']  = df['parameter'].apply(parse_parameter_name)

    # Show what was detected
    print(f"\n   Wells detected: {df['well_name'].unique()}")
    print(f"   Parameters detected: {df['param_col'].dropna().unique()}")

    # Drop rows where parameter not recognized
    unrecognized = df[df['param_col'].isna()]['parameter'].unique()
    if len(unrecognized) > 0:
        print(f"\n   ⚠️  Unrecognized parameters (skipped): {unrecognized}")
    df = df.dropna(subset=['param_col'])

    # ── PIVOT LONG → WIDE ─────────────────────────────────────────────────────
    # Convert from:
    #   well | timestamp | motor_temp | value
    #   R7A1 | 2026-04-05| ...        | 118.7
    # To:
    #   well | timestamp | motor_temp | vfd_freq | intake_pressure | ...
    #   R7A1 | 2026-04-05| 118.7      | 44       | 983.5           | ...
    pivot_df = df.pivot_table(
        index=['timestamp', 'well_name'],
        columns='param_col',
        values='value',
        aggfunc='first'  # If duplicate, take first value
    ).reset_index()

    # Clean up column naming after pivot
    pivot_df.columns.name = None

    # ── ADD QUALITY FLAG ──────────────────────────────────────────────────────
    quality_df = df.groupby(
        ['timestamp', 'well_name']
    )['quality_text'].first().reset_index()
    pivot_df = pivot_df.merge(quality_df, on=['timestamp', 'well_name'], how='left')
    pivot_df.rename(columns={'quality_text': 'quality_flag'}, inplace=True)

    # ── DEAD SENSOR DETECTION ─────────────────────────────────────────────────
    # Zero values on pressure/temperature sensors are suspicious
    # A pump running cannot have 0 psi discharge pressure — sensor is dead
    suspect_cols = [
        'pump_discharge_pressure_psi',
        'pump_intake_pressure_psi',
        'motor_temp_1_c',
        'pump_intake_temp_c'
    ]
    print(f"\n   Dead sensor check:")
    for col in suspect_cols:
        if col in pivot_df.columns:
            zero_count = (pivot_df[col] == 0).sum()
            total = pivot_df[col].notna().sum()
            if zero_count > 0:
                print(f"   ⚠️  {col}: {zero_count}/{total} readings are zero — possible dead sensor")
            else:
                print(f"   ✅ {col}: No zero readings detected")

    # ── CONVERT TIMESTAMP TO STRING FOR SQLITE ────────────────────────────────
    pivot_df['timestamp'] = pivot_df['timestamp'].astype(str)

    # ── INSERT INTO DATABASE ──────────────────────────────────────────────────
    conn = get_connection()
    inserted = 0
    skipped  = 0

    for _, row in pivot_df.iterrows():
        try:
            conn.execute("""
                INSERT OR IGNORE INTO esp_parameters
                (timestamp, well_name,
                motor_temp_1_c, vfd_output_frequency_hz,
                pump_discharge_pressure_psi, pump_intake_pressure_psi,
                motor_load_pct, motor_current_avg_amp,
                motor_current_a_amp, motor_current_b_amp, motor_current_c_amp,
                pump_intake_temp_c, vibration_x, vibration_y,
                quality_flag)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                row.get('timestamp'),
                row.get('well_name'),
                row.get('motor_temp_1_c'),
                row.get('vfd_output_frequency_hz'),
                row.get('pump_discharge_pressure_psi'),
                row.get('pump_intake_pressure_psi'),
                row.get('motor_load_pct'),
                row.get('motor_current_avg_amp'),
                row.get('motor_current_a_amp'),
                row.get('motor_current_b_amp'),
                row.get('motor_current_c_amp'),
                row.get('pump_intake_temp_c'),
                row.get('vibration_x'),
                row.get('vibration_y'),
                row.get('quality_flag'),
            ))
            inserted += 1
        except Exception as e:
            skipped += 1

    conn.commit()
    conn.close()

    print(f"\n✅ Avalon ingestion complete!")
    print(f"   Records inserted: {inserted}")
    print(f"   Records skipped (duplicates): {skipped}")

if __name__ == "__main__":
    ingest_avalon()