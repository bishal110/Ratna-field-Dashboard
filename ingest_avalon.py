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

    Real Avalon format: ONGC.NH.Ratna Field.R7A.R7A1.Motor Temperature 1
    Structure: [company].[region].[field].[platform].[well].[parameter]
    Well name is always the 5th element (index 4) when split by '.'

    Sample/masked format fallback: BGOIL well2Motor Temperature 1
    Extracts just the well identifier without parameter suffix
    """
    try:
        parts = str(raw_name).strip().split('.')
        if len(parts) >= 6:
            return parts[4].strip()

        # Fallback for sample/masked files
        match = re.search(r'\bwell(\w+)', raw_name, re.IGNORECASE)
        if match:
            raw_id = match.group(0)
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

    Real format: ONGC.NH.Ratna Field.R7A.R7A1.Motor Temperature 1
    Parameter = everything after 5th dot (6th part onwards)

    Maps parameter string to database column name.
    Add new parameters to param_map if Avalon adds more in future.
    """
    try:
        parts = str(raw_name).strip().split('.')
        if len(parts) >= 6:
            param_str = '.'.join(parts[5:]).strip()
        else:
            # Fallback for sample files
            param_str = re.sub(
                r'^[\w\s]+well\w+\s*', '',
                str(raw_name).strip(),
                flags=re.IGNORECASE
            ).strip()
            if not param_str:
                param_str = raw_name.strip()

    except Exception:
        param_str = str(raw_name).strip()

    # ── PARAMETER MAP ─────────────────────────────────────────────────────────
    # Maps Avalon parameter names → database column names
    # Key = substring to match (case insensitive)
    # Value = database column name
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
        "Vibration Z":              "vibration_z",
    }

    for key, col in param_map.items():
        if key.lower() in param_str.lower():
            return col

    return None

def ingest_avalon():
    """
    Main ingestion function for Avalon ESP export files.

    Process:
    1. Read CSV or Excel file
    2. Standardize column names
    3. Extract well name and parameter from parameter column
    4. Pivot long format → wide format (one row per well per timestamp)
    5. Forward fill missing values within each well
    6. Flag dead sensors (zero values on pressure/temp)
    7. Insert into database
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
    # Avalon may use slightly different column names across exports
    # We standardize to consistent internal names
    df.columns = ['parameter', 'timestamp', 'value',
                  'quality', 'quality_text', 'uom'] + list(df.columns[6:])

    # Remove header rows that may have been included in data
    df = df[df['parameter'] != 'parameters'].copy()
    df = df[df['timestamp'] != 'Time (Asia/Calcutta)'].copy()

    # ── PARSE TIMESTAMP ───────────────────────────────────────────────────────
    # Avalon timestamps include timezone info — convert to UTC then remove tz
    # so SQLite can store as plain text
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    df = df.dropna(subset=['timestamp'])

    # ── PARSE VALUE ───────────────────────────────────────────────────────────
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    # ── EXTRACT WELL NAME AND PARAMETER ──────────────────────────────────────
    df['well_name'] = df['parameter'].apply(clean_well_name)
    df['param_col'] = df['parameter'].apply(parse_parameter_name)

    print(f"\n   Wells detected: {df['well_name'].unique()}")
    print(f"   Parameters detected: {df['param_col'].dropna().unique()}")

    # Show unrecognized parameters
    unrecognized = df[df['param_col'].isna()]['parameter'].unique()
    if len(unrecognized) > 0:
        print(f"\n   ⚠️  Unrecognized parameters (skipped): {unrecognized}")

    # Drop unrecognized parameters
    df = df.dropna(subset=['param_col'])

    # ── PIVOT LONG → WIDE ─────────────────────────────────────────────────────
    # Convert from one row per parameter per timestamp
    # to one row per well per timestamp with all parameters as columns
    #
    # Before pivot:
    #   well | timestamp | param_col      | value
    #   R7A1 | 05:30     | motor_temp_1_c | 107.8
    #   R7A1 | 05:30     | motor_load_pct | 57.1
    #
    # After pivot:
    #   well | timestamp | motor_temp_1_c | motor_load_pct | ...
    #   R7A1 | 05:30     | 107.8          | 57.1           | ...
    pivot_df = df.pivot_table(
        index=['timestamp', 'well_name'],
        columns='param_col',
        values='value',
        aggfunc='first'
    ).reset_index()

    pivot_df.columns.name = None

    # ── FORWARD FILL MISSING VALUES ───────────────────────────────────────────
    # Problem: Avalon records different parameters at different timestamps
    # This creates alternating None values after pivot:
    #   R7A1 | 05:30 | motor_temp=107.8 | vfd=None    | load=None
    #   R7A1 | 17:30 | motor_temp=None  | vfd=45.0    | load=57.1
    #
    # Solution: Forward fill within each well
    # Carries last known value forward to fill gaps
    # This is standard practice for sensor data at different frequencies
    param_cols = [
        'motor_temp_1_c', 'vfd_output_frequency_hz',
        'pump_discharge_pressure_psi', 'pump_intake_pressure_psi',
        'motor_load_pct', 'motor_current_avg_amp',
        'motor_current_a_amp', 'motor_current_b_amp', 'motor_current_c_amp',
        'pump_intake_temp_c', 'vibration_x', 'vibration_y', 'vibration_z'
    ]

    # Sort by well and timestamp before filling
    pivot_df = pivot_df.sort_values(['well_name', 'timestamp'])

    for col in param_cols:
        if col in pivot_df.columns:
            # ffill = forward fill: carry last valid value forward
            pivot_df[col] = pivot_df.groupby('well_name')[col].ffill()

    print(f"\n   After forward fill — sample check:")
    print(f"   Rows with motor_temp: {pivot_df['motor_temp_1_c'].notna().sum()}")
    if 'vfd_output_frequency_hz' in pivot_df.columns:
        print(f"   Rows with VFD freq:   {pivot_df['vfd_output_frequency_hz'].notna().sum()}")

    # ── ADD QUALITY FLAG ──────────────────────────────────────────────────────
    quality_df = df.groupby(
        ['timestamp', 'well_name']
    )['quality_text'].first().reset_index()
    pivot_df = pivot_df.merge(quality_df, on=['timestamp', 'well_name'], how='left')
    pivot_df.rename(columns={'quality_text': 'quality_flag'}, inplace=True)

    # ── DEAD SENSOR DETECTION ─────────────────────────────────────────────────
    # Zero values on pressure/temperature are suspicious
    # A running pump cannot have 0 psi discharge pressure
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

    # ── CONVERT TIMESTAMP FOR SQLITE ──────────────────────────────────────────
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