import pandas as pd
import sqlite3
import os
import re
from database import get_connection

RAW_FOLDER = os.path.join(os.path.dirname(__file__), "data", "raw")

def find_avalon_file():
    for f in os.listdir(RAW_FOLDER):
        if "avalon" in f.lower() or "Avalon" in f:
            return os.path.join(RAW_FOLDER, f)
    return None

def clean_well_name(raw_name):
    """Extract well name from parameter string like 'BGOIL well2Motor Temperature 1'"""
    match = re.search(r'well(\w+)', raw_name, re.IGNORECASE)
    if match:
        return match.group(0).strip()
    return raw_name.strip()

def parse_parameter_name(raw_name):
    """Extract parameter type from full parameter string"""
    raw_name = str(raw_name).strip()
    param_map = {
        "Motor Temperature 1": "motor_temp_1_c",
        "VFD Output Frequency": "vfd_output_frequency_hz",
        "Pump Discharge Pressure": "pump_discharge_pressure_psi",
        "Pump Intake Pressure": "pump_intake_pressure_psi",
        "Motor load": "motor_load_pct",
        "Motor Load": "motor_load_pct",
        "Motor Current (Average)": "motor_current_avg_amp",
        "Motor Current A": "motor_current_a_amp",
        "Motor Current B": "motor_current_b_amp",
        "Motor Current C": "motor_current_c_amp",
        "Pump Intake Temperature": "pump_intake_temp_c",
        "Vibration X": "vibration_x",
        "Vibration Y": "vibration_y",
    }
    for key, col in param_map.items():
        if key.lower() in raw_name.lower():
            return col
    return None

def ingest_avalon():
    filepath = find_avalon_file()
    if not filepath:
        print("❌ No Avalon file found in data/raw folder!")
        return

    print(f"📂 Reading Avalon file: {os.path.basename(filepath)}")

    try:
        df = pd.read_csv(filepath) if filepath.endswith('.csv') else pd.read_excel(filepath)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return

    print(f"   Rows found: {len(df)}")
    print(f"   Columns: {list(df.columns)}")

    # Rename columns for consistency
    df.columns = ['parameter', 'timestamp', 'value', 'quality', 'quality_text', 'uom'] + list(df.columns[6:])

    # Drop header row if it got included
    df = df[df['parameter'] != 'parameters'].copy()
    df = df[df['timestamp'] != 'Time (Asia/Calcutta)'].copy()

    # Parse timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    # Extract well name and parameter from parameter column
    df['well_name'] = df['parameter'].apply(clean_well_name)
    df['param_col'] = df['parameter'].apply(parse_parameter_name)

    # Drop rows where parameter not recognized
    df = df.dropna(subset=['param_col'])

    print(f"   Wells found: {df['well_name'].unique()}")
    print(f"   Parameters found: {df['param_col'].unique()}")

    # Pivot — convert long format to wide format
    pivot_df = df.pivot_table(
        index=['timestamp', 'well_name'],
        columns='param_col',
        values='value',
        aggfunc='first'
    ).reset_index()

    # Flatten column names
    pivot_df.columns.name = None

    # Add quality flag
    quality_df = df.groupby(['timestamp', 'well_name'])['quality_text'].first().reset_index()
    pivot_df = pivot_df.merge(quality_df, on=['timestamp', 'well_name'], how='left')
    pivot_df.rename(columns={'quality_text': 'quality_flag'}, inplace=True)

    # Flag suspicious zero values on pressure/temp
    suspect_cols = ['pump_discharge_pressure_psi', 'pump_intake_pressure_psi',
                    'motor_temp_1_c', 'pump_intake_temp_c']
    for col in suspect_cols:
        if col in pivot_df.columns:
            zero_count = (pivot_df[col] == 0).sum()
            if zero_count > 0:
                print(f"   ⚠️  {zero_count} zero values detected in {col} — possible dead sensor")

    # Convert timestamp to string for SQLite
    pivot_df['timestamp'] = pivot_df['timestamp'].astype(str)

    # Load to database
    conn = get_connection()
    inserted = 0
    skipped = 0

    for _, row in pivot_df.iterrows():
        try:
            conn.execute("""
                INSERT OR IGNORE INTO esp_parameters 
                (timestamp, well_name, motor_temp_1_c, vfd_output_frequency_hz,
                pump_discharge_pressure_psi, pump_intake_pressure_psi,
                motor_load_pct, motor_current_avg_amp, motor_current_a_amp,
                motor_current_b_amp, motor_current_c_amp, pump_intake_temp_c,
                vibration_x, vibration_y, quality_flag)
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
                row.get('quality_flag')
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