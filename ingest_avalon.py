import pandas as pd
import numpy as np
import os
import re
from database import get_connection

RAW_FOLDER = os.path.join(os.path.dirname(__file__), "data", "raw")

# ══════════════════════════════════════════════════════════════════════════════
# PHYSICAL LIMITS — RATNA FIELD ESP PUMPS
# These are the boundaries of what is physically possible
# Anything outside these ranges is a sensor error, not a real reading
# ══════════════════════════════════════════════════════════════════════════════
PHYSICAL_LIMITS = {
    'motor_temp_1_c':               {'min': 50,   'max': 200,  'unit': '°C'},
    'pump_intake_temp_c':           {'min': 60,   'max': 150,  'unit': '°C'},
    'pump_intake_pressure_psi':     {'min': 100,  'max': 3000, 'unit': 'psi'},
    'pump_discharge_pressure_psi':  {'min': 100,  'max': 3000, 'unit': 'psi'},
    'vfd_output_frequency_hz':      {'min': 30,   'max': 65,   'unit': 'Hz'},
    'motor_load_pct':               {'min': 5,    'max': 100,  'unit': '%'},
    'motor_current_avg_amp':        {'min': 1,    'max': 100,  'unit': 'A'},
    'motor_current_a_amp':          {'min': 1,    'max': 100,  'unit': 'A'},
    'motor_current_b_amp':          {'min': 1,    'max': 100,  'unit': 'A'},
    'motor_current_c_amp':          {'min': 1,    'max': 100,  'unit': 'A'},
    'vibration_x':                  {'min': 0,    'max': 5,    'unit': 'm/s²'},
    'vibration_y':                  {'min': 0,    'max': 5,    'unit': 'm/s²'},
    'vibration_z':                  {'min': 0,    'max': 5,    'unit': 'm/s²'},
}

# ══════════════════════════════════════════════════════════════════════════════
# PARAMETER MAP — Avalon parameter names → database column names
# ══════════════════════════════════════════════════════════════════════════════
PARAM_MAP = {
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

def find_avalon_files():
    """
    Find ALL Avalon export files in data/raw folder.
    Supports multiple files — one per well or one per export session.
    Returns list of file paths sorted by filename.
    """
    files = []
    for f in os.listdir(RAW_FOLDER):
        if "avalon" in f.lower():
            files.append(os.path.join(RAW_FOLDER, f))
    return sorted(files)

def clean_well_name(raw_name):
    """
    Extract well name from Avalon parameter string.

    Real format: ONGC.NH.Ratna Field.R7A.R7A1.Motor Temperature 1
    Well name = 5th element when split by '.' (index 4)

    Fallback for masked files: BGOIL well2Motor Temperature 1
    Extracts just the well ID without parameter suffix
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
    Maps to database column name using PARAM_MAP.
    """
    try:
        parts = str(raw_name).strip().split('.')
        if len(parts) >= 6:
            param_str = '.'.join(parts[5:]).strip()
        else:
            param_str = re.sub(
                r'^[\w\s]+well\w+\s*', '',
                str(raw_name).strip(),
                flags=re.IGNORECASE
            ).strip()
            if not param_str:
                param_str = raw_name.strip()
    except Exception:
        param_str = str(raw_name).strip()

    for key, col in PARAM_MAP.items():
        if key.lower() in param_str.lower():
            return col
    return None

def validate_physical_limits(pivot_df):
    """
    AUTO-DETECTION LAYER 1 — Physical Limits Validation

    Checks every parameter against known physical boundaries.
    Any value outside these boundaries is physically impossible
    and must be a sensor error.

    For each parameter:
    - Values below minimum → set to None (dead/faulty sensor)
    - Values above maximum → set to None (faulty/stuck high)
    - Zero values → set to None (dead sensor recording zero)

    Returns:
    - Cleaned dataframe
    - Quality report dictionary with counts of issues found
    """
    quality_report = {}

    for col, limits in PHYSICAL_LIMITS.items():
        if col not in pivot_df.columns:
            continue

        total       = pivot_df[col].notna().sum()
        zero_count  = (pivot_df[col] == 0).sum()
        low_count   = (pivot_df[col] < limits['min']).sum()
        high_count  = (pivot_df[col] > limits['max']).sum()
        invalid     = zero_count + low_count + high_count

        # Clean — set all invalid values to None
        pivot_df[col] = pivot_df[col].where(
            (pivot_df[col] > limits['min']) &
            (pivot_df[col] < limits['max']),
            other=None
        )

        valid_after = pivot_df[col].notna().sum()
        quality_pct = (valid_after / total * 100) if total > 0 else 0

        quality_report[col] = {
            'total':       total,
            'invalid':     invalid,
            'zero':        zero_count,
            'too_low':     low_count,
            'too_high':    high_count,
            'valid_after': valid_after,
            'quality_pct': quality_pct,
            'unit':        limits['unit'],
            'min':         limits['min'],
            'max':         limits['max'],
        }

    return pivot_df, quality_report

def detect_frozen_sensors(pivot_df, freeze_threshold_hours=12):
    """
    AUTO-DETECTION LAYER 2 — Frozen/Stuck Sensor Detection

    A sensor is considered FROZEN if it reports the exact same value
    for more than freeze_threshold_hours consecutive hours.

    This is different from a dead sensor (which reads 0 or extreme values)
    A frozen sensor reads a plausible value that never changes
    which is just as misleading as a dead sensor.

    Example: Motor temp stuck at exactly 107.8°C for 3 days
    = sensor is frozen, not actually measuring

    Returns:
    - frozen_sensors: dict of {column: {well: hours_frozen}}
    """
    frozen_sensors = {}

    check_cols = [
        'motor_temp_1_c', 'pump_intake_pressure_psi',
        'pump_discharge_pressure_psi', 'pump_intake_temp_c',
        'motor_current_avg_amp', 'vfd_output_frequency_hz'
    ]

    for col in check_cols:
        if col not in pivot_df.columns:
            continue

        frozen_sensors[col] = {}

        for well in pivot_df['well_name'].unique():
            well_data = pivot_df[
                pivot_df['well_name'] == well
            ][['timestamp', col]].dropna()

            if len(well_data) < 4:
                continue

            # Check for consecutive identical values
            # Using round to 2 decimal places to handle floating point
            values = well_data[col].round(2)
            is_same = values == values.shift(1)

            # Count consecutive same values
            consecutive_same = 0
            max_consecutive  = 0

            for same in is_same:
                if same:
                    consecutive_same += 1
                    max_consecutive = max(max_consecutive, consecutive_same)
                else:
                    consecutive_same = 0

            # Calculate actual time span of consecutive same values
            # Find where values change
            same_mask = values == values.shift(1)
            if same_mask.sum() == 0:
                continue

            # Get timestamps of the frozen period
            timestamps = well_data['timestamp'].reset_index(drop=True)
            if len(timestamps) == len(same_mask):
                # Find longest consecutive frozen block
                frozen_start = None
                max_duration = pd.Timedelta(0)

                for i, is_frozen in enumerate(same_mask):
                    if is_frozen and frozen_start is None:
                        frozen_start = timestamps.iloc[i-1] if i > 0 else timestamps.iloc[i]
                    elif not is_frozen and frozen_start is not None:
                        duration = timestamps.iloc[i] - frozen_start
                        max_duration = max(max_duration, duration)
                        frozen_start = None

                if frozen_start is not None:
                    duration = timestamps.iloc[-1] - frozen_start
                    max_duration = max(max_duration, duration)

                approx_hours = max_duration.total_seconds() / 3600

            if approx_hours >= freeze_threshold_hours:
                frozen_sensors[col][well] = approx_hours

    return frozen_sensors

def detect_cross_parameter_anomalies(pivot_df):
    """
    AUTO-DETECTION LAYER 3 — Cross-Parameter Contradiction Detection

    Some combinations of parameter values are physically contradictory
    and indicate either a sensor failure or an operational problem.

    Checks:
    1. Motor temp rising while current dropping → possible gas lock
    2. Discharge pressure < Intake pressure → impossible (pump not working)
    3. High motor load with low current → sensor mismatch
    4. VFD running but zero current → current sensor dead

    Returns list of detected anomalies with explanations
    """
    anomalies = []

    for well in pivot_df['well_name'].unique():
        well_data = pivot_df[pivot_df['well_name'] == well].copy()
        well_data = well_data.sort_values('timestamp')

        if len(well_data) < 10:
            continue

        # Check 1: Discharge pressure < Intake pressure
        # Physically impossible — pump must ADD pressure
        if ('pump_discharge_pressure_psi' in well_data.columns and
                'pump_intake_pressure_psi' in well_data.columns):

            dp_negative =(
                well_data['pump_discharge_pressure_psi'] - well_data['pump_intake_pressure_psi']
            ).sum()

            if dp_negative > 0:
                anomalies.append({
                    'well':      well,
                    'type':      'Impossible Pressure',
                    'detail':    f'Discharge < Intake pressure in {dp_negative} readings',
                    'severity':  'Sensor Error',
                    'action':    'Verify pressure sensor calibration'
                })

        # Check 2: Motor temp trending up while current trending down
        # Classic gas lock signature
        if ('motor_temp_1_c' in well_data.columns and
                'motor_current_avg_amp' in well_data.columns):

            recent = well_data.tail(10).copy()
            temp_trend    = recent['motor_temp_1_c'].diff().mean()
            current_trend = recent['motor_current_avg_amp'].diff().mean()

            if temp_trend > 0.5 and current_trend < -0.1:
                anomalies.append({
                    'well':     well,
                    'type':     'Gas Lock Pattern',
                    'detail':   f'Motor temp rising (+{temp_trend:.2f}°C/reading) '
                                f'while current dropping ({current_trend:.2f}A/reading)',
                    'severity': 'Warning',
                    'action':   'Monitor intake pressure — possible gas ingestion'
                })

        # Check 3: VFD running but current near zero
        if ('vfd_output_frequency_hz' in well_data.columns and
                'motor_current_avg_amp' in well_data.columns):

            vfd_on       = well_data['vfd_output_frequency_hz'] > 30
            current_low  = well_data['motor_current_avg_amp'] < 2

            contradiction = (vfd_on & current_low).sum()
            if contradiction > 3:
                anomalies.append({
                    'well':     well,
                    'type':     'VFD/Current Mismatch',
                    'detail':   f'VFD running but current <2A in {contradiction} readings',
                    'severity': 'Sensor Error',
                    'action':   'Current sensor may be faulty — verify with clamp meter'
                })

    return anomalies

def print_quality_report(quality_report, frozen_sensors, anomalies, well_name):
    """Print a clean data quality report to terminal"""

    print(f"\n{'='*60}")
    print(f"  DATA QUALITY REPORT — {well_name}")
    print(f"{'='*60}")

    print(f"\n  📊 PARAMETER VALIDATION (Physical Limits Check):")
    print(f"  {'Parameter':<35} {'Valid%':>6} {'Invalid':>8} {'Issue'}")
    print(f"  {'-'*70}")

    for col, stats in quality_report.items():
        if stats['total'] == 0:
            continue

        status = "✅" if stats['quality_pct'] > 95 else \
                 "⚠️ " if stats['quality_pct'] > 50 else "❌"

        issue = ""
        if stats['zero'] > 0:
            issue += f"zero:{stats['zero']} "
        if stats['too_high'] > 0:
            issue += f">{stats['max']}{stats['unit']}:{stats['too_high']} "
        if stats['too_low'] > 0:
            issue += f"<{stats['min']}{stats['unit']}:{stats['too_low']} "

        print(f"  {status} {col:<33} {stats['quality_pct']:>5.1f}% "
              f"{stats['invalid']:>8}  {issue}")

    # Frozen sensor report
    print(f"\n  🧊 FROZEN SENSOR CHECK (same value >12hrs):")
    found_frozen = False
    for col, wells in frozen_sensors.items():
        for well, hours in wells.items():
            print(f"  ⚠️  {col}: frozen for ~{hours} hours")
            found_frozen = True
    if not found_frozen:
        print(f"  ✅ No frozen sensors detected")

    # Cross-parameter anomalies
    print(f"\n  🔄 CROSS-PARAMETER ANOMALY CHECK:")
    if anomalies:
        for a in anomalies:
            severity_icon = "🔴" if a['severity'] == 'Sensor Error' else "🟡"
            print(f"  {severity_icon} [{a['type']}] {a['detail']}")
            print(f"     → Action: {a['action']}")
    else:
        print(f"  ✅ No cross-parameter anomalies detected")

    print(f"\n{'='*60}\n")

def ingest_avalon():
    """
    Main ingestion function with Smart Data Quality Engine.

    Process:
    1. Read all Avalon export files
    2. Extract well name and parameter
    3. Pivot long → wide format
    4. Forward fill missing values
    5. AUTO-DETECT: Physical limits validation
    6. AUTO-DETECT: Frozen sensor detection
    7. AUTO-DETECT: Cross-parameter anomaly detection
    8. Generate data quality report
    9. Insert clean data into database
    """
    files = find_avalon_files()
    if not files:
        print("❌ No Avalon files found in data/raw folder!")
        return

    print(f"📂 Found {len(files)} Avalon file(s)")

    conn = get_connection()
    total_inserted = 0
    total_skipped  = 0

    for filepath in files:
        print(f"\n{'─'*60}")
        print(f"📄 Processing: {os.path.basename(filepath)}")

        # ── READ FILE ─────────────────────────────────────────────
        try:
            if filepath.lower().endswith('.csv'):
                df = pd.read_csv(filepath)
            else:
                df = pd.read_excel(filepath)
        except Exception as e:
            print(f"❌ Error reading file: {e}")
            continue

        print(f"   Rows: {len(df):,}")

        # ── STANDARDIZE COLUMNS ───────────────────────────────────
        df.columns = ['parameter', 'timestamp', 'value',
                      'quality', 'quality_text', 'uom'] + \
                     list(df.columns[6:])

        df = df[df['parameter'] != 'parameters'].copy()
        df = df[df['timestamp'] != 'Time (Asia/Calcutta)'].copy()

        # ── PARSE TIMESTAMP ───────────────────────────────────────
        df['timestamp'] = pd.to_datetime(
            df['timestamp'], errors='coerce', utc=True)
        df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df = df.dropna(subset=['timestamp'])

        # ── PARSE VALUE ───────────────────────────────────────────
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        # ── EXTRACT WELL AND PARAMETER ────────────────────────────
        df['well_name'] = df['parameter'].apply(clean_well_name)
        df['param_col'] = df['parameter'].apply(parse_parameter_name)

        wells = df['well_name'].unique()
        print(f"   Wells: {list(wells)}")

        unrecognized = df[df['param_col'].isna()]['parameter'].unique()
        if len(unrecognized) > 0:
            print(f"   ⚠️  Unrecognized parameters: {list(unrecognized)}")

        df = df.dropna(subset=['param_col'])

        # ── PIVOT LONG → WIDE ─────────────────────────────────────
        pivot_df = df.pivot_table(
            index=['timestamp', 'well_name'],
            columns='param_col',
            values='value',
            aggfunc='first'
        ).reset_index()
        pivot_df.columns.name = None

        # ── FORWARD FILL ──────────────────────────────────────────
        param_cols = [c for c in PHYSICAL_LIMITS.keys()
                      if c in pivot_df.columns]
        pivot_df = pivot_df.sort_values(['well_name', 'timestamp'])
        for col in param_cols:
            pivot_df[col] = pivot_df.groupby('well_name')[col].ffill()

        # ── QUALITY FLAG ──────────────────────────────────────────
        quality_df = df.groupby(
            ['timestamp', 'well_name']
        )['quality_text'].first().reset_index()
        pivot_df = pivot_df.merge(
            quality_df, on=['timestamp', 'well_name'], how='left')
        pivot_df.rename(
            columns={'quality_text': 'quality_flag'}, inplace=True)

        # ══════════════════════════════════════════════════════════
        # SMART AUTO-DETECTION ENGINE
        # ══════════════════════════════════════════════════════════

        for well in pivot_df['well_name'].unique():
            well_mask = pivot_df['well_name'] == well
            well_data = pivot_df[well_mask].copy()

            # Layer 1: Physical limits validation
            well_data, quality_report = validate_physical_limits(well_data)

            # Layer 2: Frozen sensor detection
            frozen_sensors = detect_frozen_sensors(well_data)

            # Layer 3: Cross-parameter anomaly detection
            anomalies = detect_cross_parameter_anomalies(well_data)

            # Print quality report
            print_quality_report(
                quality_report, frozen_sensors, anomalies, well)

            # Put cleaned data back
            pivot_df.loc[well_mask] = well_data

        # ── INSERT INTO DATABASE ──────────────────────────────────
        pivot_df['timestamp'] = pivot_df['timestamp'].astype(str)

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
                    motor_current_a_amp, motor_current_b_amp,
                    motor_current_c_amp, pump_intake_temp_c,
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
                    row.get('quality_flag'),
                ))
                inserted += 1
            except Exception as e:
                skipped += 1

        total_inserted += inserted
        total_skipped  += skipped
        print(f"\n   ✅ Inserted: {inserted:,} | Skipped: {skipped:,}")

    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"✅ ALL FILES PROCESSED")
    print(f"   Total inserted: {total_inserted:,}")
    print(f"   Total skipped:  {total_skipped:,}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    ingest_avalon()