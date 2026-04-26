import pandas as pd
import numpy as np
import os
import re
from database import get_connection

RAW_FOLDER = os.path.join(os.path.dirname(__file__), "data", "raw")

# ══════════════════════════════════════════════════════════════════════════════
# ██████████████████████████████████████████████████████████████████████████
# CONFIGURATION BLOCK — ALL RULES AND THRESHOLDS LIVE HERE
# To tweak any logic, only change values in this block
# Never need to touch the functions below
# ██████████████████████████████████████████████████████████████████████████
# ══════════════════════════════════════════════════════════════════════════════

ESP_CONFIG = {

    # ── PHYSICAL LIMITS ───────────────────────────────────────────────────────
    # Hard boundaries — values outside these are physically impossible
    # Any reading outside min/max is a sensor error, set to None
    'physical_limits': {
        'motor_temp_1_c':              {'min': 50,   'max': 200,  'unit': '°C',   'check_frozen': True,  'freeze_std_threshold': 0.1,  'freeze_days': 7},
        'pump_intake_temp_c':          {'min': 60,   'max': 150,  'unit': '°C',   'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
        'pump_intake_pressure_psi':    {'min': 100,  'max': 3000, 'unit': 'psi',  'check_frozen': True,  'freeze_std_threshold': 5.0,  'freeze_days': 60},
        'pump_discharge_pressure_psi': {'min': 100,  'max': 3000, 'unit': 'psi',  'check_frozen': True,  'freeze_std_threshold': 5.0,  'freeze_days': 60},
        'vfd_output_frequency_hz':     {'min': 0,    'max': 65,   'unit': 'Hz',   'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
        'motor_load_pct':              {'min': 0,    'max': 100,  'unit': '%',    'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
        'motor_current_avg_amp':       {'min': 0,    'max': 100,  'unit': 'A',    'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
        'motor_current_a_amp':         {'min': 0,    'max': 100,  'unit': 'A',    'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
        'motor_current_b_amp':         {'min': 0,    'max': 100,  'unit': 'A',    'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
        'motor_current_c_amp':         {'min': 0,    'max': 100,  'unit': 'A',    'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
        'vibration_x':                 {'min': 0,    'max': 5,    'unit': 'm/s²', 'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
        'vibration_y':                 {'min': 0,    'max': 5,    'unit': 'm/s²', 'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
        'vibration_z':                 {'min': 0,    'max': 5,    'unit': 'm/s²', 'check_frozen': False, 'freeze_std_threshold': None, 'freeze_days': None},
    },

    # ── WELL STATE THRESHOLDS ─────────────────────────────────────────────────
    # Defines what "running" and "stopped" look like
    # Adjust these if your field uses different setpoints
    'well_state': {
        'running_freq_min':    30,    # Hz — below this = ESP not running
        'running_current_min': 1.0,   # Amps — below this = no current flowing
        'trip_detection_window': 3,   # readings — how many consecutive zeros = trip
    },

    # ── CROSS-PARAMETER RULES ─────────────────────────────────────────────────
    # Rules that check relationships between parameters
    # These catch contradictions that individual checks miss
    'cross_parameter': {
        # Sensor conflict detection
        'freq_current_conflict': True,   # Check if VFD and current disagree

        # Gas lock detection
        'gas_lock_check': True,
        'gas_lock_temp_rise_per_reading': 0.5,    # °C — min temp rise to flag
        'gas_lock_current_drop_per_reading': 0.1, # A — min current drop to flag
        'gas_lock_window': 10,                     # readings to check trend over

        # Phase imbalance
        'phase_imbalance_check': True,
        'phase_imbalance_warning_pct': 5,   # % — warn above this
        'phase_imbalance_critical_pct': 10, # % — critical above this

        # Motor overload
        'overload_check': True,
        'overload_load_pct': 90,    # % motor load — warn above this
        'underload_load_pct': 30,   # % motor load — warn below this (gas lock)
    },

    # ── INTERMITTENT WELL DETECTION ───────────────────────────────────────────
    # Identifies wells that cycle on/off (intermittent producers)
    # These have different validation rules — pressure cycling is NORMAL
    'intermittent': {
        'detect': True,
        'min_cycles_to_classify': 3,      # How many on/off cycles = intermittent
        'cycle_window_days': 30,           # Look back this many days
        'shutdown_min_hours': 4,           # Min hours off to count as a cycle
        'pressure_drop_normal_psi': 300,  # Pressure drop this large is normal for intermittent
    },

    # ── MOTOR TEMPERATURE RULES ───────────────────────────────────────────────
    # Ratna Field specific — trip set at 150°C
    'motor_temp': {
        'trip_point':     150,  # °C — actual trip setpoint in field
        'critical':       145,  # °C — alert before reaching trip
        'warning':        135,  # °C — early warning
        'delta_t_warning':  35, # °C above intake temp — warn
        'delta_t_critical': 45, # °C above intake temp — critical
    },

    # ── VFD FREQUENCY RULES ───────────────────────────────────────────────────
    # Ratna Field operating range: 40-60 Hz
    'vfd': {
        'min_operating': 40,  # Hz — below this while running = low inflow
        'max_operating': 60,  # Hz — above this = above design limit
        'absolute_max':  65,  # Hz — physically impossible above this
    },

    # ── PARAMETER MAP ─────────────────────────────────────────────────────────
    # Maps Avalon parameter names to database column names
    # Add new parameters here if Avalon adds more in future
    'param_map': {
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
    },
}

# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS — FILE READING
# ══════════════════════════════════════════════════════════════════════════════

def find_avalon_files():
    """Find ALL Avalon export files in data/raw folder"""
    files = []
    for f in os.listdir(RAW_FOLDER):
        if "avalon" in f.lower():
            files.append(os.path.join(RAW_FOLDER, f))
    return sorted(files)

def clean_well_name(raw_name):
    """
    Extract well name from Avalon parameter string.
    Real format:    ONGC.NH.Ratna Field.R7A.R7A1.Motor Temperature 1
    Masked format:  BGOIL well2Motor Temperature 1
    """
    try:
        parts = str(raw_name).strip().split('.')
        if len(parts) >= 6:
            return parts[4].strip()
        match = re.search(r'\bwell(\w+)', raw_name, re.IGNORECASE)
        if match:
            raw_id  = match.group(0)
            id_match = re.match(r'(well\d+)', raw_id, re.IGNORECASE)
            if id_match:
                return id_match.group(1)
            return raw_id
    except Exception:
        pass
    return str(raw_name).strip()

def parse_parameter_name(raw_name):
    """Map Avalon parameter string to database column name"""
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

    for key, col in ESP_CONFIG['param_map'].items():
        if key.lower() in param_str.lower():
            return col
    return None

# ══════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE ENGINE — LAYER 1: WELL STATE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_well_state(well_df):
    """
    Determine the operational state of the well at each timestamp.

    States:
    - RUNNING:      VFD > running_freq_min AND current > running_current_min
    - SHUTDOWN:     VFD = 0 AND current = 0
    - SENSOR_FAULT: VFD and current disagree with each other
    - TRIP:         Sudden transition from RUNNING to SHUTDOWN
    - INTERMITTENT: Well cycling between RUNNING and SHUTDOWN repeatedly

    This state is used by all other checks — different rules apply
    in different states. A sensor reading that looks wrong in RUNNING
    state may be perfectly normal in SHUTDOWN state.
    """
    cfg     = ESP_CONFIG['well_state']
    freq    = well_df.get('vfd_output_frequency_hz',  pd.Series([None] * len(well_df)))
    current = well_df.get('motor_current_avg_amp',     pd.Series([None] * len(well_df)))

    states = []
    for i, (f, c) in enumerate(zip(freq, current)):
        f_valid = f is not None and not pd.isna(f)
        c_valid = c is not None and not pd.isna(c)

        if not f_valid and not c_valid:
            states.append('UNKNOWN')
        elif f_valid and c_valid:
            f_running = f > cfg['running_freq_min']
            c_running = c > cfg['running_current_min']

            if f_running and c_running:
                states.append('RUNNING')
            elif not f_running and not c_running:
                states.append('SHUTDOWN')
            elif f_running and not c_running:
                # VFD says running but no current — current sensor fault
                states.append('SENSOR_FAULT_CURRENT')
            else:
                # Current flowing but VFD says off — frequency sensor fault
                states.append('SENSOR_FAULT_FREQ')
        elif f_valid:
            states.append('RUNNING' if f > cfg['running_freq_min'] else 'SHUTDOWN')
        else:
            states.append('RUNNING' if c > cfg['running_current_min'] else 'SHUTDOWN')

    well_df = well_df.copy()
    well_df['well_state'] = states

    # ── TRIP DETECTION ────────────────────────────────────────────────────────
    # A TRIP is a sudden transition from RUNNING to SHUTDOWN
    # (not a planned shutdown which would show gradual frequency reduction)
    prev_states  = pd.Series(states).shift(1)
    curr_states  = pd.Series(states)
    trip_mask    = (prev_states == 'RUNNING') & (curr_states == 'SHUTDOWN')
    well_df['is_trip'] = trip_mask.values

    # ── INTERMITTENT DETECTION ────────────────────────────────────────────────
    cfg_int   = ESP_CONFIG['intermittent']
    if cfg_int['detect']:
        state_series  = pd.Series(states)
        transitions   = (state_series != state_series.shift(1)).sum()
        # Each on/off cycle = 2 transitions
        cycles        = transitions // 2
        well_df['is_intermittent'] = cycles >= cfg_int['min_cycles_to_classify']
    else:
        well_df['is_intermittent'] = False

    return well_df

# ══════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE ENGINE — LAYER 2: PHYSICAL LIMITS VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_physical_limits(well_df):
    """
    Check every parameter against physical boundaries.
    Values outside min/max are physically impossible → set to None.

    Uses ESP_CONFIG['physical_limits'] — all thresholds configurable above.
    """
    quality_report = {}

    for col, limits in ESP_CONFIG['physical_limits'].items():
        if col not in well_df.columns:
            continue

        total      = well_df[col].notna().sum()
        zero_count = (well_df[col] == 0).sum()
        low_count  = (well_df[col] < limits['min']).sum()
        high_count = (well_df[col] > limits['max']).sum()
        invalid    = zero_count + low_count + high_count

        # Special case: VFD and current CAN be zero (ESP is OFF)
        # Only flag as invalid if ESP appears to be running
        if col in ['vfd_output_frequency_hz', 'motor_current_avg_amp',
                   'motor_current_a_amp', 'motor_current_b_amp',
                   'motor_current_c_amp']:
            # Zero is valid for these when ESP is shutdown
            # Only clean values above physical max
            well_df[col] = well_df[col].where(
                well_df[col].isna() |
                (well_df[col] >= 0) & (well_df[col] <= limits['max']),
                other=None
            )
        else:
            # For pressure, temperature — zero and out of range are invalid
            well_df[col] = well_df[col].where(
                well_df[col].isna() |
                ((well_df[col] > limits['min']) &
                 (well_df[col] < limits['max'])),
                other=None
            )

        valid_after  = well_df[col].notna().sum()
        quality_pct  = (valid_after / total * 100) if total > 0 else 0

        quality_report[col] = {
            'total':       int(total),
            'invalid':     int(invalid),
            'zero':        int(zero_count),
            'too_low':     int(low_count),
            'too_high':    int(high_count),
            'valid_after': int(valid_after),
            'quality_pct': round(quality_pct, 1),
            'unit':        limits['unit'],
            'min':         limits['min'],
            'max':         limits['max'],
        }

    return well_df, quality_report

# ══════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE ENGINE — LAYER 3: FROZEN SENSOR DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_frozen_sensors(well_df):
    """
    Detect sensors stuck at same value — but only for parameters where
    variation is EXPECTED during normal operation.

    Key insight from field operations:
    - VFD frequency: operator-set, stable is NORMAL → skip check
    - Current: follows frequency, stable is NORMAL → skip check
    - Intake temperature: formation fluid, very stable → skip check
    - Motor temperature: should vary with load → CHECK
    - Intake/Discharge pressure: should trend over time → CHECK

    A sensor is FROZEN if:
    1. std deviation < threshold over freeze_days window
    AND
    2. Well was RUNNING during that period (not shutdown)
    AND
    3. At least one other parameter WAS varying (confirms well is active)

    All thresholds in ESP_CONFIG['physical_limits'][col]['freeze_*']
    """
    frozen_report = {}

    for col, limits in ESP_CONFIG['physical_limits'].items():
        if not limits.get('check_frozen'):
            continue
        if col not in well_df.columns:
            continue

        threshold_std  = limits['freeze_std_threshold']
        freeze_days    = limits['freeze_days']

        if threshold_std is None or freeze_days is None:
            continue

        # Only check during RUNNING periods
        running_mask = well_df.get('well_state', pd.Series(
            ['UNKNOWN'] * len(well_df))) == 'RUNNING'
        running_data = well_df[running_mask][['timestamp', col]].dropna()

        if len(running_data) < 10:
            continue

        running_data = running_data.set_index('timestamp').sort_index()

        # Rolling std deviation over freeze_days window
        # If std is below threshold for entire window → frozen
        rolling_std = running_data[col].rolling(
            window=f'{freeze_days}D',
            min_periods=5
        ).std()

        frozen_periods = rolling_std < threshold_std
        frozen_pct     = frozen_periods.mean() * 100

        if frozen_pct > 20:  # If >20% of time appears frozen → flag
            # Find the longest frozen period
            max_frozen_hours = 0
            consecutive = 0

            for i in range(len(frozen_periods)):
                if frozen_periods.iloc[i]:
                    consecutive += 1
                else:
                    if consecutive > max_frozen_hours:
                        max_frozen_hours = consecutive
                    consecutive = 0

            # Convert readings to approximate hours
            # Estimate based on actual time span / number of readings
            if len(running_data) > 1:
                total_hours = (
                    running_data.index[-1] - running_data.index[0]
                ).total_seconds() / 3600
                hours_per_reading = total_hours / len(running_data)
                frozen_hours = max_frozen_hours * hours_per_reading
            else:
                frozen_hours = 0

            if frozen_hours > 24:
                frozen_report[col] = {
                    'frozen_hours':  round(frozen_hours, 1),
                    'frozen_pct':    round(frozen_pct, 1),
                    'std_threshold': threshold_std,
                }

    return frozen_report

# ══════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE ENGINE — LAYER 4: CROSS-PARAMETER ANOMALY DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_cross_parameter_anomalies(well_df, well_name):
    """
    Detect contradictions and patterns across multiple parameters.

    Checks enabled/disabled in ESP_CONFIG['cross_parameter'].

    Current checks:
    1. Sensor conflict — VFD and current disagree
    2. Gas lock pattern — temp rising while current dropping
    3. Phase current imbalance
    4. Motor overload / underload
    5. Pressure dynamics for intermittent wells
    """
    cfg       = ESP_CONFIG['cross_parameter']
    anomalies = []

    # Only check during RUNNING periods for operational anomalies
    running_mask = well_df.get('well_state', pd.Series(
        ['UNKNOWN'] * len(well_df))) == 'RUNNING'
    running_df   = well_df[running_mask].copy()

    if len(running_df) < 5:
        return anomalies

    # ── CHECK 1: SENSOR CONFLICT ──────────────────────────────────────────────
    if cfg['freq_current_conflict']:
        sensor_faults = well_df[
            well_df['well_state'].isin(
                ['SENSOR_FAULT_CURRENT', 'SENSOR_FAULT_FREQ'])
        ] if 'well_state' in well_df.columns else pd.DataFrame()

        if len(sensor_faults) > 3:
            fault_type = sensor_faults['well_state'].mode()[0]
            anomalies.append({
                'type':     'Sensor Conflict',
                'severity': '🔴 Sensor Error',
                'detail':   f'VFD and current disagree in {len(sensor_faults)} readings '
                            f'({fault_type})',
                'action':   'Verify frequency or current sensor — one may be faulty'
            })

    # ── CHECK 2: GAS LOCK PATTERN ─────────────────────────────────────────────
    if cfg['gas_lock_check']:
        if ('motor_temp_1_c' in running_df.columns and
                'motor_current_avg_amp' in running_df.columns):

            window       = cfg['gas_lock_window']
            recent       = running_df.tail(window)
            temp_trend   = recent['motor_temp_1_c'].diff().mean()
            curr_trend   = recent['motor_current_avg_amp'].diff().mean()

            if (temp_trend > cfg['gas_lock_temp_rise_per_reading'] and
                    curr_trend < -cfg['gas_lock_current_drop_per_reading']):
                anomalies.append({
                    'type':     'Gas Lock Pattern',
                    'severity': '🟡 Warning',
                    'detail':   f'Motor temp rising (+{temp_trend:.2f}°C/reading) '
                                f'while current dropping ({curr_trend:.2f}A/reading)',
                    'action':   'Monitor intake pressure — possible gas ingestion developing'
                })

    # ── CHECK 3: PHASE CURRENT IMBALANCE ─────────────────────────────────────
    if cfg['phase_imbalance_check']:
        phase_cols = ['motor_current_a_amp', 'motor_current_b_amp',
                      'motor_current_c_amp']
        if all(c in running_df.columns for c in phase_cols):
            latest = running_df.tail(10)
            ca  = latest['motor_current_a_amp'].mean()
            cb  = latest['motor_current_b_amp'].mean()
            cc  = latest['motor_current_c_amp'].mean()
            avg = (ca + cb + cc) / 3

            if avg > 0:
                imbalance = max(
                    abs(ca-avg), abs(cb-avg), abs(cc-avg)
                ) / avg * 100

                if imbalance > cfg['phase_imbalance_critical_pct']:
                    anomalies.append({
                        'type':     'Phase Imbalance',
                        'severity': '🔴 Critical',
                        'detail':   f'Phase imbalance: {imbalance:.1f}% '
                                    f'(A:{ca:.1f}A B:{cb:.1f}A C:{cc:.1f}A)',
                        'action':   'Cable integrity suspected — plan megger test on next shutdown'
                    })
                elif imbalance > cfg['phase_imbalance_warning_pct']:
                    anomalies.append({
                        'type':     'Phase Imbalance',
                        'severity': '🟡 Warning',
                        'detail':   f'Phase imbalance: {imbalance:.1f}% '
                                    f'(A:{ca:.1f}A B:{cb:.1f}A C:{cc:.1f}A)',
                        'action':   'Monitor cable health — early imbalance developing'
                    })

    # ── CHECK 4: MOTOR OVERLOAD / UNDERLOAD ───────────────────────────────────
    if cfg['overload_check'] and 'motor_load_pct' in running_df.columns:
        recent_load = running_df['motor_load_pct'].tail(6).mean()

        if recent_load > cfg['overload_load_pct']:
            anomalies.append({
                'type':     'Motor Overload',
                'severity': '🔴 Critical',
                'detail':   f'Motor load averaging {recent_load:.1f}% — above {cfg["overload_load_pct"]}%',
                'action':   'Reduce VFD frequency — motor running above rated load'
            })
        elif recent_load < cfg['underload_load_pct'] and recent_load > 5:
            anomalies.append({
                'type':     'Motor Underload',
                'severity': '🟡 Warning',
                'detail':   f'Motor load averaging {recent_load:.1f}% — below {cfg["underload_load_pct"]}%',
                'action':   'Possible gas ingestion or low inflow — check intake pressure trend'
            })

    # ── CHECK 5: TEMPERATURE DELTA T ─────────────────────────────────────────
    temp_cfg = ESP_CONFIG['motor_temp']
    if ('motor_temp_1_c' in running_df.columns and
            'pump_intake_temp_c' in running_df.columns):
        latest_temp    = running_df['motor_temp_1_c'].tail(6).mean()
        latest_intake  = running_df['pump_intake_temp_c'].tail(6).mean()

        if pd.notna(latest_temp) and pd.notna(latest_intake):
            delta_t = latest_temp - latest_intake

            if delta_t > temp_cfg['delta_t_critical']:
                anomalies.append({
                    'type':     'High Motor ΔT',
                    'severity': '🔴 Critical',
                    'detail':   f'ΔT = {delta_t:.1f}°C (Motor:{latest_temp:.1f}°C '
                                f'Intake:{latest_intake:.1f}°C)',
                    'action':   'Possible gas lock or scale buildup on motor — investigate immediately'
                })
            elif delta_t > temp_cfg['delta_t_warning']:
                anomalies.append({
                    'type':     'Elevated Motor ΔT',
                    'severity': '🟡 Warning',
                    'detail':   f'ΔT = {delta_t:.1f}°C (Motor:{latest_temp:.1f}°C '
                                f'Intake:{latest_intake:.1f}°C)',
                    'action':   'Motor running hotter than normal — monitor closely'
                })

    # ── CHECK 6: TRIP COUNT ───────────────────────────────────────────────────
    # Count how many times the well tripped — high trip count = instability
    if 'is_trip' in well_df.columns:
        trip_count = well_df['is_trip'].sum()
        if trip_count > 5:
            anomalies.append({
                'type':     'Frequent Trips',
                'severity': '🟡 Warning',
                'detail':   f'Well tripped {trip_count} times in this data period',
                'action':   'Investigate root cause — frequent trips accelerate ESP wear'
            })

    return anomalies

# ══════════════════════════════════════════════════════════════════════════════
# REPORTING
# ══════════════════════════════════════════════════════════════════════════════

def print_quality_report(quality_report, frozen_report,
                         anomalies, well_name, well_df):
    """Print formatted data quality report to terminal"""

    print(f"\n{'='*65}")
    print(f"  DATA QUALITY REPORT — {well_name}")
    print(f"{'='*65}")

    # Well state summary
    if 'well_state' in well_df.columns:
        state_counts  = well_df['well_state'].value_counts()
        total         = len(well_df)
        running_pct   = state_counts.get('RUNNING',  0) / total * 100
        shutdown_pct  = state_counts.get('SHUTDOWN', 0) / total * 100
        trip_count    = well_df.get('is_trip', pd.Series([False]*total)).sum()
        is_intermittent = well_df.get(
            'is_intermittent', pd.Series([False]*total)).any()

        print(f"\n  ⚙️  WELL STATE SUMMARY:")
        print(f"     Running:    {running_pct:5.1f}% of time")
        print(f"     Shutdown:   {shutdown_pct:5.1f}% of time")
        print(f"     Trips detected: {trip_count}")
        print(f"     Well type:  {'INTERMITTENT' if is_intermittent else 'CONTINUOUS'}")

    # Parameter validation
    print(f"\n  📊 PARAMETER VALIDATION (Physical Limits):")
    print(f"  {'Parameter':<35} {'Valid%':>6}  {'Invalid':>8}  Issue")
    print(f"  {'-'*70}")

    for col, stats in quality_report.items():
        if stats['total'] == 0:
            continue

        status = "✅" if stats['quality_pct'] > 95 else \
                 "🟡" if stats['quality_pct'] > 50 else "❌"

        issue = ""
        if stats['zero']     > 0: issue += f"zero:{stats['zero']} "
        if stats['too_high'] > 0: issue += f">{stats['max']}{stats['unit']}:{stats['too_high']} "
        if stats['too_low']  > 0: issue += f"<{stats['min']}{stats['unit']}:{stats['too_low']} "

        print(f"  {status} {col:<33} {stats['quality_pct']:>5.1f}%  "
              f"{stats['invalid']:>8}  {issue or 'clean'}")

    # Frozen sensors
    print(f"\n  🧊 FROZEN SENSOR CHECK:")
    if frozen_report:
        for col, info in frozen_report.items():
            print(f"  ⚠️  {col}: frozen ~{info['frozen_hours']:.0f}hrs "
                  f"({info['frozen_pct']:.0f}% of running time)")
    else:
        print(f"  ✅ No frozen sensors detected")

    # Cross-parameter anomalies
    print(f"\n  🔄 CROSS-PARAMETER ANOMALY CHECK:")
    if anomalies:
        for a in anomalies:
            print(f"  {a['severity']} [{a['type']}]")
            print(f"     Detail: {a['detail']}")
            print(f"     Action: {a['action']}")
    else:
        print(f"  ✅ No cross-parameter anomalies detected")

    print(f"\n{'='*65}\n")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN INGESTION FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

def ingest_avalon():
    """
    Main ingestion with complete Smart Data Quality Engine.

    Pipeline:
    1. Read all Avalon files
    2. Extract well name + parameter
    3. Pivot long → wide
    4. Forward fill
    5. Layer 1: Physical limits validation
    6. Layer 2: Well state detection
    7. Layer 3: Frozen sensor detection
    8. Layer 4: Cross-parameter anomaly detection
    9. Generate quality report
    10. Insert clean data to database
    """
    files = find_avalon_files()
    if not files:
        print("❌ No Avalon files found in data/raw folder!")
        return

    print(f"📂 Found {len(files)} Avalon file(s)")

    conn           = get_connection()
    total_inserted = 0
    total_skipped  = 0

    for filepath in files:
        print(f"\n{'─'*65}")
        print(f"📄 Processing: {os.path.basename(filepath)}")

        # ── READ FILE ─────────────────────────────────────────────────────────
        try:
            df = pd.read_csv(filepath) if filepath.lower().endswith('.csv') \
                 else pd.read_excel(filepath)
        except Exception as e:
            print(f"❌ Error reading: {e}")
            continue

        print(f"   Rows: {len(df):,}")

        # ── STANDARDIZE COLUMNS ───────────────────────────────────────────────
        df.columns = ['parameter', 'timestamp', 'value',
                      'quality', 'quality_text', 'uom'] + list(df.columns[6:])
        df = df[df['parameter'] != 'parameters'].copy()
        df = df[df['timestamp'] != 'Time (Asia/Calcutta)'].copy()

        # ── PARSE TIMESTAMP ───────────────────────────────────────────────────
        df['timestamp'] = pd.to_datetime(
            df['timestamp'], errors='coerce', utc=True)
        df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df = df.dropna(subset=['timestamp'])

        # ── PARSE VALUE ───────────────────────────────────────────────────────
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        # ── EXTRACT WELL AND PARAMETER ────────────────────────────────────────
        df['well_name'] = df['parameter'].apply(clean_well_name)
        df['param_col'] = df['parameter'].apply(parse_parameter_name)

        wells = list(df['well_name'].unique())
        print(f"   Wells: {wells}")

        unrecognized = df[df['param_col'].isna()]['parameter'].unique()
        if len(unrecognized):
            print(f"   ⚠️  Unrecognized parameters: {list(unrecognized)}")

        df = df.dropna(subset=['param_col'])

        # ── PIVOT LONG → WIDE ─────────────────────────────────────────────────
        pivot_df = df.pivot_table(
            index=['timestamp', 'well_name'],
            columns='param_col',
            values='value',
            aggfunc='first'
        ).reset_index()
        pivot_df.columns.name = None

        # ── FORWARD FILL ──────────────────────────────────────────────────────
        param_cols = [c for c in ESP_CONFIG['physical_limits'].keys()
                      if c in pivot_df.columns]
        pivot_df   = pivot_df.sort_values(['well_name', 'timestamp'])
        for col in param_cols:
            pivot_df[col] = pivot_df.groupby('well_name')[col].ffill()

        # ── QUALITY FLAG ──────────────────────────────────────────────────────
        quality_df = df.groupby(
            ['timestamp', 'well_name'])['quality_text'].first().reset_index()
        pivot_df   = pivot_df.merge(
            quality_df, on=['timestamp', 'well_name'], how='left')
        pivot_df.rename(columns={'quality_text': 'quality_flag'}, inplace=True)

        # ══════════════════════════════════════════════════════════════════════
        # SMART INTELLIGENCE ENGINE — PER WELL
        # ══════════════════════════════════════════════════════════════════════
        for well in pivot_df['well_name'].unique():
            mask      = pivot_df['well_name'] == well
            well_data = pivot_df[mask].copy().reset_index(drop=True)

            # Layer 1: Physical limits
            well_data, quality_report = validate_physical_limits(well_data)

            # Layer 2: Well state detection
            well_data = detect_well_state(well_data)

            # Layer 3: Frozen sensor detection
            frozen_report = detect_frozen_sensors(well_data)

            # Layer 4: Cross-parameter anomalies
            anomalies = detect_cross_parameter_anomalies(well_data, well)

            # Print report
            print_quality_report(
                quality_report, frozen_report, anomalies, well, well_data)

            # Remove helper columns before database insert
            well_data = well_data.drop(
                columns=['well_state', 'is_trip', 'is_intermittent'],
                errors='ignore'
            )

            # Put cleaned data back
            pivot_df.loc[mask] = well_data.values

        # ── INSERT INTO DATABASE ──────────────────────────────────────────────
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
        print(f"   ✅ Inserted: {inserted:,} | Skipped: {skipped:,}")

    conn.commit()
    conn.close()

    print(f"\n{'='*65}")
    print(f"✅ ALL FILES PROCESSED")
    print(f"   Total inserted: {total_inserted:,}")
    print(f"   Total skipped:  {total_skipped:,}")
    print(f"{'='*65}\n")

if __name__ == "__main__":
    ingest_avalon()