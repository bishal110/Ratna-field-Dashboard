"""Step 1 configuration for ESP failure prediction (R9A).

Purpose:
- Keep every tunable setting in one place.
- Ensure Step 2+ modules contain no hardcoded operational values.
"""

from pathlib import Path

# -----------------------------------------------------------------------------
# Core paths
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ESP_PREDICTION_DIR = PROJECT_ROOT / "esp_prediction"
MODELS_DIR = ESP_PREDICTION_DIR / "models"
DB_PATH = PROJECT_ROOT / "ratna_field.db"

# Absolute Windows reference path requested by user notes.
RAW_DATA_WINDOWS_PATH = (
    r"C:\Users\bisha\Documents\Ratna field Dashboard\data\raw"
)

# -----------------------------------------------------------------------------
# File discovery config (must be used by find_r9a_file in data_ingestion.py)
# -----------------------------------------------------------------------------
R9A_FILE_CONFIG = {
    "folder": RAW_DATA_WINDOWS_PATH,
    "strategy": "keyword",
    "keywords": ["R9A", "r9a"],
    "extension": ".xlsx",
    "fallback_exact": "ONGC_NH Asset_R9A - 27-07-2023.xlsx",
    "error_if_missing": (
        "No R9A Excel file found in data/raw/. "
        "Place the file there and run again."
    ),
}

# Optional runtime fallback for non-Windows/dev containers.
# Step 2 can use this if R9A_FILE_CONFIG['folder'] does not exist.
RAW_DATA_REPO_FALLBACK = str(PROJECT_ROOT / "data" / "raw")

# -----------------------------------------------------------------------------
# Wells + normalization + pump catalog
# -----------------------------------------------------------------------------
CANONICAL_WELLS = ["R9A#1", "R9A#2", "R9A#4", "R9A#5"]

WELL_NAME_NORMALIZATION = {
    "R9A1": "R9A#1",
    "R9A#1": "R9A#1",
    "R9A2": "R9A#2",
    "R9A#2": "R9A#2",
    "R9A4": "R9A#4",
    "R9A#4": "R9A#4",
    "R9A5": "R9A#5",
    "R9A#5": "R9A#5",
}

PUMP_CATALOG = {
    "R9A#1": {"model": "NHV(790-1000)H", "stages": 252},
    "R9A#2": {"model": "406 NH-ER(1500-2500)H", "stages": 116},
    "R9A#4": {"model": "NH-ER(1500-2500)H", "stages": 126},
    "R9A#5": {"model": "NH-ER(1500-2500)H", "stages": 116},
}

# -----------------------------------------------------------------------------
# Sheet matching patterns (case-insensitive contains checks)
# -----------------------------------------------------------------------------
SHEET_MATCH_RULES = {
    "esp_parameter_sheets": {
        "R9A#1": ["r9a#1", "r9a1"],
        "R9A#2": ["r9a#2", "r9a2"],
        "R9A#4": ["r9a#4", "r9a4"],
        "R9A#5": ["r9a#5", "r9a5"],
    },
    "start_stop_sheets": {
        "R9A#1": ["start_stop counter", "r9a#1", "r9a1"],
        "R9A#2": ["start_stop counter", "r9a#2", "r9a2"],
        "R9A#4": ["start_stop counter", "r9a#4", "r9a4"],
        "R9A#5": ["start_stop counter", "r9a#5", "r9a5"],
    },
    "summary_sheet": ["summary r9a", "summary"],
}

# -----------------------------------------------------------------------------
# Header detection + parsing behavior
# -----------------------------------------------------------------------------
HEADER_DETECTION = {
    "date_anchor_values": ["date", "Date", "DATE"],
    "date_anchor_column_index": 0,
}

DATE_PARSE_SETTINGS = {
    "dayfirst": True,
    "errors": "coerce",
}

TEXT_AS_NAN_TOKENS = ["sensor readings lost", "na", "n/a", "-", ""]

# Choke conversion must use fractions.Fraction in Step 2.
CHOKE_PARSE_SETTINGS = {
    "fraction_separator": "/",
    "allow_fraction_strings": True,
}

# -----------------------------------------------------------------------------
# Column aliases (verify against actual workbook in Step 2 run output)
# -----------------------------------------------------------------------------
ESP_COLUMN_ALIASES = {
    "date": ["Date", "DATE", "Data Recorded Date", "Recorded Date"],
    "time": ["Time", "TIME"],
    "frequency_hz": ["Frequency (Hz)", "Frequency", "Hz"],
    "esm_active_current_amps": [
        "ESM Active Current (Amps)",
        "ESM Active Current",
        "ESM Active Curr.",
    ],
    "total_esm_current_amps": [
        "Total ESM Current (Amps)",
        "Total ESM Current",
        "Total ESM Curr.",
    ],
    "pi_psia": ["Pi (Intake Pressure psia)", "Pi", "Intake Pressure"],
    "pd_psia": ["Pd (Discharge Pressure psia)", "Pd", "Discharge Pressure"],
    "ti_c": ["Ti (Intake Temp °C)", "Ti", "Intake Temp"],
    "tm_c": ["TM (Motor Temp °C)", "TM", "Motor Temp"],
    "vibration_xyz": ["Vibration Vx/Vy/Vz (mm/s)", "Vibration", "Vx/Vy/Vz"],
    "header_pressure_bar": ["Production Header Pressure (Bar)", "Header Pressure"],
    "fthp_kgcm2": ["FTHP (kg/cm²)", "FTHP"],
    "fat_c": ["FAT (Flow Arm Temp °C)", "FAT"],
    "abc_sec_pressure_kgcm2": [
        "A/B/C Sec Pressure (kg/cm²)",
        "A/B/C Sec Pressure",
    ],
    "current_ia_ib_ic": ["Current IA/IB/IC (Amps)", "IA/IB/IC"],
    "motor_load_pct": ["Motor Load (%)", "Motor Load"],
    "choke_size_in": ["Choke Size (inches)", "Choke Size"],
    "remarks": ["Remarks", "Remark", "Comments", "Reason/Comment"],
}

EVENT_COLUMN_ALIASES = {
    "stop_dt": ["Stop date/time", "Stop Date/Time", "Stop Date Time", "Stop date", "Stop Date"],
    "stop_time": ["time (Hrs)", "Time (Hrs)", "time hrs", "time hr"],
    "start_dt": ["Start date/time", "Start Date/Time", "Start Date Time", "Start date", "Start Date"],
    "run_hours": ["Run hours", "Run Hrs", "Running Hours", "Run duration"],
    "shutdown_hours": ["Shutdown hours", "Shutdown Hrs", "Shut down hours", "Shutdown duration (Hrs)", "Shutdown duration"],
    "reason_text": ["Reason/Comment", "Reason", "Comment", "Remarks"],
}

# -----------------------------------------------------------------------------
# Failure labeling config
# -----------------------------------------------------------------------------
FAILURE_LABELS = {
    0: "NORMAL_RUNNING",
    1: "RECOVERABLE_TRIP",
    2: "EXTERNAL_IGNORE",
    3: "MECHANICAL_ELECTRICAL_FAILURE",
}

FAILURE_KEYWORDS = {
    1: [
        "gas lock",
        "underload",
        "gas effect",
        "gas interference",
        "low intake pressure",
        "high motor temp",
        "high intake temp",
        "overload",
        "high mlp",
        "backpressure",
    ],
    2: [
        "dg trip",
        "dg shutdown",
        "power failure",
        "manually shutdown",
        "choke change",
        "redirect flow",
        "planned",
        "routine",
    ],
    3: [
        "p-g",
        "phase to phase",
        "ohm",
        "ground fault",
        "pump stuck",
        "debris",
        "mechanical failure",
        "insulation failure",
        "cable fault",
    ],
}

CONFIRMED_FAILURE_OVERRIDES = [
    {
        "well_name": "R9A#5",
        "event_date": "2023-04-03",
        "force_label": 3,
        "description": "Confirmed phase-ground dead fault event.",
    }
]

# -----------------------------------------------------------------------------
# Thresholds and field parameters
# -----------------------------------------------------------------------------
THRESHOLDS = {
    "motor_temp_trip_c": 150.0,
    "motor_temp_warning_c": 135.0,
    "delta_t_warning_c": 35.0,
    "delta_t_critical_c": 45.0,
    "vfd_min_hz": 40.0,
    "vfd_max_hz": 60.0,
    "intake_temp_normal_min_c": 99.0,
    "intake_temp_normal_max_c": 120.0,
    "phase_imbalance_warning_pct": 5.0,
    "phase_imbalance_critical_pct": 10.0,
    "esp_pressure_min_psi": 100.0,
    "esp_pressure_max_psi": 3000.0,
    "curve_degradation_warning_pct": 15.0,
}

# -----------------------------------------------------------------------------
# OCC config (Step 4)
# -----------------------------------------------------------------------------
OCC_THRESHOLDS = {
    "frequency_delta_hz": 1.0,
    "flowrate_delta_ratio": 0.15,
    "backpressure_delta_bar": 3.0,
}

OCC_STABILIZATION_DAYS = {
    "OCC_CHOKE": 3,
    "OCC_FREQUENCY": 2,
    "OCC_FLOWRATE": 3,
    "OCC_BACKPRESSURE": 1,
    "OCC_RESTART": 2,
    "OCC_INTERVENTION": 7,
    "OCC_PIGGING": 1,
}

OCC_INTERVENTION_KEYWORDS = ["intervention", "choke change", "redirect flow"]
OCC_PIGGING_KEYWORDS = ["pigging", "pig run", "dewatering pig"]

# -----------------------------------------------------------------------------
# Modeling settings (Step 5)
# -----------------------------------------------------------------------------
BASELINE_HEALTHY_DAYS = 60
POST_OCC_REFIT_DAYS = 14
DEGRADATION_SUSTAIN_DAYS = 7

CONFIDENCE_WEIGHTS = {
    "measured": 1.0,
    "virtual_strong": 0.7,
    "virtual_weak": 0.4,
    "unavailable": 0.0,
}

RISK_LEVEL_BANDS = {
    "NORMAL_MIN": 70,
    "WARNING_MIN": 40,
}

# -----------------------------------------------------------------------------
# Database table names
# -----------------------------------------------------------------------------
TABLE_NAMES = {
    "esp_raw": "esp_raw_r9a",
    "esp_events": "esp_events_r9a",
    "esp_health_scores": "esp_health_scores",
}

# -----------------------------------------------------------------------------
# Diagnostics / logging control
# -----------------------------------------------------------------------------
LOGGING = {
    "print_discovered_file": True,
    "print_label_distribution": True,
    "print_row_count_by_well": True,
    "print_date_range": True,
    "print_parse_warnings": True,
    "print_raw_value_on_parse_error": True,
    "max_warning_samples": 20,
}
