import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "ratna_field.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def initialize_database():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oil_production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            platform TEXT NOT NULL,
            well_name TEXT NOT NULL,
            liquid_rate_bpd REAL,
            oil_rate_bpd REAL,
            production_loss_bbl REAL,
            well_status TEXT,
            remarks TEXT,
            UNIQUE(date, well_name)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS water_injection (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            platform TEXT NOT NULL,
            well_name TEXT NOT NULL,
            header_pressure_ksc REAL,
            choke_size TEXT,
            ithp REAL,
            status TEXT,
            flow_rate_sm3hr REAL,
            flow_rate_bpd REAL,
            injecting_hours REAL,
            cumulative_flow_bbl REAL,
            planned_wi_bpd REAL,
            UNIQUE(date, well_name)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS esp_parameters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            well_name TEXT NOT NULL,
            motor_temp_1_c REAL,
            vfd_output_frequency_hz REAL,
            pump_discharge_pressure_psi REAL,
            pump_intake_pressure_psi REAL,
            motor_load_pct REAL,
            motor_current_avg_amp REAL,
            motor_current_a_amp REAL,
            motor_current_b_amp REAL,
            motor_current_c_amp REAL,
            pump_intake_temp_c REAL,
            vibration_x REAL,
            vibration_y REAL,
            quality_flag TEXT,
            UNIQUE(timestamp, well_name)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pressure_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            data_frequency TEXT,
            r7a_r10a_lp REAL,
            r7a_r10a_lt REAL,
            r10a_mlp REAL,
            r10a_mlt REAL,
            r10a_r7a_rp REAL,
            r10a_r9a_rp REAL,
            r10a_r12a_lp REAL,
            r10a_hra_lp REAL,
            r10a_r13a_rp REAL,
            r9a_r10a_lp REAL,
            r9a_r10a_lt REAL,
            r12a_hra_lp REAL,
            r12a_hra_lt REAL,
            r12a_r10a_rp REAL,
            r12a_r10a_rt REAL,
            r12a_r12b_rp REAL,
            r12a_r12b_rt REAL,
            r12b_mlp REAL,
            r12b_mlt REAL,
            r13a_r10a_lp REAL,
            r13a_r10a_lt REAL,
            pigging_remarks TEXT,
            UNIQUE(timestamp)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS well_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            well_name TEXT NOT NULL,
            platform TEXT NOT NULL,
            event_type TEXT,
            cause TEXT,
            remarks TEXT,
            logged_by TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS platform_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            platform TEXT NOT NULL,
            total_liquid_bpd REAL,
            total_oil_bpd REAL,
            total_loss_bbl REAL,
            wells_flowing INTEGER,
            wells_total INTEGER,
            esp_running INTEGER,
            esp_tripped INTEGER,
            mlp REAL,
            mlt REAL,
            diesel_kl REAL,
            UNIQUE(date, platform)
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Database initialized successfully!")
    print(f"📁 Database location: {DB_PATH}")

if __name__ == "__main__":
    initialize_database()