import pandas as pd
import numpy as np
import os
from database import get_connection

RAW_FOLDER = os.path.join(os.path.dirname(__file__), "data", "raw")
FREQUENCY_CHANGE_DATE = pd.Timestamp('2021-02-04')

def find_pressure_file():
    for f in os.listdir(RAW_FOLDER):
        if "pressure" in f.lower():
            return os.path.join(RAW_FOLDER, f)
    return None

def clean_header(val):
    if pd.isna(val):
        return ''
    return str(val).replace('\n', ' ').replace('  ', ' ').strip()

def ingest_pressure():
    filepath = find_pressure_file()
    if not filepath:
        print("❌ No pressure file found in data/raw folder!")
        return

    print(f"📂 Reading pressure file: {os.path.basename(filepath)}")
    df_raw = pd.read_excel(filepath, sheet_name=0, header=None)
    print(f"   Raw shape: {df_raw.shape}")

    platform_row = df_raw.iloc[0, :]
    param_row = df_raw.iloc[1, :]

    col_names = []
    current_platform = ''
    for i in range(df_raw.shape[1]):
        plat = clean_header(platform_row.iloc[i])
        param = clean_header(param_row.iloc[i])
        if plat and plat not in ['Date17/07/2024', 'NaN', 'Time']:
            current_platform = plat
        if i == 0:
            col_names.append('date')
        elif i == 1:
            col_names.append('time')
        else:
            combined = f"{current_platform}|{param}" if param else f"{current_platform}|col_{i}"
            col_names.append(combined)

    df = df_raw.iloc[2:, :].copy()
    df.columns = col_names
    df = df.reset_index(drop=True)

    df['date'] = df['date'].replace('', np.nan)

    def parse_date_smart(val):
        """
        Smart date parser that handles mixed formats:
        - datetime objects from Excel (already correct)
        - YYYY-MM-DD strings
        - D/M/YYYY or DD/MM/YYYY strings
        - Strings with backtick/quote prefix errors
        """
        if pd.isna(val):
            return pd.NaT

        # Already a datetime — return as-is
        if isinstance(val, pd.Timestamp) or hasattr(val, 'year'):
            return pd.Timestamp(val)

        # Clean string — remove backticks, quotes, spaces
        val_str = str(val).strip().replace('`', '').replace("'", '').strip()

        if not val_str or val_str == 'nan':
            return pd.NaT

        # Try YYYY-MM-DD format first (already correct)
        try:
            if '-' in val_str and len(val_str) >= 10:
                return pd.to_datetime(val_str, format='%Y-%m-%d')
        except:
            pass

        # Try D/M/YYYY or DD/MM/YYYY (dayfirst)
        try:
            if '/' in val_str:
                return pd.to_datetime(val_str, dayfirst=True)
        except:
            pass

        # Try DD.MM.YYYY
        try:
            if '.' in val_str:
                return pd.to_datetime(val_str, dayfirst=True)
        except:
            pass

        # Last resort
        try:
            return pd.to_datetime(val_str, dayfirst=True, errors='coerce')
        except:
            return pd.NaT

    # Apply smart parser to each date value
    df['date'] = df['date'].apply(parse_date_smart)
    df['date'] = df['date'].ffill()

    df['time'] = df['time'].astype(str).str.strip()

    df['timestamp'] = pd.to_datetime(
        df['date'].dt.strftime('%Y-%m-%d') + ' ' + df['time'],
        errors='coerce'
    )

    df = df.dropna(subset=['timestamp'])
    print(f"   Valid timestamps: {len(df)}")
    print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    df['data_frequency'] = df['timestamp'].apply(
        lambda x: 'hourly' if x < FREQUENCY_CHANGE_DATE else '6hourly'
    )
    print(f"   Hourly records: {(df['data_frequency']=='hourly').sum()}")
    print(f"   6-hourly records: {(df['data_frequency']=='6hourly').sum()}")

    col_map = {
        'r7a_r10a_lp':  'R-7A|MLP',
        'r7a_r10a_lt':  'R-7A|MLT',
        'r10a_mlp':     'R-10A|MLP',
        'r10a_mlt':     'R-10A|MLT',
        'r10a_r7a_rp':  'R-10A|R7A -R10A R/P',
        'r10a_r9a_rp':  'R-10A|R9A -R10A R/P',
        'r10a_r12a_lp': 'R-10A|R10A -R12A L/ P (R-7A to R-12A )',
        'r10a_hra_lp':  'R-10A|R10A -HRA L/ P (R-9A, R-13A & R-10A to HRA )',
        'r10a_r13a_rp': 'R-10A|R13A-R10A R/P',
        'r9a_r10a_lp':  'R-9A|R9A-R10A L/P',
        'r9a_r10a_lt':  'R-9A|R9A-R10A L/T',
        'r12a_hra_lp':  'R12A|R12A-HRA L/ P',
        'r12a_hra_lt':  'R12A|R12A-HRA L/T',
        'r12a_r10a_rp': 'R12A|R10A-R12A R/P',
        'r12a_r10a_rt': 'R12A|R10-R12A R/ T',
        'r12a_r12b_rp': 'R12A|R12B-R12A R/P',
        'r12a_r12b_rt': 'R12A|R12B-R12A R/ T',
        'r12b_mlp':     'R-12B|MLP',
        'r12b_mlt':     'R-12B|MLT',
        'r13a_r10a_lp': 'R 13A|R13A-R10A L/P',
        'r13a_r10a_lt': 'R 13A|R13A-R10A L/T',
        'pigging_remarks': 'R 13A|Pigging details/Remarks',
    }

    print(f"\n   Column mapping results:")
    for k, v in col_map.items():
        status = "✅" if v else "⚠️  (not in file)"
        print(f"   {status} {k} → {v}")

    conn = get_connection()
    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        def get_val(col):
            if col and col in df.columns:
                val = row.get(col)
                result = pd.to_numeric(val, errors='coerce')
                return None if pd.isna(result) else float(result)
            return None

        try:
            pigging = str(row.get(col_map['pigging_remarks'], '')) if col_map['pigging_remarks'] in df.columns else None
            if pigging in ['nan', 'None', '']:
                pigging = None

            conn.execute("""
                INSERT OR IGNORE INTO pressure_data
                (timestamp, data_frequency,
                r7a_r10a_lp, r7a_r10a_lt,
                r10a_mlp, r10a_mlt,
                r10a_r7a_rp, r10a_r9a_rp,
                r10a_r12a_lp, r10a_hra_lp, r10a_r13a_rp,
                r9a_r10a_lp, r9a_r10a_lt,
                r12a_hra_lp, r12a_hra_lt,
                r12a_r10a_rp, r12a_r10a_rt,
                r12a_r12b_rp, r12a_r12b_rt,
                r12b_mlp, r12b_mlt,
                r13a_r10a_lp, r13a_r10a_lt,
                pigging_remarks)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                str(row['timestamp']),
                row['data_frequency'],
                get_val(col_map['r7a_r10a_lp']),
                get_val(col_map['r7a_r10a_lt']),
                get_val(col_map['r10a_mlp']),
                get_val(col_map['r10a_mlt']),
                get_val(col_map['r10a_r7a_rp']),
                get_val(col_map['r10a_r9a_rp']),
                get_val(col_map['r10a_r12a_lp']),
                get_val(col_map['r10a_hra_lp']),
                get_val(col_map['r10a_r13a_rp']),
                get_val(col_map['r9a_r10a_lp']),
                get_val(col_map['r9a_r10a_lt']),
                get_val(col_map['r12a_hra_lp']),
                get_val(col_map['r12a_hra_lt']),
                get_val(col_map['r12a_r10a_rp']),
                get_val(col_map['r12a_r10a_rt']),
                get_val(col_map['r12a_r12b_rp']),
                get_val(col_map['r12a_r12b_rt']),
                get_val(col_map['r12b_mlp']),
                get_val(col_map['r12b_mlt']),
                get_val(col_map['r13a_r10a_lp']),
                get_val(col_map['r13a_r10a_lt']),
                pigging,
            ))
            inserted += 1
        except Exception as e:
            skipped += 1

    conn.commit()
    conn.close()

    print(f"\n✅ Pressure ingestion complete!")
    print(f"   Records inserted: {inserted}")
    print(f"   Records skipped: {skipped}")

if __name__ == "__main__":
    ingest_pressure()