# Ratna Field Dashboard

A real-time operations and predictive-maintenance dashboard for the ONGC Ratna offshore oil field (R9A platform). The project has two distinct layers that are designed to eventually work together: a **live operations dashboard** built in Streamlit, and an **ESP failure prediction pipeline** built in Python/scikit-learn.

---

## Project Structure

```
Ratna-field-Dashboard/
│
├── app.py                          # Streamlit dashboard (6-page UI)
├── database.py                     # SQLite schema + connection helper
│
├── ingest_production.py            # Ingests daily production DPR Excel files
├── ingest_pressure.py              # Ingests pipeline pressure Excel file
├── ingest_avalon.py                # Ingests ESP sensor data (Avalon SCADA format)
│
├── data/
│   └── raw/                        # Source Excel files (not committed)
│
├── esp_prediction/                 # ESP failure prediction sub-package
│   ├── config.py                   # All settings, thresholds, column aliases
│   ├── data_ingestion.py           # Parses R9A ESP workbook → SQLite
│   ├── virtual_sensor.py           # Imputes missing sensor channels
│   ├── feature_engineering.py      # Builds daily feature vectors from raw readings
│   ├── occ_detector.py             # Detects Operational Condition Changes
│   ├── model_trainer.py            # Trains IsolationForest + RandomForest
│   ├── health_scorer.py            # Combines model outputs → 0-100 health score
│   ├── predictor.py                # Orchestrates scoring + writes results to DB
│   └── main.py                     # Entry point: run full pipeline end-to-end
│
├── offshore_pics.jpg               # Dashboard background image
├── ongc_logo.jpg                   # Navbar logo
├── requirements.txt                # Python dependencies
└── .gitignore
```

---

## Layer 1 — Live Operations Dashboard (`app.py`)

### Why it was created
The field operations team needed a single screen to monitor daily production, ESP sensor health, water injection performance, and pipeline pressures across all six Ratna platforms (R-7A, R-9A, R-10A, R-12A, R-12B, R-13A) without having to open multiple Excel files. `app.py` is a Streamlit single-page application that serves all of this from a local SQLite database.

### Pages

| Tab | Content |
|-----|---------|
| **Field Overview** | KPI cards (total liquid, oil, net oil, production loss, wells flowing/down), platform summary table, oil-by-platform pie chart, well status bar chart, per-well drill-down |
| **Production Trends** | Time-series of field-wide oil/liquid/loss with selectable date range; per-platform trend lines |
| **ESP Health** | Per-well motor temperature, 3-phase current balance, pump intake/discharge pressure, VFD frequency, motor load — all resampled to 12-hour averages for readability |
| **Water Injection** | Latest snapshot of all injection wells (rate, pressure, choke, cumulative), plus historical rate trend |
| **Pressure Analysis** | Pipeline pressure along Route 1 (R9A → R10A → Heera) and Route 2 (R12A → Heera), plus differential pressure (ΔP) chart as an early wax/hydrate indicator |
| **Early Warning** | Rule-based alerts: motor temperature (>135°C warning, >150°C critical), motor ΔT, phase imbalance, VFD frequency limits, and rising pipeline pressure |

### How it connects to other files
- `app.py` calls `database.get_connection()` for every data query — it never touches the Excel files directly.
- The three ingestion scripts (`ingest_production.py`, `ingest_pressure.py`, `ingest_avalon.py`) must be run manually (or scheduled) to populate the database before the dashboard can display data.
- The **ESP Health** page currently reads from the `esp_parameters` table that `ingest_avalon.py` populates (Avalon SCADA format). It does **not yet** read from the ESP prediction pipeline's output tables.

---

## Layer 2 — ESP Failure Prediction Pipeline (`esp_prediction/`)

### Why it was created
The Avalon SCADA export covers all platforms but is relatively coarse (hourly averages, limited sensor channels). The ONGC R9A workbook (a detailed Excel file maintained by the Novomet ESP service team) contains much richer data — 6-hourly readings of intake pressure, discharge pressure, motor temperature, vibration, phase currents, and a Start/Stop counter log with failure reasons for every trip. This richer dataset makes it possible to train a machine-learning model that gives an early-warning health score rather than just hard-threshold alarms.

The pipeline covers wells **R9A#1, R9A#2, R9A#4, and R9A#5**.

### Step-by-step flow

```
R9A Excel workbook
        │
        ▼
[Step 1] config.py
  All settings, thresholds, column name aliases,
  sheet-matching patterns, failure keyword lists,
  and confirmed historical override labels live here.
  No other file contains hardcoded operational values.
        │
        ▼
[Step 2] data_ingestion.py  ──────────────────────────────────────────────────
  find_r9a_file()           — locates the workbook on disk
  _resolve_sheet_for_well() — score-based sheet-name matching
  _detect_header_row()      — finds the real header among config rows
  _load_esp_sheet()         — reads 6-hourly sensor readings, forward-fills
                              dates, parses triplet columns (Vx/Vy/Vz,
                              IA/IB/IC, A/B/C pressure), handles fractions
                              in choke size
  _load_event_sheet()       — reads Start/Stop counter sheet; handles
                              datetime objects + "1158 hrs" time strings;
                              classifies each stop reason as label 0-3
  run_ingestion()           — iterates all 4 wells, concatenates frames,
                              writes esp_raw_r9a and esp_events_r9a to SQLite
        │
        ▼
[Step 3] virtual_sensor.py
  For every well, fits a pump performance constant (k_pump) from the first
  60 days of healthy operation and a reference ΔT baseline.
  Uses these to impute discharge-pressure estimates when Pd sensor is
  missing, and flags each channel as measured / virtual_strong /
  virtual_weak / unavailable.
  Confidence weights from config.py modulate how much each imputed
  channel contributes to the final health score.
        │
        ▼
[Step 4] feature_engineering.py
  Aggregates raw (or imputed) readings to one row per well per day.
  Computes:
    delta_T          — motor temp minus intake temp
    dp               — discharge minus intake pressure
    phase_imbalance  — % deviation of worst phase current from average
    vib_total        — √(Vx²+Vy²+Vz²) resultant vibration
    load_per_hz      — motor load normalised to VFD frequency
    pi_slope_7d      — 7-day linear trend in intake pressure
    tm_slope_7d      — 7-day linear trend in motor temperature
    dp_slope_7d      — 7-day linear trend in differential pressure
    trips_30d        — rolling 30-day trip count from events table
    k_pump           — pump performance constant (from virtual_sensor)
    k_degradation    — % deviation of current k_pump from baseline
  Also joins the events table to attach a failure_label (0-3) to each day.

        │
        ▼
[Step 4b] occ_detector.py
  Scans the daily feature table for Operational Condition Changes
  (choke change, frequency step, flowrate jump, backpressure change,
  restart after a stop event, intervention or pigging remarks).
  Marks the N days after each OCC as occ_active=True using
  well-specific restart dates (not a global cross-well set).
  The predictor suppresses the health score during these windows
  and shows "RECALIBRATING" instead — avoiding false alarms when
  operating conditions intentionally change.
        │
        ▼
[Step 5] model_trainer.py
  Trains two models on the labelled feature table:
    IsolationForest   — learns the boundary of normal operation from
                        healthy days (label=0); produces an anomaly score
    RandomForestClassifier — binary classifier (label 0 = no failure,
                        labels 1/3 = failure); produces a failure probability
  Saves both models as .pkl files in esp_prediction/models/.
        │
        ▼
[Step 6] health_scorer.py
  Combines the two model outputs into a single 0-100 health score:
    isolation component = (anomaly_score + 1) / 2 × 50
    rf component        = (1 - failure_probability) × 50
    health_score        = sum, clipped to [0, 100]
  Maps the score to a risk band: NORMAL (≥70) / WARNING (≥40) / CRITICAL (<40).
        │
        ▼
[Step 7] predictor.py
  Loads raw tables from SQLite, re-runs virtual sensors and feature
  engineering, applies OCC detection, calls health_scorer, suppresses
  health_score during OCC windows (sets to None / "RECALIBRATING"),
  and writes the final scored table (esp_health_scores) back to SQLite.
        │
        ▼
[Entry point] main.py
  Calls all steps in order: ingest → virtual sensors → features →
  train → predict. Run with:
    python -m esp_prediction.main
```

### Failure labeling scheme

| Label | Meaning | How assigned |
|-------|---------|-------------|
| 0 | Normal running / planned shutdown | Default if no keyword matches |
| 1 | Recoverable trip (gas lock, overload, backpressure) | Keyword match in reason text |
| 2 | External / ignore (DG trip, power failure, planned maintenance) | Keyword match in reason text |
| 3 | Mechanical / electrical failure (phase-ground fault, stuck pump, cable fault) | Keyword match; or `CONFIRMED_FAILURE_OVERRIDES` in config for known events |

The R9A#5 phase-ground dead fault on **3 April 2023** is a confirmed label-3 event. After all fixes, the model correctly scores this day as **WARNING** with `failure_probability ≈ 0.48`, and fires an early warning signal **21 days before** the actual failure.

---

## Supporting Files

### `database.py`
Defines the SQLite schema (six tables) and provides `get_connection()` used by every other file. Created as a single source of truth for table definitions so the ingestion scripts and the dashboard always share the same schema. Tables:

| Table | Populated by | Read by |
|-------|-------------|---------|
| `oil_production` | `ingest_production.py` | `app.py` pages 1, 2, 6 |
| `water_injection` | *(manual or future script)* | `app.py` page 4 |
| `esp_parameters` | `ingest_avalon.py` | `app.py` pages 3, 6 |
| `pressure_data` | `ingest_pressure.py` | `app.py` pages 5, 6 |
| `esp_raw_r9a` | `esp_prediction/data_ingestion.py` | `esp_prediction/predictor.py` |
| `esp_events_r9a` | `esp_prediction/data_ingestion.py` | `esp_prediction/predictor.py` |
| `esp_health_scores` | `esp_prediction/predictor.py` | *(future: `app.py` ESP Failure tab)* |

### `ingest_production.py`
Walks a folder tree of daily DPR (Daily Production Report) Excel files, finds the 0600-hrs snapshot for each date, extracts well-level liquid rate, oil rate, production loss, and well status, and upserts into `oil_production`. The folder path is configurable at the top of the file. Created because the DPR files arrive as one Excel per day in yearly subfolders and needed a smart traversal rather than a single file read.

### `ingest_pressure.py`
Reads a single Excel workbook of pipeline pressure readings (launcher pressures, manifold pressures, receive pressures across both export routes) and loads them into `pressure_data`. Handles a frequency change that occurred on 4 February 2021 (data went from daily to higher frequency after that date).

### `ingest_avalon.py`
Reads the Avalon SCADA ESP sensor export (a different format from the Novomet R9A workbook). Contains a detailed configuration block with physical-limit validation, frozen-sensor detection, and stuck-value detection before writing to `esp_parameters`. Created specifically because Avalon exports contain sensor noise and hardware faults that must be filtered before display in the dashboard.

### `esp_prediction/config.py`
The only file that should be edited when operational parameters change. Contains: well names, pump catalog, sheet-matching patterns, column aliases (handles variations in column header spelling across workbook versions), failure keywords, confirmed historical overrides, OCC thresholds and stabilization windows, and model hyperparameters. All other `esp_prediction/` modules import from here and contain no hardcoded values.

---

## Data Flow Diagram

```
                    Excel Files (data/raw/)
                           │
          ┌────────────────┼────────────────────┐
          │                │                    │
  ingest_production   ingest_pressure     ingest_avalon
          │                │                    │
          └────────────────┴────────────────────┘
                           │
                      database.py
                    (ratna_field.db)
                     ┌─────────┐
                     │ SQLite  │◄──── esp_prediction/
                     └────┬────┘      data_ingestion.py
                          │                  │
                      app.py            predictor.py
                   (Dashboard)        (writes scores back)
                          │
              ┌───────────┴──────────────────┐
       6 dashboard pages              esp_health_scores table
              │                             (not yet wired)
       Pages 1-6 live                  [Future: ESP Failure tab]
```

---

## Future Work — Connecting the Dashboard to ESP Failure Prediction

The **ESP Failure** tab is the primary pending integration. Here is the planned workflow:

### Step 1 — Run the prediction pipeline on a schedule
`python -m esp_prediction.main` is currently run manually. It needs to be triggered automatically whenever new R9A Excel data arrives — either via a cron job, a file-watcher, or a button in the dashboard sidebar that calls `run_pipeline()` directly.

### Step 2 — Add an "ESP Failure" page to `app.py`
A new entry in the `PAGES` list:
```python
("🤖", "ESP Failure")
```
This page reads from `esp_health_scores` (already written to SQLite by `predictor.py`) via `database.get_connection()`, exactly like every other page already does.

### Step 3 — Page layout (proposed)
The new page would contain:

1. **Well health scorecard** — one card per well (R9A#1, #2, #4, #5) showing current health score, risk badge (NORMAL / WARNING / CRITICAL / RECALIBRATING), and failure probability. Colour-coded: green / amber / red.

2. **Health score trend chart** — Plotly line chart of `health_score` over time per well, with a shaded band marking OCC/RECALIBRATING windows in grey and known failure events as red vertical markers.

3. **Feature breakdown table** — For the selected well, a date-range table showing `delta_T`, `dp`, `phase_imbalance`, `vib_total`, `trips_30d`, `k_degradation_pct` so the engineer can see which parameter is driving the score.

4. **Stop/start event log** — Table from `esp_events_r9a` showing the history of trips with their classified failure labels, so context for any WARNING/CRITICAL score is immediately visible.

5. **Early warning banner** — If any well has `risk_level = 'WARNING'` or `'CRITICAL'` and `occ_active = 0`, surface a prominent banner at the top of the page similar to the existing Early Warning page.

### Step 4 — Align the two ESP data sources
Currently there are two separate ESP data paths:
- `esp_parameters` (from Avalon SCADA, used by the ESP Health page)
- `esp_raw_r9a` (from the Novomet R9A workbook, used by the prediction pipeline)

These cover overlapping wells with different column schemas. A future cleanup step would either merge them into a single normalised table or establish a clear rule for which source to prefer per parameter (the R9A workbook has higher-resolution downhole data; Avalon has broader platform coverage).

### Step 5 — Model retraining trigger
When new confirmed failure events are added to `CONFIRMED_FAILURE_OVERRIDES` in `config.py`, the models need retraining. A simple button on the ESP Failure page — "Retrain models with latest data" — would call `train_models()` and save updated `.pkl` files, keeping the prediction current without needing a developer to run CLI commands.

---

## Running the Project

```bash
# 1. Install dependencies
pip install -r requirements.txt
pip install numpy scikit-learn python-dateutil openpyxl

# 2. Initialise the database (creates ratna_field.db)
python database.py

# 3. Populate the database with historical data
python ingest_production.py
python ingest_pressure.py
python ingest_avalon.py

# 4. Run the ESP failure prediction pipeline
python -m esp_prediction.main

# 5. Launch the dashboard
streamlit run app.py
```

---

## Key Design Decisions

**Why SQLite?** The field operates on-premise without a server. SQLite is a single file, zero-configuration, and survives network outages. The dashboard and ingestion scripts all run on the same machine.

**Why two model ensemble (IsolationForest + RandomForest)?** IsolationForest catches anomalies even when no labelled failures exist in training (unsupervised). RandomForest uses the labelled trip history to push the score lower for patterns that historically led to failures. Combining both makes the score robust to wells with few labelled events.

**Why OCC suppression?** A choke change or a pump restart legitimately shifts all operating parameters. Without suppression, every planned intervention would trigger a false CRITICAL. The RECALIBRATING window (2-7 days depending on change type) gives the model time to establish a new baseline before resuming scoring.

**Why a separate `config.py`?** Operational parameters (keyword lists, thresholds, well names, column aliases) change as the workbook format evolves or new wells are added. Keeping them in one place means a field engineer can update settings without touching any logic code.
