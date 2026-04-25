import warnings
warnings.filterwarnings('ignore')
import logging
logging.getLogger('streamlit').setLevel(logging.ERROR)

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from database import get_connection
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Ratna Field Dashboard",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #1e1e1e;
        border-radius: 10px;
        padding: 15px;
        border-left: 4px solid #00b4d8;
    }
    .alert-red { border-left: 4px solid #e63946; }
    .alert-yellow { border-left: 4px solid #f4a261; }
    .alert-green { border-left: 4px solid #2a9d8f; }
</style>
""", unsafe_allow_html=True)

PLATFORMS = ['R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A']
VALID_PLATFORMS = ['R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A']

# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def format_metric(value, decimals=1):
    """
    Safely format a numeric value for display.

    Problem: When a parameter is missing or sensor is dead,
    Python shows 'nan' which confuses field users.

    Solution: Return 'N/A' for any missing/null/nan value
    instead of showing raw 'nan' string.

    Args:
        value: The numeric value to format
        decimals: Number of decimal places (default 1)

    Returns:
        Formatted string like '146.1' or 'N/A'
    """
    try:
        if value is None:
            return "N/A"
        if isinstance(value, float) and pd.isna(value):
            return "N/A"
        return f"{float(value):.{decimals}f}"
    except:
        return "N/A"

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def load_latest_production():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT * FROM oil_production
        WHERE date = (SELECT MAX(date) FROM oil_production)
    """, conn)
    conn.close()

    # Safety net filter — only valid platforms and well names with '#'
    df = df[df['platform'].isin(VALID_PLATFORMS)]
    df = df[df['well_name'].str.contains('#', na=False)]
    return df

def load_production_trend(days=30):
    conn = get_connection()
    df = pd.read_sql(f"""
        SELECT date,
               SUM(oil_rate_bpd) as total_oil,
               SUM(liquid_rate_bpd) as total_liquid,
               SUM(production_loss_bbl) as total_loss
        FROM oil_production
        WHERE date >= date('now', '-{days} days')
        GROUP BY date
        ORDER BY date
    """, conn)
    conn.close()
    return df

def load_latest_pressure():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT * FROM pressure_data
        ORDER BY timestamp DESC
        LIMIT 1
    """, conn)
    conn.close()
    return df

def load_pressure_trend(days=7):
    conn = get_connection()
    df = pd.read_sql(f"""
        SELECT * FROM pressure_data
        WHERE timestamp >= datetime('now', '-{days} days')
        ORDER BY timestamp ASC
    """, conn)
    conn.close()
    return df

def load_esp_data(well=None):
    conn = get_connection()
    if well:
        df = pd.read_sql("""
            SELECT * FROM esp_parameters
            WHERE well_name = ?
            ORDER BY timestamp ASC
        """, conn, params=(well,))
    else:
        df = pd.read_sql("""
            SELECT * FROM esp_parameters
            ORDER BY timestamp ASC
        """, conn)
    conn.close()
    return df

def load_water_injection():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT * FROM water_injection
        WHERE date = (SELECT MAX(date) FROM water_injection)
        ORDER BY platform, well_name
    """, conn)
    conn.close()
    return df

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

st.sidebar.image(
    "https://upload.wikimedia.org/wikipedia/en/thumb/f/f4/ONGC_Logo.svg/200px-ONGC_Logo.svg.png",
    width=120
)
st.sidebar.title("Ratna Field")
st.sidebar.caption("Offshore Production Dashboard")

page = st.sidebar.radio("Navigation", [
    "🏠 Field Overview",
    "📈 Production Trends",
    "🔧 ESP Health",
    "💧 Water Injection",
    "📊 Pressure Analysis",
    "⚠️ Early Warning"
])

st.sidebar.divider()
st.sidebar.caption(f"Last updated: {datetime.now().strftime('%d-%m-%Y %H:%M')}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — FIELD OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Field Overview":
    st.title("🛢️ Ratna Field — Production Overview")

    prod_df = load_latest_production()
    pressure_df = load_latest_pressure()

    if prod_df.empty:
        st.warning("No production data found. Please run ingestion scripts first.")
        st.stop()

    latest_date = prod_df['date'].max()
    st.caption(f"📅 Data as of: {latest_date}")

    # ── TOP KPI ROW ───────────────────────────────────────────────────────────
    total_oil    = prod_df['oil_rate_bpd'].sum()
    total_liquid = prod_df['liquid_rate_bpd'].sum()
    total_loss   = prod_df['production_loss_bbl'].sum()
    wells_flowing = len(prod_df[prod_df['well_status'].str.contains(
        'Flowing', na=False, case=False)])
    wells_total  = len(prod_df)
    wells_down   = len(prod_df[prod_df['well_status'].str.contains(
        'Non|Workover|Failure', na=False, case=False)])

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("🛢️ Total Oil (BOPD)",       f"{total_oil:,.0f}")
    col2.metric("💧 Total Liquid (BLPD)",    f"{total_liquid:,.0f}")
    col3.metric("📉 Production Loss (BBL)",  f"{total_loss:,.0f}",
                delta=f"-{total_loss:,.0f}", delta_color="inverse")
    col4.metric("✅ Wells Flowing",          f"{wells_flowing} / {wells_total}")
    col5.metric("🔴 Wells Down",             f"{wells_down}",
                delta_color="inverse")

    st.divider()

    # ── PLATFORM SUMMARY TABLE ────────────────────────────────────────────────
    st.subheader("📊 Platform Summary")
    platform_summary = prod_df.groupby('platform').agg(
        Oil_BOPD      = ('oil_rate_bpd',    'sum'),
        Liquid_BLPD   = ('liquid_rate_bpd', 'sum'),
        Loss_BBL      = ('production_loss_bbl', 'sum'),
        Wells_Total   = ('well_name',       'count'),
        Wells_Flowing = ('well_status',
                         lambda x: x.str.contains('Flowing', na=False,
                                                   case=False).sum())
    ).reset_index()

    # Add latest pressure per platform
    if not pressure_df.empty:
        press_row = pressure_df.iloc[0]
        platform_pressure = {
            'R-7A':  press_row.get('r7a_r10a_lp'),
            'R-10A': press_row.get('r10a_mlp'),
            'R-9A':  press_row.get('r9a_r10a_lp'),
            'R-12A': press_row.get('r12a_hra_lp'),
            'R-12B': press_row.get('r12b_mlp'),
            'R-13A': press_row.get('r13a_r10a_lp'),
        }
        platform_summary['Line_Pressure_KSC'] = platform_summary['platform'].map(
            platform_pressure)

    st.dataframe(
        platform_summary.style.format({
            'Oil_BOPD':          '{:,.0f}',
            'Liquid_BLPD':       '{:,.0f}',
            'Loss_BBL':          '{:,.0f}',
            'Line_Pressure_KSC': '{:.1f}'
        }),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # ── CHARTS ROW ────────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🛢️ Oil Contribution by Platform")
        fig = px.pie(
            platform_summary,
            values='Oil_BOPD',
            names='platform',
            color_discrete_sequence=px.colors.sequential.Blues_r
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(
            showlegend=True, height=350,
            paper_bgcolor='rgba(0,0,0,0)', font_color='white'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("⚡ Well Status Distribution")
        status_counts = prod_df['well_status'].fillna('Unknown').value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        color_map = {
            'Flowing':               '#2a9d8f',
            'Non-Flowing':           '#e63946',
            'Intermittent':          '#f4a261',
            'Self Flowing':          '#457b9d',
            'Workover':              '#e9c46a',
            'ESP Downhole failure':  '#c77dff',
            'Non-FLOWING (CD ESP)':  '#e63946',
            'Unknown':               '#6c757d'
        }
        fig2 = px.bar(
            status_counts, x='Status', y='Count',
            color='Status', color_discrete_map=color_map
        )
        fig2.update_layout(
            showlegend=False, height=350,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='white'
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # ── WELL LEVEL DETAIL ─────────────────────────────────────────────────────
    st.subheader("🔍 Well Level Detail")
    platform_filter = st.selectbox("Filter by Platform", ['All'] + PLATFORMS)

    filtered_df = prod_df if platform_filter == 'All' else \
        prod_df[prod_df['platform'] == platform_filter]

    display_cols = ['platform', 'well_name', 'liquid_rate_bpd', 'oil_rate_bpd',
                    'production_loss_bbl', 'well_status', 'remarks']
    available_cols = [c for c in display_cols if c in filtered_df.columns]

    def color_status(val):
        if pd.isna(val): return ''
        val = str(val)
        if 'Non' in val or 'Failure' in val:
            return 'background-color: #e63946; color: white'
        elif 'Intermittent' in val:
            return 'background-color: #f4a261; color: white'
        elif 'Workover' in val:
            return 'background-color: #e9c46a'
        elif 'Flowing' in val:
            return 'background-color: #2a9d8f; color: white'
        return ''

    st.dataframe(
        filtered_df[available_cols].style.map(
            color_status, subset=['well_status']),
        use_container_width=True,
        hide_index=True
    )

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — PRODUCTION TRENDS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📈 Production Trends":
    st.title("📈 Production Trends")

    days = st.slider("Select time range (days)", 7, 365, 30)
    trend_df = load_production_trend(days)

    if trend_df.empty:
        st.warning("No trend data available for selected range.")
        st.stop()

    # Fix date axis — convert to proper datetime and format as date only
    trend_df['date'] = pd.to_datetime(trend_df['date']).dt.normalize()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df['date'], y=trend_df['total_oil'],
        name='Oil (BOPD)',
        line=dict(color='#00b4d8', width=2),
        fill='tozeroy', fillcolor='rgba(0,180,216,0.1)'
    ))
    fig.add_trace(go.Scatter(
        x=trend_df['date'], y=trend_df['total_liquid'],
        name='Liquid (BLPD)',
        line=dict(color='#90e0ef', width=1.5, dash='dash')
    ))
    fig.add_trace(go.Scatter(
        x=trend_df['date'], y=trend_df['total_loss'],
        name='Loss (BBL)',
        line=dict(color='#e63946', width=1.5)
    ))
    fig.update_layout(
        title='Field Production Trend',
        xaxis_title='Date',
        yaxis_title='Barrels',
        height=450,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02),
        # Format x-axis to show dates cleanly
        xaxis=dict(
            tickformat='%d-%b-%Y',
            tickangle=-45
        )
    )
    fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig, use_container_width=True)

    # ── PLATFORM WISE TREND ───────────────────────────────────────────────────
    st.subheader("Platform wise Oil Trend")
    conn = get_connection()
    plat_trend = pd.read_sql(f"""
        SELECT date, platform, SUM(oil_rate_bpd) as oil
        FROM oil_production
        WHERE date >= date('now', '-{days} days')
        AND platform IN ('R-7A','R-9A','R-10A','R-12A','R-12B','R-13A')
        GROUP BY date, platform
        ORDER BY date
    """, conn)
    conn.close()

    if not plat_trend.empty:
        plat_trend['date'] = pd.to_datetime(plat_trend['date']).dt.normalize()
        fig2 = px.line(
            plat_trend, x='date', y='oil',
            color='platform',
            title='Oil Production by Platform'
        )
        fig2.update_layout(
            height=400,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='white',
            xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
        )
        fig2.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        fig2.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — ESP HEALTH
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔧 ESP Health":
    st.title("🔧 ESP Health Monitor")

    esp_df = load_esp_data()

    if esp_df.empty:
        st.warning("No ESP data found. Please add Avalon export files.")
        st.stop()

    wells = sorted(esp_df['well_name'].unique().tolist())
    selected_well = st.selectbox("Select Well", wells)

    well_df = esp_df[esp_df['well_name'] == selected_well].copy()
    well_df['timestamp'] = pd.to_datetime(well_df['timestamp'])
    well_df = well_df.sort_values('timestamp')

    latest = well_df.iloc[-1]

    # ── LATEST READINGS ───────────────────────────────────────────────────────
    st.subheader(f"Latest Reading — {selected_well}")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🌡️ Motor Temp (°C)",
                format_metric(latest.get('motor_temp_1_c')))
    col2.metric("⚡ VFD Frequency (Hz)",
                format_metric(latest.get('vfd_output_frequency_hz')))
    col3.metric("📊 Motor Load (%)",
                format_metric(latest.get('motor_load_pct')))
    col4.metric("🔌 Motor Current (A)",
                format_metric(latest.get('motor_current_avg_amp')))

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("⬆️ Discharge Pressure (psi)",
                format_metric(latest.get('pump_discharge_pressure_psi')))
    col6.metric("⬇️ Intake Pressure (psi)",
                format_metric(latest.get('pump_intake_pressure_psi')))
    col7.metric("🌡️ Intake Temp (°C)",
                format_metric(latest.get('pump_intake_temp_c')))
    col8.metric("📳 Vibration X",
                format_metric(latest.get('vibration_x'), decimals=3))

    st.divider()

    # ── MOTOR TEMPERATURE TREND ───────────────────────────────────────────────
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=well_df['timestamp'], y=well_df['motor_temp_1_c'],
        name='Motor Temp (°C)',
        line=dict(color='#e63946', width=2)
    ))
    fig.add_hline(y=135, line_dash="dash", line_color="orange",
                  annotation_text="Warning: 135°C")
    fig.add_hline(y=150, line_dash="dash", line_color="red",
                  annotation_text="Critical: 150°C (Trip)")
    fig.update_layout(
        title=f'Motor Temperature Trend — {selected_well}',
        xaxis_title='Timestamp',
        yaxis_title='Temperature (°C)',
        height=350,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig, use_container_width=True)

    # ── PHASE CURRENT BALANCE ─────────────────────────────────────────────────
    st.subheader("⚡ Phase Current Balance")
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=well_df['timestamp'], y=well_df['motor_current_a_amp'],
        name='Phase A', line=dict(color='#00b4d8')
    ))
    fig2.add_trace(go.Scatter(
        x=well_df['timestamp'], y=well_df['motor_current_b_amp'],
        name='Phase B', line=dict(color='#f4a261')
    ))
    fig2.add_trace(go.Scatter(
        x=well_df['timestamp'], y=well_df['motor_current_c_amp'],
        name='Phase C', line=dict(color='#2a9d8f')
    ))
    fig2.update_layout(
        title='3-Phase Motor Current',
        xaxis_title='Timestamp',
        yaxis_title='Current (Amps)',
        height=350,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig2.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig2.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig2, use_container_width=True)

    # ── PUMP PRESSURES ────────────────────────────────────────────────────────
    st.subheader("🔄 Pump Intake vs Discharge Pressure")
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(
        x=well_df['timestamp'], y=well_df['pump_intake_pressure_psi'],
        name='Intake Pressure (psi)', line=dict(color='#90e0ef')
    ))
    fig3.add_trace(go.Scatter(
        x=well_df['timestamp'], y=well_df['pump_discharge_pressure_psi'],
        name='Discharge Pressure (psi)', line=dict(color='#0077b6')
    ))
    fig3.update_layout(
        height=350,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig3.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig3.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig3, use_container_width=True)

    # ── VFD FREQUENCY TREND ───────────────────────────────────────────────────
    st.subheader("📡 VFD Output Frequency")
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(
        x=well_df['timestamp'], y=well_df['vfd_output_frequency_hz'],
        name='VFD Frequency (Hz)', line=dict(color='#e9c46a')
    ))
    fig4.add_hline(y=60, line_dash="dash", line_color="red",
                   annotation_text="Max: 60 Hz")
    fig4.add_hline(y=40, line_dash="dash", line_color="orange",
                   annotation_text="Min: 40 Hz")
    fig4.update_layout(
        height=300,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig4.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig4.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig4, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — WATER INJECTION
# ══════════════════════════════════════════════════════════════════════════════
elif page == "💧 Water Injection":
    st.title("💧 Water Injection Summary")

    wi_df = load_water_injection()

    if wi_df.empty:
        st.warning("No water injection data found.")
        st.stop()

    latest_date = wi_df['date'].max()
    st.caption(f"📅 Data as of: {latest_date}")

    total_injected  = wi_df['flow_rate_bpd'].sum()
    total_planned   = wi_df['planned_wi_bpd'].sum()
    total_cumulative = wi_df['cumulative_flow_bbl'].sum()
    wells_injecting = len(wi_df[wi_df['status'] == 'Injection'])

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💉 Total Injection (BPD)",  f"{total_injected:,.0f}")
    col2.metric("🎯 Planned Injection (BPD)", f"{total_planned:,.0f}")
    col3.metric("📊 Cumulative (BBL)",        f"{total_cumulative:,.0f}")
    col4.metric("✅ Wells Injecting",         f"{wells_injecting}")

    st.divider()
    st.subheader("Well Level Injection Data")

    display_cols = ['platform', 'well_name', 'header_pressure_ksc',
                    'choke_size', 'ithp', 'status', 'flow_rate_sm3hr',
                    'flow_rate_bpd', 'injecting_hours',
                    'cumulative_flow_bbl', 'planned_wi_bpd']
    available = [c for c in display_cols if c in wi_df.columns]
    st.dataframe(wi_df[available], use_container_width=True, hide_index=True)

    # ── HISTORICAL WI TREND ───────────────────────────────────────────────────
    st.subheader("📈 Historical Injection Trend")
    days = st.slider("Days to show", 30, 365, 90)
    conn = get_connection()
    wi_trend = pd.read_sql(f"""
        SELECT date,
               SUM(flow_rate_bpd) as total_bpd,
               SUM(cumulative_flow_bbl) as cumulative
        FROM water_injection
        WHERE date >= date('now', '-{days} days')
        GROUP BY date
        ORDER BY date
    """, conn)
    conn.close()

    if not wi_trend.empty:
        wi_trend['date'] = pd.to_datetime(wi_trend['date']).dt.normalize()
        fig = px.line(wi_trend, x='date', y='total_bpd',
                      title='Daily Water Injection Rate (BPD)')
        fig.update_layout(
            height=350,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='white',
            xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
        )
        fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        fig.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — PRESSURE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊 Pressure Analysis":
    st.title("📊 Pipeline Pressure Analysis")

    days = st.slider("Select time range (days)", 7, 180, 30)
    press_df = load_pressure_trend(days)

    if press_df.empty:
        st.warning("No pressure data in selected range.")
        st.stop()

    press_df['timestamp'] = pd.to_datetime(press_df['timestamp'])

    # ── OUTLIER FILTER ────────────────────────────────────────────────────────
    # Remove unrealistic pressure spikes (>200 KSC is physically impossible
    # for these pipelines — anything higher is a sensor error or data entry mistake)
    PRESSURE_COLS = [col for col in press_df.columns
                     if col not in ['id', 'timestamp', 'data_frequency',
                                    'pigging_remarks']]
    for col in PRESSURE_COLS:
        if press_df[col].dtype in ['float64', 'int64']:
            press_df[col] = press_df[col].where(press_df[col] <= 200, other=None)

    # ── ROUTE 1 — R10A TO HEERA ───────────────────────────────────────────────
    st.subheader("🔵 Route 1 — R10A to Heera")
    st.caption("R9A + R13A + R10A crude → Heera Complex via 10\" 45km line")

    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r7a_r10a_lp'],
        name='R7A L/P', line=dict(color='#00b4d8')))
    fig1.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r10a_r9a_rp'],
        name='R9A→R10A R/P', line=dict(color='#90e0ef')))
    fig1.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r10a_hra_lp'],
        name='R10A→HRA L/P', line=dict(color='#0077b6')))
    fig1.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r9a_r10a_lp'],
        name='R9A L/P', line=dict(color='#48cae4')))
    fig1.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r13a_r10a_lp'],
        name='R13A L/P', line=dict(color='#ade8f4')))
    fig1.update_layout(
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        yaxis_title='Pressure (KSC)',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig1.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig1.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig1, use_container_width=True)

    # ── ROUTE 2 — R12A TO HEERA ───────────────────────────────────────────────
    st.subheader("🟠 Route 2 — R12A to Heera")
    st.caption("R7A + R12B + R12A crude → Heera Complex via 12\" 41km + 10\" 41km lines")

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r10a_r12a_lp'],
        name='R10A→R12A L/P (R7A diversion)',
        line=dict(color='#f4a261')))
    fig2.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r12a_r10a_rp'],
        name='R12A R/P from R10A',
        line=dict(color='#e76f51')))
    fig2.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r12a_r12b_rp'],
        name='R12A R/P from R12B',
        line=dict(color='#e9c46a')))
    fig2.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r12a_hra_lp'],
        name='R12A→HRA L/P',
        line=dict(color='#264653')))
    fig2.update_layout(
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        yaxis_title='Pressure (KSC)',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig2.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig2.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig2, use_container_width=True)

    # ── DIFFERENTIAL PRESSURE ANALYSIS ───────────────────────────────────────
    st.subheader("⚠️ Pipeline ΔP Analysis")
    st.caption("ΔP = Launcher Pressure - Receiver Pressure. Rising ΔP = pipeline restriction building up")

    press_df['r7a_dp']  = press_df['r7a_r10a_lp']  - press_df['r10a_r7a_rp']
    press_df['r9a_dp']  = press_df['r9a_r10a_lp']  - press_df['r10a_r9a_rp']
    press_df['r13a_dp'] = press_df['r13a_r10a_lp'] - press_df['r10a_r13a_rp']

    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r7a_dp'],
        name='R7A ΔP', line=dict(color='#00b4d8')))
    fig3.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r9a_dp'],
        name='R9A ΔP', line=dict(color='#f4a261')))
    fig3.add_trace(go.Scatter(
        x=press_df['timestamp'], y=press_df['r13a_dp'],
        name='R13A ΔP', line=dict(color='#2a9d8f')))
    fig3.update_layout(
        title='Pipeline Differential Pressure (L/P - R/P)',
        height=350,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        yaxis_title='ΔP (KSC)',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig3.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig3.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig3, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — EARLY WARNING
# ══════════════════════════════════════════════════════════════════════════════
elif page == "⚠️ Early Warning":
    st.title("⚠️ Early Warning System")
    st.caption("Automated alerts based on threshold monitoring")

    alerts = []

    # ── ESP CHECKS ────────────────────────────────────────────────────────────
    esp_df = load_esp_data()
    if not esp_df.empty:
        latest_esp = esp_df.sort_values('timestamp').groupby(
            'well_name').last().reset_index()

        for _, row in latest_esp.iterrows():

            # Motor temperature check
            temp = row.get('motor_temp_1_c')
            intake_temp = row.get('pump_intake_temp_c')
            if temp and not pd.isna(temp):
                if temp > 150:
                    alerts.append({
                        'Level':     '🔴 CRITICAL',
                        'Well':      row['well_name'],
                        'Parameter': 'Motor Temperature',
                        'Value':     f"{temp:.1f} °C",
                        'Threshold': '>150°C (Trip point)',
                        'Action':    'Check for gas lock or overload immediately'
                    })
                elif temp > 135:
                    alerts.append({
                        'Level':     '🟡 WARNING',
                        'Well':      row['well_name'],
                        'Parameter': 'Motor Temperature',
                        'Value':     f"{temp:.1f} °C",
                        'Threshold': '>135°C warning',
                        'Action':    'Monitor closely, check intake pressure'
                    })

            # Delta T check (motor temp - intake temp)
            if temp and intake_temp and not pd.isna(temp) and not pd.isna(intake_temp):
                delta_t = temp - intake_temp
                if delta_t > 45:
                    alerts.append({
                        'Level':     '🔴 CRITICAL',
                        'Well':      row['well_name'],
                        'Parameter': 'Motor ΔT (Motor-Intake)',
                        'Value':     f"{delta_t:.1f} °C",
                        'Threshold': '>45°C ΔT critical',
                        'Action':    'Possible gas lock or scale on motor — investigate'
                    })
                elif delta_t > 35:
                    alerts.append({
                        'Level':     '🟡 WARNING',
                        'Well':      row['well_name'],
                        'Parameter': 'Motor ΔT (Motor-Intake)',
                        'Value':     f"{delta_t:.1f} °C",
                        'Threshold': '>35°C ΔT warning',
                        'Action':    'Monitor trend — motor running hotter than normal'
                    })

            # Phase current imbalance check
            ca = row.get('motor_current_a_amp')
            cb = row.get('motor_current_b_amp')
            cc = row.get('motor_current_c_amp')
            if (ca and cb and cc and
                    not pd.isna(ca) and not pd.isna(cb) and not pd.isna(cc)):
                avg = (ca + cb + cc) / 3
                if avg > 0:
                    imbalance = max(
                        abs(ca-avg), abs(cb-avg), abs(cc-avg)
                    ) / avg * 100
                    if imbalance > 10:
                        alerts.append({
                            'Level':     '🔴 CRITICAL',
                            'Well':      row['well_name'],
                            'Parameter': 'Phase Current Imbalance',
                            'Value':     f"{imbalance:.1f}%",
                            'Threshold': '>10% imbalance',
                            'Action':    'Possible cable degradation — plan megger test'
                        })
                    elif imbalance > 5:
                        alerts.append({
                            'Level':     '🟡 WARNING',
                            'Well':      row['well_name'],
                            'Parameter': 'Phase Current Imbalance',
                            'Value':     f"{imbalance:.1f}%",
                            'Threshold': '>5% imbalance',
                            'Action':    'Monitor — early cable integrity concern'
                        })

            # VFD frequency check
            freq = row.get('vfd_output_frequency_hz')
            if freq and not pd.isna(freq):
                if freq > 60:
                    alerts.append({
                        'Level':     '🔴 CRITICAL',
                        'Well':      row['well_name'],
                        'Parameter': 'VFD Frequency',
                        'Value':     f"{freq:.1f} Hz",
                        'Threshold': '>60 Hz max',
                        'Action':    'Reduce frequency immediately — above design limit'
                    })
                elif freq < 40:
                    alerts.append({
                        'Level':     '🟡 WARNING',
                        'Well':      row['well_name'],
                        'Parameter': 'VFD Frequency',
                        'Value':     f"{freq:.1f} Hz",
                        'Threshold': '<40 Hz min',
                        'Action':    'Check inflow — possible low reservoir pressure'
                    })

    # ── PRESSURE TREND CHECKS ─────────────────────────────────────────────────
    press_df = load_pressure_trend(days=3)
    if not press_df.empty:
        press_df['timestamp'] = pd.to_datetime(press_df['timestamp'])

        # Filter outliers
        for col in press_df.select_dtypes(include='number').columns:
            press_df[col] = press_df[col].where(press_df[col] <= 200, other=None)

        pressure_checks = [
            ('r7a_r10a_lp',  'R7A Launcher Pressure'),
            ('r9a_r10a_lp',  'R9A Launcher Pressure'),
            ('r13a_r10a_lp', 'R13A Launcher Pressure'),
            ('r10a_hra_lp',  'R10A→HRA Launcher Pressure'),
            ('r12a_hra_lp',  'R12A→HRA Launcher Pressure'),
        ]

        for col, label in pressure_checks:
            if col in press_df.columns:
                recent = press_df[col].dropna()
                if len(recent) >= 4:
                    trend = recent.iloc[-1] - recent.iloc[-4]
                    if trend > 5:
                        alerts.append({
                            'Level':     '🔴 CRITICAL',
                            'Well':      'Field',
                            'Parameter': f'{label} Rising Fast',
                            'Value':     f"+{trend:.1f} KSC in last 3 readings",
                            'Threshold': '>5 KSC rise',
                            'Action':    'Back pressure building — check for wax/hydrate or pigging required'
                        })
                    elif trend > 3:
                        alerts.append({
                            'Level':     '🟡 WARNING',
                            'Well':      'Field',
                            'Parameter': f'{label} Rising',
                            'Value':     f"+{trend:.1f} KSC in last 3 readings",
                            'Threshold': '>3 KSC rise',
                            'Action':    'Monitor back pressure trend closely'
                        })

    # ── DISPLAY ALERTS ────────────────────────────────────────────────────────
    if alerts:
        alert_df = pd.DataFrame(alerts)
        # Show critical first
        alert_df['sort'] = alert_df['Level'].apply(
            lambda x: 0 if 'CRITICAL' in x else 1)
        alert_df = alert_df.sort_values('sort').drop('sort', axis=1)
        st.dataframe(alert_df, use_container_width=True, hide_index=True)
        critical = len([a for a in alerts if 'CRITICAL' in a['Level']])
        warning  = len([a for a in alerts if 'WARNING' in a['Level']])
        if critical > 0:
            st.error(f"🔴 {critical} CRITICAL alert(s) — immediate action required!")
        if warning > 0:
            st.warning(f"🟡 {warning} WARNING alert(s) — monitor closely")
    else:
        st.success("✅ All parameters within normal range. No active alerts.")

    st.divider()

    # ── THRESHOLD REFERENCE ───────────────────────────────────────────────────
    st.subheader("⚙️ Alert Thresholds — Ratna Field Specific")
    st.info("""
    **ESP Motor Temperature (Trip set at 150°C):**
    - Normal: Intake Temp + 20-30°C above intake fluid temperature
    - Warning: Motor Temp >135°C OR ΔT >35°C above intake temp
    - Critical: Motor Temp >150°C OR ΔT >45°C above intake temp

    **Phase Current Imbalance:**
    - Warning: >5% imbalance between phases
    - Critical: >10% imbalance — possible cable degradation

    **VFD Operating Frequency (Ratna Field range: 40-60 Hz):**
    - Warning: <40 Hz (low inflow) or >60 Hz (above design)

    **Pipeline Launcher Pressure Rising:**
    - Warning: >3 KSC increase in 3 consecutive readings
    - Critical: >5 KSC increase in 3 consecutive readings

    *Thresholds calibrated for Ratna Field offshore conditions*
    """)