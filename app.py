import warnings
warnings.filterwarnings('ignore')
import logging
logging.getLogger('streamlit').setLevel(logging.ERROR)

import base64
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from database import get_connection
from datetime import datetime

st.set_page_config(
    page_title="Oil Field Dashboard",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── BACKGROUND IMAGE ──────────────────────────────────────────────────────────
def get_base64_image(image_path):
    """
    Convert local image to base64 string.
    Embeds image directly in CSS — works locally and on Streamlit Cloud.
    """
    try:
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    except FileNotFoundError:
        return None

bg_image = get_base64_image("offshore_pics.jpg")

# Build background CSS based on whether image loaded successfully
if bg_image:
    bg_css = f"""
        background-image:
            linear-gradient(
                rgba(0, 0, 0, 0.60),
                rgba(0, 10, 30, 0.70)
            ),
            url("data:image/jpeg;base64,{bg_image}");
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
    """
    bg_css_light = f"""
        background-image:
            linear-gradient(
                rgba(255, 255, 255, 0.65),
                rgba(220, 240, 255, 0.70)
            ),
            url("data:image/jpeg;base64,{bg_image}");
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
    """
else:
    # Fallback if image not found
    bg_css       = "background: linear-gradient(135deg, #000a1e, #001a3a);"
    bg_css_light = "background: linear-gradient(135deg, #dce8f5, #f0f8ff);"

# ── STYLES ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=Inter:wght@300;400;500&display=swap');

    /* ── BACKGROUND ── */
    .stApp {{
        {bg_css}
    }}

    /* ── SIDEBAR ── */
    [data-testid="stSidebar"] {{
        background: rgba(0, 10, 30, 0.85) !important;
        backdrop-filter: blur(12px);
        border-right: 1px solid rgba(0, 180, 216, 0.2);
    }}

    /* ── METRIC CARDS — Glassmorphism ── */
    [data-testid="stMetric"] {{
        background: rgba(255, 255, 255, 0.05) !important;
        border: 1px solid rgba(0, 180, 216, 0.25) !important;
        border-radius: 12px !important;
        padding: 16px !important;
        backdrop-filter: blur(10px) !important;
        transition: all 0.3s ease !important;
    }}

    [data-testid="stMetric"]:hover {{
        background: rgba(0, 180, 216, 0.1) !important;
        border-color: rgba(0, 180, 216, 0.5) !important;
        transform: translateY(-2px);
        box-shadow: 0 8px 32px rgba(0, 180, 216, 0.15);
    }}

    [data-testid="stMetricLabel"] {{
        color: #90e0ef !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 12px !important;
        letter-spacing: 0.5px !important;
    }}

    [data-testid="stMetricValue"] {{
        color: #ffffff !important;
        font-family: 'Rajdhani', sans-serif !important;
        font-size: 2rem !important;
        font-weight: 600 !important;
    }}

    /* ── DATAFRAME ── */
    [data-testid="stDataFrame"] {{
        background: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(0, 180, 216, 0.15) !important;
        border-radius: 12px !important;
        backdrop-filter: blur(10px) !important;
    }}

    /* ── HEADINGS ── */
    h1, h2, h3 {{
        font-family: 'Rajdhani', sans-serif !important;
        letter-spacing: 1px !important;
    }}
    h1 {{ color: #ffffff !important; font-weight: 700 !important; }}
    h2 {{ color: #90e0ef !important; font-weight: 600 !important; }}
    h3 {{ color: #caf0f8 !important; font-weight: 500 !important; }}

    /* ── SELECTBOX ── */
    [data-testid="stSelectbox"] > div {{
        background: rgba(255, 255, 255, 0.05) !important;
        border: 1px solid rgba(0, 180, 216, 0.25) !important;
        border-radius: 8px !important;
        backdrop-filter: blur(10px) !important;
    }}

    /* ── DIVIDER ── */
    hr {{ border-color: rgba(0, 180, 216, 0.2) !important; }}

    /* ── SMOOTH PAGE TRANSITIONS ── */
    .main .block-container {{
        animation: fadeIn 0.4s ease-in-out;
    }}
    @keyframes fadeIn {{
        from {{ opacity: 0; transform: translateY(8px); }}
        to   {{ opacity: 1; transform: translateY(0); }}
    }}

    /* ── LIGHT MODE ── */
    @media (prefers-color-scheme: light) {{
        .stApp {{ {bg_css_light} }}

        [data-testid="stSidebar"] {{
            background: rgba(220, 240, 255, 0.85) !important;
            border-right: 1px solid rgba(0, 100, 180, 0.2);
        }}
        [data-testid="stMetric"] {{
            background: rgba(255, 255, 255, 0.6) !important;
            border: 1px solid rgba(0, 100, 180, 0.2) !important;
        }}
        [data-testid="stMetricLabel"] {{ color: #0077b6 !important; }}
        [data-testid="stMetricValue"] {{ color: #03045e !important; }}
        h1 {{ color: #03045e !important; }}
        h2 {{ color: #0077b6 !important; }}
        h3 {{ color: #0096c7 !important; }}
    }}

    /* ── SCROLLBAR ── */
    ::-webkit-scrollbar {{ width: 4px; }}
    ::-webkit-scrollbar-track {{ background: transparent; }}
    ::-webkit-scrollbar-thumb {{
        background: rgba(0, 180, 216, 0.4);
        border-radius: 4px;
    }}

    /* ── CAPTION ── */
    .stCaption {{ color: rgba(144, 224, 239, 0.7) !important; }}
</style>
""", unsafe_allow_html=True)

PLATFORMS       = ['R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A']
VALID_PLATFORMS = ['R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A']

# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def format_metric(value, decimals=1):
    try:
        if value is None:
            return "N/A"
        if isinstance(value, float) and pd.isna(value):
            return "N/A"
        return f"{float(value):.{decimals}f}"
    except:
        return "N/A"

def filter_by_days(df, date_col, days):
    if df.empty:
        return df
    df[date_col] = pd.to_datetime(df[date_col])
    max_date     = df[date_col].max()
    cutoff       = max_date - pd.Timedelta(days=days)
    return df[df[date_col] >= cutoff]

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_latest_production():
    conn = get_connection()
    df   = pd.read_sql("""
        SELECT * FROM oil_production
        WHERE date = (SELECT MAX(date) FROM oil_production)
    """, conn)
    conn.close()
    df = df[df['platform'].isin(VALID_PLATFORMS)]
    df = df[df['well_name'].str.contains('#', na=False)]
    return df

def load_production_trend(days=30):
    conn = get_connection()
    df   = pd.read_sql("""
        SELECT date,
               SUM(oil_rate_bpd)        as total_oil,
               SUM(liquid_rate_bpd)     as total_liquid,
               SUM(production_loss_bbl) as total_loss
        FROM oil_production
        GROUP BY date ORDER BY date
    """, conn)
    conn.close()
    return filter_by_days(df, 'date', days)

def load_platform_trend(days=30):
    conn = get_connection()
    df   = pd.read_sql("""
        SELECT date, platform, SUM(oil_rate_bpd) as oil
        FROM oil_production
        WHERE platform IN ('R-7A','R-9A','R-10A','R-12A','R-12B','R-13A')
        GROUP BY date, platform ORDER BY date
    """, conn)
    conn.close()
    return filter_by_days(df, 'date', days)

def load_latest_pressure():
    conn = get_connection()
    df   = pd.read_sql("""
        SELECT * FROM pressure_data
        ORDER BY timestamp DESC LIMIT 1
    """, conn)
    conn.close()
    return df

def load_pressure_trend(days=7):
    conn = get_connection()
    df   = pd.read_sql("SELECT * FROM pressure_data ORDER BY timestamp ASC", conn)
    conn.close()
    return filter_by_days(df, 'timestamp', days)

def load_esp_data(well=None):
    conn = get_connection()
    if well:
        df = pd.read_sql("""
            SELECT * FROM esp_parameters
            WHERE well_name = ? ORDER BY timestamp ASC
        """, conn, params=(well,))
    else:
        df = pd.read_sql(
            "SELECT * FROM esp_parameters ORDER BY timestamp ASC", conn)
    conn.close()
    return df

def load_water_injection():
    conn = get_connection()
    df   = pd.read_sql("""
        SELECT * FROM water_injection
        WHERE date = (SELECT MAX(date) FROM water_injection)
        ORDER BY platform, well_name
    """, conn)
    conn.close()
    return df

def load_water_injection_trend(days=90):
    conn = get_connection()
    df   = pd.read_sql("""
        SELECT date,
               SUM(flow_rate_bpd)       as total_bpd,
               SUM(cumulative_flow_bbl) as cumulative
        FROM water_injection
        GROUP BY date ORDER BY date
    """, conn)
    conn.close()
    return filter_by_days(df, 'date', days)

# ── TOP NAVIGATION BAR ────────────────────────────────────────────────────────
st.markdown(f"""
<style>
    /* Hide default sidebar */
    [data-testid="stSidebar"] {{
        display: none;
    }}

    /* Top navbar */
    .navbar {{
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        z-index: 999;
        background: rgba(0, 10, 30, 0.90);
        backdrop-filter: blur(12px);
        border-bottom: 1px solid rgba(0, 180, 216, 0.25);
        padding: 0 24px;
        display: flex;
        align-items: center;
        height: 56px;
        gap: 8px;
    }}

    .navbar-brand {{
        display: flex;
        align-items: center;
        gap: 10px;
        margin-right: 32px;
        text-decoration: none;
    }}

    .navbar-brand img {{
        height: 36px;
        width: auto;
    }}

    .navbar-brand-text {{
        font-family: 'Rajdhani', sans-serif;
        font-size: 18px;
        font-weight: 700;
        color: #ffffff;
        letter-spacing: 1px;
        white-space: nowrap;
    }}

    .nav-link {{
        font-family: 'Inter', sans-serif;
        font-size: 13px;
        font-weight: 500;
        color: rgba(144, 224, 239, 0.75);
        padding: 6px 14px;
        border-radius: 6px;
        cursor: pointer;
        border: none;
        background: transparent;
        transition: all 0.2s ease;
        white-space: nowrap;
        text-decoration: none;
        letter-spacing: 0.3px;
    }}

    .nav-link:hover {{
        color: #ffffff;
        background: rgba(0, 180, 216, 0.15);
    }}

    .nav-link.active {{
        color: #ffffff;
        background: rgba(0, 180, 216, 0.25);
        border: 1px solid rgba(0, 180, 216, 0.4);
    }}

    .nav-spacer {{
        flex: 1;
    }}

    .nav-timestamp {{
        font-family: 'Inter', sans-serif;
        font-size: 11px;
        color: rgba(144, 224, 239, 0.5);
        white-space: nowrap;
    }}

    /* Push content below navbar */
    .main .block-container {{
        padding-top: 80px !important;
        animation: fadeIn 0.4s ease-in-out;
    }}

    @keyframes fadeIn {{
        from {{ opacity: 0; transform: translateY(8px); }}
        to   {{ opacity: 1; transform: translateY(0); }}
    }}
</style>
""", unsafe_allow_html=True)

# Navigation pages
PAGES = [
    ("🏠", "Field Overview"),
    ("📈", "Production Trends"),
    ("🔧", "ESP Health"),
    ("💧", "Water Injection"),
    ("📊", "Pressure Analysis"),
    ("⚠️", "Early Warning"),
]

# Use query params to track active page
if 'page' not in st.session_state:
    st.session_state.page = "Field Overview"

# Build navbar HTML
nav_links = ""
for icon, name in PAGES:
    active_class = "active" if st.session_state.page == name else ""
    nav_links += f"""
        <span class="nav-link {active_class}"
              onclick="window.location.href='?page={name.replace(' ', '_')}'"
              id="nav-{name.replace(' ', '_')}">
            {icon} {name}
        </span>
    """

# Read image for navbar logo
logo_b64 = get_base64_image("ongc_logo.jpg")
logo_img = f'<img src="data:image/jpeg;base64,{logo_b64}">' if logo_b64 else "⚡"

st.markdown(f"""
<div class="navbar" style="display: flex !important; position: fixed !important; top: 0 !important; left: 0 !important; right: 0 !important; z-index: 9999 !important;">
    <div class="navbar-brand">
        {logo_img}
        <span class="navbar-brand-text">OIL FIELD</span>
    </div>
    {nav_links}
    <div class="nav-spacer"></div>
    <span class="nav-timestamp">
        🕐 {datetime.now().strftime('%d-%b-%Y %H:%M')}
    </span>
</div>
<script>
document.querySelector('.navbar').style.position = 'fixed';
document.querySelector('.navbar').style.zIndex = '9999';
</script>
""", unsafe_allow_html=True)

# Check if page was changed via URL query parameter
if 'page' in st.query_params:
    page_param = st.query_params['page'].replace('_', ' ')
    if page_param in [name for _, name in PAGES]:
        st.session_state.page = page_param

page = st.session_state.page
# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — FIELD OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "Field Overview":
    st.title("🛢️ Oil Field — Production Overview")

    prod_df     = load_latest_production()
    pressure_df = load_latest_pressure()

    if prod_df.empty:
        st.warning("No production data found. Please run ingestion scripts.")
        st.stop()

    latest_date = prod_df['date'].max()
    st.caption(f"📅 Data as of: {latest_date}")

    total_oil     = prod_df['oil_rate_bpd'].sum()
    total_liquid  = prod_df['liquid_rate_bpd'].sum()
    total_loss    = prod_df['production_loss_bbl'].sum()
    wells_flowing = len(prod_df[prod_df['well_status'].str.contains(
        'Flowing', na=False, case=False)])
    wells_total   = len(prod_df)
    wells_down    = len(prod_df[prod_df['well_status'].str.contains(
        'Non|Workover|Failure', na=False, case=False)])

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("🛢️ Total Oil (BOPD)",      f"{total_oil:,.0f}")
    col2.metric("💧 Total Liquid (BLPD)",   f"{total_liquid:,.0f}")
    col3.metric("📉 Production Loss (BBL)", f"{total_loss:,.0f}",
                delta=f"-{total_loss:,.0f}", delta_color="inverse")
    col4.metric("✅ Wells Flowing",         f"{wells_flowing} / {wells_total}")
    col5.metric("🔴 Wells Down",            f"{wells_down}",
                delta_color="inverse")

    st.divider()

    st.subheader("📊 Platform Summary")
    platform_summary = prod_df.groupby('platform').agg(
        Oil_BOPD      = ('oil_rate_bpd',        'sum'),
        Liquid_BLPD   = ('liquid_rate_bpd',     'sum'),
        Loss_BBL      = ('production_loss_bbl', 'sum'),
        Wells_Total   = ('well_name',            'count'),
        Wells_Flowing = ('well_status',
                         lambda x: x.str.contains(
                             'Flowing', na=False, case=False).sum())
    ).reset_index()

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
        platform_summary['Line_Pressure_KSC'] = platform_summary[
            'platform'].map(platform_pressure)

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

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🛢️ Oil Contribution by Platform")
        fig = px.pie(
            platform_summary, values='Oil_BOPD', names='platform',
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
        status_counts = prod_df['well_status'].fillna(
            'Unknown').value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        color_map = {
            'Flowing':              '#2a9d8f',
            'Non-Flowing':          '#e63946',
            'Intermittent':         '#f4a261',
            'Self Flowing':         '#457b9d',
            'Workover':             '#e9c46a',
            'ESP Downhole failure':  '#c77dff',
            'Non-FLOWING (CD ESP)': '#e63946',
            'Unknown':              '#6c757d'
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

    st.subheader("🔍 Well Level Detail")
    platform_filter = st.selectbox("Filter by Platform", ['All'] + PLATFORMS)
    filtered_df = prod_df if platform_filter == 'All' else \
        prod_df[prod_df['platform'] == platform_filter]

    display_cols   = ['platform', 'well_name', 'liquid_rate_bpd',
                      'oil_rate_bpd', 'production_loss_bbl',
                      'well_status', 'remarks']
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
        use_container_width=True, hide_index=True
    )

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — PRODUCTION TRENDS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Production Trends":
    st.title("📈 Production Trends")

    conn     = get_connection()
    date_rng = pd.read_sql(
        "SELECT MIN(date) as min_d, MAX(date) as max_d FROM oil_production",
        conn).iloc[0]
    conn.close()

    min_date = pd.to_datetime(date_rng['min_d'])
    max_date = pd.to_datetime(date_rng['max_d'])
    max_days = max(1, (max_date - min_date).days + 1)

    st.caption(f"📅 Data available: {min_date.strftime('%d-%b-%Y')} "
               f"to {max_date.strftime('%d-%b-%Y')} ({max_days} days)")

    days     = st.slider("Select time range (days)", 1, max_days,
                         min(30, max_days))
    trend_df = load_production_trend(days)

    if trend_df.empty:
        st.warning("No trend data available.")
        st.stop()

    trend_df['date'] = pd.to_datetime(trend_df['date']).dt.normalize()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df['date'], y=trend_df['total_oil'],
        name='Oil (BOPD)', line=dict(color='#00b4d8', width=2),
        fill='tozeroy', fillcolor='rgba(0,180,216,0.1)',
        mode='lines+markers', marker=dict(size=6)
    ))
    fig.add_trace(go.Scatter(
        x=trend_df['date'], y=trend_df['total_liquid'],
        name='Liquid (BLPD)',
        line=dict(color='#90e0ef', width=1.5, dash='dash'),
        mode='lines+markers', marker=dict(size=6)
    ))
    fig.add_trace(go.Scatter(
        x=trend_df['date'], y=trend_df['total_loss'],
        name='Loss (BBL)', line=dict(color='#e63946', width=1.5),
        mode='lines+markers', marker=dict(size=6)
    ))
    fig.update_layout(
        title='Field Production Trend',
        xaxis_title='Date', yaxis_title='Barrels',
        height=450,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02),
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Platform wise Oil Trend")
    plat_trend = load_platform_trend(days)
    if not plat_trend.empty:
        plat_trend['date'] = pd.to_datetime(
            plat_trend['date']).dt.normalize()
        fig2 = px.line(
            plat_trend, x='date', y='oil',
            color='platform',
            title='Oil Production by Platform',
            markers=True
        )
        fig2.update_layout(
            height=400,
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font_color='white',
            xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
        )
        fig2.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        fig2.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("📋 Daily Production Summary")
    summary          = trend_df.copy()
    summary['date']  = summary['date'].dt.strftime('%d-%b-%Y')
    summary.columns  = ['Date', 'Oil (BOPD)', 'Liquid (BLPD)', 'Loss (BBL)']
    summary          = summary.sort_values('Date', ascending=False)
    st.dataframe(
        summary.style.format({
            'Oil (BOPD)':    '{:,.0f}',
            'Liquid (BLPD)': '{:,.0f}',
            'Loss (BBL)':    '{:,.0f}'
        }),
        use_container_width=True, hide_index=True
    )

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — ESP HEALTH
# ══════════════════════════════════════════════════════════════════════════════
elif page == "ESP Health":
    st.title("ESP Health Monitor")

    esp_df = load_esp_data()
    if esp_df.empty:
        st.warning("No ESP data found. Please add Avalon export files.")
        st.stop()

    wells         = sorted(esp_df['well_name'].unique().tolist())
    selected_well = st.selectbox("Select Well", wells)

    well_df = esp_df[esp_df['well_name'] == selected_well].copy()
    well_df['timestamp'] = pd.to_datetime(well_df['timestamp'])
    well_df = well_df.sort_values('timestamp')
    latest  = well_df.iloc[-1]

    numeric_cols = [
        'motor_temp_1_c', 'vfd_output_frequency_hz',
        'pump_discharge_pressure_psi', 'pump_intake_pressure_psi',
        'motor_load_pct', 'motor_current_avg_amp',
        'motor_current_a_amp', 'motor_current_b_amp', 'motor_current_c_amp',
        'pump_intake_temp_c', 'vibration_x', 'vibration_y'
    ]
    available_numeric = [c for c in numeric_cols if c in well_df.columns]
    well_df_resampled = (
        well_df.set_index('timestamp')[available_numeric]
        .resample('12h').mean().dropna(how='all').reset_index()
    )

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
    st.caption(
        f"📊 Charts show 12-hourly averages | "
        f"Raw points: {len(well_df):,} | "
        f"Chart points: {len(well_df_resampled):,} | "
        f"Range: {well_df['timestamp'].min().strftime('%d-%b-%Y')} "
        f"to {well_df['timestamp'].max().strftime('%d-%b-%Y')}"
    )

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=well_df_resampled['timestamp'],
        y=well_df_resampled['motor_temp_1_c'],
        name='Motor Temp (°C)', line=dict(color='#e63946', width=2),
        mode='lines+markers', marker=dict(size=4)
    ))
    fig.add_hline(y=135, line_dash="dash", line_color="orange",
                  annotation_text="Warning: 135°C")
    fig.add_hline(y=150, line_dash="dash", line_color="red",
                  annotation_text="Critical: 150°C (Trip)")
    fig.update_layout(
        title=f'Motor Temperature — {selected_well}',
        xaxis_title='Date', yaxis_title='Temperature (°C)', height=350,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("⚡ Phase Current Balance")
    fig2 = go.Figure()
    for phase, color in [
        ('motor_current_a_amp', '#00b4d8'),
        ('motor_current_b_amp', '#f4a261'),
        ('motor_current_c_amp', '#2a9d8f')
    ]:
        fig2.add_trace(go.Scatter(
            x=well_df_resampled['timestamp'],
            y=well_df_resampled[phase],
            name=f'Phase {phase[-5].upper()}',
            line=dict(color=color),
            mode='lines+markers', marker=dict(size=4)
        ))
    fig2.update_layout(
        title='3-Phase Motor Current (12-hour averages)',
        xaxis_title='Date', yaxis_title='Current (Amps)', height=350,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig2.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig2.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("🔄 Pump Pressure")
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(
        x=well_df_resampled['timestamp'],
        y=well_df_resampled['pump_intake_pressure_psi'],
        name='Intake (psi)', line=dict(color='#90e0ef'),
        mode='lines+markers', marker=dict(size=4)
    ))
    fig3.add_trace(go.Scatter(
        x=well_df_resampled['timestamp'],
        y=well_df_resampled['pump_discharge_pressure_psi'],
        name='Discharge (psi)', line=dict(color='#0077b6'),
        mode='lines+markers', marker=dict(size=4)
    ))
    fig3.update_layout(
        title='Pump Pressure (12-hour averages)', height=350,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='white', yaxis_title='Pressure (psi)',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig3.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig3.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("📡 VFD Frequency")
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(
        x=well_df_resampled['timestamp'],
        y=well_df_resampled['vfd_output_frequency_hz'],
        name='VFD (Hz)', line=dict(color='#e9c46a'),
        mode='lines+markers', marker=dict(size=4)
    ))
    fig4.add_hline(y=60, line_dash="dash", line_color="red",
                   annotation_text="Max: 60 Hz")
    fig4.add_hline(y=40, line_dash="dash", line_color="orange",
                   annotation_text="Min: 40 Hz")
    fig4.update_layout(
        title='VFD Frequency (12-hour averages)', height=300,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='white', yaxis_title='Hz',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig4.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig4.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig4, use_container_width=True)

    st.subheader("📊 Motor Load")
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(
        x=well_df_resampled['timestamp'],
        y=well_df_resampled['motor_load_pct'],
        name='Motor Load (%)', line=dict(color='#c77dff'),
        fill='tozeroy', fillcolor='rgba(199,125,255,0.1)',
        mode='lines+markers', marker=dict(size=4)
    ))
    fig5.add_hline(y=90, line_dash="dash", line_color="red",
                   annotation_text="Overload: 90%")
    fig5.add_hline(y=30, line_dash="dash", line_color="orange",
                   annotation_text="Underload: 30%")
    fig5.update_layout(
        title='Motor Load (12-hour averages)', height=300,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='white', yaxis_title='Load (%)',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig5.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig5.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig5, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — WATER INJECTION
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Water Injection":
    st.title("Water Injection Summary")

    wi_df = load_water_injection()
    if wi_df.empty:
        st.warning("No water injection data found.")
        st.stop()

    latest_date = wi_df['date'].max()
    st.caption(f"📅 Data as of: {latest_date}")

    total_injected   = wi_df['flow_rate_bpd'].sum()
    total_planned    = wi_df['planned_wi_bpd'].sum()
    total_cumulative = wi_df['cumulative_flow_bbl'].sum()
    wells_injecting  = len(wi_df[wi_df['status'] == 'Injection'])

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

    st.subheader("📈 Historical Injection Trend")
    conn     = get_connection()
    wi_range = pd.read_sql(
        "SELECT MIN(date) as min_d, MAX(date) as max_d FROM water_injection",
        conn).iloc[0]
    conn.close()

    wi_min  = pd.to_datetime(wi_range['min_d'])
    wi_max  = pd.to_datetime(wi_range['max_d'])
    wi_days = max(1, (wi_max - wi_min).days + 1)

    days     = st.slider("Days to show", 1, wi_days, min(90, wi_days))
    wi_trend = load_water_injection_trend(days)

    if not wi_trend.empty:
        wi_trend['date'] = pd.to_datetime(wi_trend['date']).dt.normalize()
        fig = px.line(wi_trend, x='date', y='total_bpd',
                      title='Daily Water Injection Rate (BPD)', markers=True)
        fig.update_layout(
            height=350,
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font_color='white',
            xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
        )
        fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        fig.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — PRESSURE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Pressure Analysis":
    st.title("Pipeline Pressure Analysis")

    conn      = get_connection()
    pr_range  = pd.read_sql(
        "SELECT MIN(timestamp) as min_t, MAX(timestamp) as max_t FROM pressure_data",
        conn).iloc[0]
    conn.close()

    pr_min  = pd.to_datetime(pr_range['min_t'])
    pr_max  = pd.to_datetime(pr_range['max_t'])
    pr_days = max(1, (pr_max - pr_min).days + 1)

    st.caption(f"📅 Pressure data: {pr_min.strftime('%d-%b-%Y')} "
               f"to {pr_max.strftime('%d-%b-%Y')}")

    days     = st.slider("Select time range (days)", 7, pr_days,
                         min(30, pr_days))
    press_df = load_pressure_trend(days)

    if press_df.empty:
        st.warning("No pressure data in selected range.")
        st.stop()

    press_df['timestamp'] = pd.to_datetime(press_df['timestamp'])
    for col in press_df.select_dtypes(include='number').columns:
        press_df[col] = press_df[col].where(press_df[col] <= 200, other=None)

    st.subheader("🔵 Route 1 — R10A to Heera")
    st.caption("R9A + R13A + R10A crude → Heera Complex via 10\" 45km line")
    fig1 = go.Figure()
    for col, name, color in [
        ('r7a_r10a_lp',  'R7A L/P',        '#00b4d8'),
        ('r10a_r9a_rp',  'R9A→R10A R/P',   '#90e0ef'),
        ('r10a_hra_lp',  'R10A→HRA L/P',   '#0077b6'),
        ('r9a_r10a_lp',  'R9A L/P',        '#48cae4'),
        ('r13a_r10a_lp', 'R13A L/P',       '#ade8f4'),
    ]:
        fig1.add_trace(go.Scatter(
            x=press_df['timestamp'], y=press_df[col],
            name=name, line=dict(color=color)
        ))
    fig1.update_layout(
        height=400,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='white', yaxis_title='Pressure (KSC)',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig1.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig1.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("🟠 Route 2 — R12A to Heera")
    st.caption("R7A + R12B + R12A crude → Heera via 12\" 41km + 10\" 41km")
    fig2 = go.Figure()
    for col, name, color in [
        ('r10a_r12a_lp', 'R10A→R12A L/P (R7A)', '#f4a261'),
        ('r12a_r10a_rp', 'R12A R/P from R10A',  '#e76f51'),
        ('r12a_r12b_rp', 'R12A R/P from R12B',  '#e9c46a'),
        ('r12a_hra_lp',  'R12A→HRA L/P',        '#264653'),
    ]:
        fig2.add_trace(go.Scatter(
            x=press_df['timestamp'], y=press_df[col],
            name=name, line=dict(color=color)
        ))
    fig2.update_layout(
        height=400,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='white', yaxis_title='Pressure (KSC)',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig2.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig2.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("⚠️ Pipeline ΔP Analysis")
    st.caption("ΔP = L/P - R/P. Rising ΔP = pipeline restriction building up")
    press_df['r7a_dp']  = press_df['r7a_r10a_lp']  - press_df['r10a_r7a_rp']
    press_df['r9a_dp']  = press_df['r9a_r10a_lp']  - press_df['r10a_r9a_rp']
    press_df['r13a_dp'] = press_df['r13a_r10a_lp'] - press_df['r10a_r13a_rp']

    fig3 = go.Figure()
    for col, name, color in [
        ('r7a_dp',  'R7A ΔP',  '#00b4d8'),
        ('r9a_dp',  'R9A ΔP',  '#f4a261'),
        ('r13a_dp', 'R13A ΔP', '#2a9d8f'),
    ]:
        fig3.add_trace(go.Scatter(
            x=press_df['timestamp'], y=press_df[col],
            name=name, line=dict(color=color)
        ))
    fig3.update_layout(
        title='Pipeline ΔP (L/P - R/P)', height=350,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color='white', yaxis_title='ΔP (KSC)',
        xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45)
    )
    fig3.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    fig3.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    st.plotly_chart(fig3, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — EARLY WARNING
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Early Warning":
    st.title("Early Warning System")
    st.caption("Automated alerts based on threshold monitoring")

    alerts = []

    esp_df = load_esp_data()
    if not esp_df.empty:
        latest_esp = esp_df.sort_values('timestamp').groupby(
            'well_name').last().reset_index()

        for _, row in latest_esp.iterrows():
            temp        = row.get('motor_temp_1_c')
            intake_temp = row.get('pump_intake_temp_c')

            if temp and not pd.isna(temp):
                if temp > 150:
                    alerts.append({
                        'Level': '🔴 CRITICAL', 'Well': row['well_name'],
                        'Parameter': 'Motor Temperature',
                        'Value': f"{temp:.1f} °C",
                        'Threshold': '>150°C (Trip)',
                        'Action': 'Check gas lock or overload immediately'
                    })
                elif temp > 135:
                    alerts.append({
                        'Level': '🟡 WARNING', 'Well': row['well_name'],
                        'Parameter': 'Motor Temperature',
                        'Value': f"{temp:.1f} °C",
                        'Threshold': '>135°C',
                        'Action': 'Monitor closely'
                    })

            if (temp and intake_temp and
                    not pd.isna(temp) and not pd.isna(intake_temp)):
                delta_t = temp - intake_temp
                if delta_t > 45:
                    alerts.append({
                        'Level': '🔴 CRITICAL', 'Well': row['well_name'],
                        'Parameter': 'Motor ΔT',
                        'Value': f"{delta_t:.1f} °C",
                        'Threshold': '>45°C',
                        'Action': 'Possible gas lock or scale'
                    })
                elif delta_t > 35:
                    alerts.append({
                        'Level': '🟡 WARNING', 'Well': row['well_name'],
                        'Parameter': 'Motor ΔT',
                        'Value': f"{delta_t:.1f} °C",
                        'Threshold': '>35°C',
                        'Action': 'Motor running hotter than normal'
                    })

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
                            'Level': '🔴 CRITICAL', 'Well': row['well_name'],
                            'Parameter': 'Phase Imbalance',
                            'Value': f"{imbalance:.1f}%",
                            'Threshold': '>10%',
                            'Action': 'Plan megger test'
                        })
                    elif imbalance > 5:
                        alerts.append({
                            'Level': '🟡 WARNING', 'Well': row['well_name'],
                            'Parameter': 'Phase Imbalance',
                            'Value': f"{imbalance:.1f}%",
                            'Threshold': '>5%',
                            'Action': 'Monitor cable health'
                        })

            freq = row.get('vfd_output_frequency_hz')
            if freq and not pd.isna(freq):
                if freq > 60:
                    alerts.append({
                        'Level': '🔴 CRITICAL', 'Well': row['well_name'],
                        'Parameter': 'VFD Frequency',
                        'Value': f"{freq:.1f} Hz",
                        'Threshold': '>60 Hz',
                        'Action': 'Reduce frequency immediately'
                    })
                elif freq < 40 and freq > 0:
                    alerts.append({
                        'Level': '🟡 WARNING', 'Well': row['well_name'],
                        'Parameter': 'VFD Frequency',
                        'Value': f"{freq:.1f} Hz",
                        'Threshold': '<40 Hz',
                        'Action': 'Check inflow'
                    })

    press_df = load_pressure_trend(days=3)
    if not press_df.empty:
        press_df['timestamp'] = pd.to_datetime(press_df['timestamp'])
        for col in press_df.select_dtypes(include='number').columns:
            press_df[col] = press_df[col].where(
                press_df[col] <= 200, other=None)

        for col, label in [
            ('r7a_r10a_lp',  'R7A Launcher Pressure'),
            ('r9a_r10a_lp',  'R9A Launcher Pressure'),
            ('r13a_r10a_lp', 'R13A Launcher Pressure'),
            ('r10a_hra_lp',  'R10A→HRA Launcher Pressure'),
            ('r12a_hra_lp',  'R12A→HRA Launcher Pressure'),
        ]:
            if col in press_df.columns:
                recent = press_df[col].dropna()
                if len(recent) >= 4:
                    trend = recent.iloc[-1] - recent.iloc[-4]
                    if trend > 5:
                        alerts.append({
                            'Level': '🔴 CRITICAL', 'Well': 'Field',
                            'Parameter': f'{label} Rising Fast',
                            'Value': f"+{trend:.1f} KSC",
                            'Threshold': '>5 KSC',
                            'Action': 'Check for wax/hydrate buildup'
                        })
                    elif trend > 3:
                        alerts.append({
                            'Level': '🟡 WARNING', 'Well': 'Field',
                            'Parameter': f'{label} Rising',
                            'Value': f"+{trend:.1f} KSC",
                            'Threshold': '>3 KSC',
                            'Action': 'Monitor back pressure trend'
                        })

    if alerts:
        alert_df = pd.DataFrame(alerts)
        alert_df['sort'] = alert_df['Level'].apply(
            lambda x: 0 if 'CRITICAL' in x else 1)
        alert_df = alert_df.sort_values('sort').drop('sort', axis=1)
        st.dataframe(alert_df, use_container_width=True, hide_index=True)
        critical = len([a for a in alerts if 'CRITICAL' in a['Level']])
        warning  = len([a for a in alerts if 'WARNING'  in a['Level']])
        if critical > 0:
            st.error(f"🔴 {critical} CRITICAL alert(s) — immediate action!")
        if warning > 0:
            st.warning(f"🟡 {warning} WARNING alert(s) — monitor closely")
    else:
        st.success("✅ All parameters within normal range. No active alerts.")

    st.divider()
    st.subheader("⚙️ Alert Thresholds — Field Specific")
    st.info("""
    **ESP Motor Temperature (Trip: 150°C):**
    - Warning: >135°C OR ΔT >35°C above intake
    - Critical: >150°C OR ΔT >45°C above intake

    **Phase Current Imbalance:**
    - Warning: >5% | Critical: >10%

    **VFD Frequency (40-60 Hz):**
    - Warning: <40 Hz or >60 Hz

    **Pipeline Launcher Pressure:**
    - Warning: >3 KSC rise | Critical: >5 KSC rise
    """)