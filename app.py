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
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

st.set_page_config(
    page_title="Oil Field Dashboard",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── IMAGES ────────────────────────────────────────────────────────────────────
def get_base64_image(path):
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except:
        return None

bg   = get_base64_image("offshore_pics.jpg")
logo = get_base64_image("ongc_logo.jpg")
bg_url   = f'url("data:image/jpeg;base64,{bg}")'  if bg   else "none"
logo_src = f'data:image/jpeg;base64,{logo}'        if logo else ""

# ── GLOBAL STYLES ─────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=Inter:wght@300;400;500&display=swap');

.stApp {{
    background-image: linear-gradient(rgba(0,0,0,0.60),rgba(0,10,30,0.72)), {bg_url};
    background-size: cover;
    background-position: center;
    background-attachment: fixed;
}}

[data-testid="stSidebar"]        {{ display: none !important; }}
[data-testid="collapsedControl"] {{ display: none !important; }}

/* NAVBAR */
.navbar {{
    position: fixed; top: 0; left: 0; right: 0; z-index: 1000;
    background: rgba(0,8,24,0.92); backdrop-filter: blur(14px);
    border-bottom: 1px solid rgba(0,180,216,0.25);
    display: flex; align-items: center;
    height: 54px; padding: 0 20px; gap: 4px;
    pointer-events: none;
}}
.nb-brand {{ display:flex; align-items:center; gap:8px; margin-right:20px; }}
.nb-brand img {{ height:32px; width:auto; border-radius:4px; }}
.nb-brand-text {{
    font-family:'Rajdhani',sans-serif; font-size:16px; font-weight:700;
    color:#fff; letter-spacing:1px; white-space:nowrap;
}}
.nb-item {{
    font-family:'Inter',sans-serif; font-size:12px; font-weight:500;
    color:rgba(144,224,239,0.75); padding:5px 11px; border-radius:6px;
    white-space:nowrap;
}}
.nb-item.active {{
    color:#fff; background:rgba(0,180,216,0.22);
    border:1px solid rgba(0,180,216,0.4);
}}
.nb-spacer {{ flex:1; }}
.nb-time {{
    font-family:'Inter',sans-serif; font-size:11px;
    color:rgba(144,224,239,0.5); white-space:nowrap;
}}

.main .block-container {{
    padding-top: 70px !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
    animation: fadeIn 0.35s ease;
}}
@keyframes fadeIn {{
    from {{ opacity:0; transform:translateY(6px); }}
    to   {{ opacity:1; transform:translateY(0); }}
}}

.nav-btn-row {{
    position: fixed !important;
    top: 0 !important; left: 160px !important; right: 160px !important;
    z-index: 1001 !important; height: 54px !important; display: flex !important;
}}
.nav-btn-row > div[data-testid="column"] {{
    flex: 1 !important; height: 54px !important; padding: 0 !important;
}}
.nav-btn-row button {{
    width: 100% !important; height: 54px !important;
    opacity: 0 !important; border-radius: 0 !important;
    border: none !important; cursor: pointer !important;
}}

/* METRIC CARDS */
[data-testid="stMetric"] {{
    background: rgba(255,255,255,0.05) !important;
    border: 1px solid rgba(0,180,216,0.22) !important;
    border-radius: 10px !important; padding: 14px !important;
    backdrop-filter: blur(8px) !important;
    transition: all 0.25s ease !important;
}}
[data-testid="stMetric"]:hover {{
    background: rgba(0,180,216,0.09) !important;
    border-color: rgba(0,180,216,0.45) !important;
    transform: translateY(-2px);
    box-shadow: 0 6px 24px rgba(0,180,216,0.12);
}}
[data-testid="stMetricLabel"] {{
    color: #90e0ef !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 11px !important;
}}
[data-testid="stMetricValue"] {{
    color: #fff !important;
    font-family: 'Rajdhani', sans-serif !important;
    font-size: 1.9rem !important; font-weight: 600 !important;
}}

h1,h2,h3 {{ font-family:'Rajdhani',sans-serif !important; letter-spacing:1px !important; }}
h1 {{ color:#fff !important; font-weight:700 !important; }}
h2 {{ color:#90e0ef !important; font-weight:600 !important; }}
h3 {{ color:#caf0f8 !important; font-weight:500 !important; }}

[data-testid="stDataFrame"] {{
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(0,180,216,0.13) !important;
    border-radius: 10px !important;
}}

hr {{ border-color: rgba(0,180,216,0.18) !important; }}

::-webkit-scrollbar {{ width: 4px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: rgba(0,180,216,0.35); border-radius:4px; }}

.stCaption {{ color: rgba(144,224,239,0.65) !important; }}

/* STYLED SELECTBOX — dark glass theme */
[data-testid="stSelectbox"] > div > div {{
    background: rgba(0,8,24,0.85) !important;
    border: 1px solid rgba(0,180,216,0.3) !important;
    border-radius: 8px !important;
    backdrop-filter: blur(10px) !important;
    color: #90e0ef !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 12px !important;
}}
[data-testid="stSelectbox"] > div > div:hover {{
    border-color: rgba(0,180,216,0.6) !important;
    background: rgba(0,20,50,0.90) !important;
}}
[data-testid="stSelectbox"] label {{
    color: rgba(144,224,239,0.7) !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 11px !important;
}}

/* STYLED DATE INPUT */
[data-testid="stDateInput"] > div > div > input {{
    background: rgba(0,8,24,0.85) !important;
    border: 1px solid rgba(0,180,216,0.3) !important;
    border-radius: 8px !important;
    color: #90e0ef !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 12px !important;
}}
[data-testid="stDateInput"] label {{
    color: rgba(144,224,239,0.7) !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 11px !important;
}}

/* ESP PILL BUTTONS */
.pill-container {{
    display: flex; gap: 6px; flex-wrap: wrap;
    margin-bottom: 16px;
}}
.pill {{
    font-family: 'Inter', sans-serif;
    font-size: 12px; font-weight: 500;
    color: rgba(144,224,239,0.75);
    padding: 5px 14px; border-radius: 20px;
    border: 1px solid rgba(0,180,216,0.25);
    background: rgba(0,8,24,0.7);
    backdrop-filter: blur(8px);
    cursor: pointer; white-space: nowrap;
    transition: all 0.2s ease;
}}
.pill.active {{
    color: #fff;
    background: rgba(0,180,216,0.25);
    border-color: rgba(0,180,216,0.5);
    box-shadow: 0 0 12px rgba(0,180,216,0.2);
}}

/* LIGHT MODE */
@media (prefers-color-scheme: light) {{
    .stApp {{
        background-image: linear-gradient(rgba(255,255,255,0.68),rgba(220,240,255,0.72)), {bg_url};
    }}
    .navbar {{ background: rgba(220,240,255,0.93); border-bottom:1px solid rgba(0,100,180,0.2); }}
    .nb-brand-text {{ color: #03045e; }}
    .nb-item {{ color: rgba(0,100,180,0.8); }}
    .nb-item.active {{ color:#03045e; background:rgba(0,100,180,0.12); border-color:rgba(0,100,180,0.3); }}
    [data-testid="stMetricLabel"] {{ color: #0077b6 !important; }}
    [data-testid="stMetricValue"] {{ color: #03045e !important; }}
    h1 {{ color: #03045e !important; }}
    h2 {{ color: #0077b6 !important; }}
    h3 {{ color: #0096c7 !important; }}
}}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# NAVIGATION
# ══════════════════════════════════════════════════════════════════════════════

PAGES = [
    ("🏠", "Field Overview"),
    ("📈", "Production Trends"),
    ("🔧", "ESP Health"),
    ("💧", "Water Injection"),
    ("📊", "Pressure Analysis"),
    ("⚠️", "Early Warning"),
]

if 'page' not in st.session_state:
    st.session_state.page = "Field Overview"

nav_html = ""
for icon, name in PAGES:
    cls = "nb-item active" if st.session_state.page == name else "nb-item"
    nav_html += f'<span class="{cls}">{icon} {name}</span>'

logo_html = f'<img src="{logo_src}" />' if logo_src else "⚡"

st.markdown(f"""
<div class="navbar">
    <div class="nb-brand">{logo_html}<span class="nb-brand-text">OIL FIELD</span></div>
    {nav_html}
    <div class="nb-spacer"></div>
    <span class="nb-time">🕐 {datetime.now().strftime('%d-%b-%Y %H:%M')}</span>
</div>
""", unsafe_allow_html=True)

st.markdown('<div class="nav-btn-row">', unsafe_allow_html=True)
cols = st.columns(len(PAGES))
for i, (icon, name) in enumerate(PAGES):
    with cols[i]:
        if st.button(f"{icon} {name}", key=f"nav_{name}",
                     use_container_width=True):
            st.session_state.page = name
            st.rerun()
st.markdown('</div>', unsafe_allow_html=True)

page = st.session_state.page

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS & HELPERS
# ══════════════════════════════════════════════════════════════════════════════

PLATFORMS       = ['R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A']
VALID_PLATFORMS = ['R-7A', 'R-9A', 'R-10A', 'R-12A', 'R-12B', 'R-13A']
CHART = dict(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
             font_color='white')
GRID  = dict(showgrid=True, gridcolor='rgba(255,255,255,0.08)')

TIME_OPTIONS = ["1 Day", "5 Days", "1 Month", "3 Months",
                "6 Months", "1 Year", "Custom"]

def format_metric(value, decimals=1):
    try:
        if value is None: return "N/A"
        if isinstance(value, float) and pd.isna(value): return "N/A"
        return f"{float(value):.{decimals}f}"
    except:
        return "N/A"

def get_date_range(key, db_min, db_max, default="3 Months"):
    """
    Renders a right-aligned styled dropdown for time range selection.
    When Custom is selected, shows from/to date pickers with calendar.
    Returns (date_from, date_to) as date objects.

    key       — unique key for session state
    db_min    — earliest date in database (date object)
    db_max    — latest date in database (date object)
    default   — default selection from TIME_OPTIONS
    """
    # Right-align the selector by using columns
    _, right = st.columns([3, 1])
    with right:
        selected = st.selectbox(
            "📅 Range",
            TIME_OPTIONS,
            index=TIME_OPTIONS.index(default),
            key=f"range_{key}"
        )

    if selected == "Custom":
        col1, col2 = st.columns(2)
        with col1:
            date_from = st.date_input(
                "From date",
                value=db_min,
                min_value=db_min,
                max_value=db_max,
                key=f"from_{key}",
                format="DD/MM/YYYY"
            )
        with col2:
            date_to = st.date_input(
                "To date",
                value=db_max,
                min_value=db_min,
                max_value=db_max,
                key=f"to_{key}",
                format="DD/MM/YYYY"
            )
    else:
        date_to = db_max
        delta_map = {
            "1 Day":    timedelta(days=1),
            "5 Days":   timedelta(days=5),
            "1 Month":  relativedelta(months=1),
            "3 Months": relativedelta(months=3),
            "6 Months": relativedelta(months=6),
            "1 Year":   relativedelta(years=1),
        }
        date_from = max(db_min, date_to - delta_map[selected])

    return date_from, date_to

def filter_df_by_dates(df, date_col, date_from, date_to):
    """Filter dataframe between two date objects"""
    if df.empty: return df
    df[date_col] = pd.to_datetime(df[date_col])
    return df[
        (df[date_col].dt.date >= date_from) &
        (df[date_col].dt.date <= date_to)
    ]

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_latest_production():
    conn = get_connection()
    df = pd.read_sql(
        "SELECT * FROM oil_production WHERE date=(SELECT MAX(date) FROM oil_production)",
        conn)
    conn.close()
    df = df[df['platform'].isin(VALID_PLATFORMS)]
    df = df[df['well_name'].str.contains('#', na=False)]
    return df

def load_all_production():
    conn = get_connection()
    df = pd.read_sql("""SELECT date,
        SUM(oil_rate_bpd) as total_oil,
        SUM(liquid_rate_bpd) as total_liquid,
        SUM(production_loss_bbl) as total_loss
        FROM oil_production GROUP BY date ORDER BY date""", conn)
    conn.close()
    return df

def load_all_platform_trend():
    conn = get_connection()
    df = pd.read_sql("""SELECT date, platform, SUM(oil_rate_bpd) as oil
        FROM oil_production
        WHERE platform IN ('R-7A','R-9A','R-10A','R-12A','R-12B','R-13A')
        GROUP BY date, platform ORDER BY date""", conn)
    conn.close()
    return df

def load_latest_pressure():
    conn = get_connection()
    df = pd.read_sql(
        "SELECT * FROM pressure_data ORDER BY timestamp DESC LIMIT 1", conn)
    conn.close()
    return df

def load_all_pressure():
    conn = get_connection()
    df = pd.read_sql(
        "SELECT * FROM pressure_data ORDER BY timestamp ASC", conn)
    conn.close()
    # Clean outliers
    for col in df.select_dtypes(include='number').columns:
        df[col] = df[col].where(df[col] <= 200, other=None)
    return df

def load_esp_data(well=None):
    conn = get_connection()
    if well:
        df = pd.read_sql(
            "SELECT * FROM esp_parameters WHERE well_name=? ORDER BY timestamp ASC",
            conn, params=(well,))
    else:
        df = pd.read_sql(
            "SELECT * FROM esp_parameters ORDER BY timestamp ASC", conn)
    conn.close()
    return df

def load_water_injection():
    conn = get_connection()
    df = pd.read_sql("""SELECT * FROM water_injection
        WHERE date=(SELECT MAX(date) FROM water_injection)
        ORDER BY platform, well_name""", conn)
    conn.close()
    return df

def load_all_water_injection_trend():
    conn = get_connection()
    df = pd.read_sql("""SELECT date,
        SUM(flow_rate_bpd) as total_bpd,
        SUM(cumulative_flow_bbl) as cumulative
        FROM water_injection
        WHERE date >= '2000-01-01'
        GROUP BY date ORDER BY date""", conn)
    conn.close()
    return df

def get_db_date_range(table, date_col='date'):
    """Get min/max dates from a table, filtering bad dates"""
    conn = get_connection()
    df = pd.read_sql(
        f"SELECT MIN({date_col}) as mn, MAX({date_col}) as mx FROM {table} WHERE {date_col} >= '2000-01-01'",
        conn).iloc[0]
    conn.close()
    return pd.to_datetime(df['mn']).date(), pd.to_datetime(df['mx']).date()

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

    st.caption(f"📅 Data as of: {prod_df['date'].max()}")

    total_oil     = prod_df['oil_rate_bpd'].sum()
    total_liquid  = prod_df['liquid_rate_bpd'].sum()
    total_loss    = prod_df['production_loss_bbl'].sum()
    wells_flowing = len(prod_df[prod_df['well_status'].str.contains(
        'Flowing', na=False, case=False)])
    wells_total   = len(prod_df)
    wells_down    = len(prod_df[prod_df['well_status'].str.contains(
        'Non|Workover|Failure', na=False, case=False)])

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("🛢️ Total Oil (BOPD)",      f"{total_oil:,.0f}")
    c2.metric("💧 Total Liquid (BLPD)",   f"{total_liquid:,.0f}")
    c3.metric("📉 Production Loss (BBL)", f"{total_loss:,.0f}",
              delta=f"-{total_loss:,.0f}", delta_color="inverse")
    c4.metric("✅ Wells Flowing",         f"{wells_flowing} / {wells_total}")
    c5.metric("🔴 Wells Down",            f"{wells_down}", delta_color="inverse")

    st.divider()
    st.subheader("📊 Platform Summary")

    plat = prod_df.groupby('platform').agg(
        Oil_BOPD=('oil_rate_bpd','sum'),
        Liquid_BLPD=('liquid_rate_bpd','sum'),
        Loss_BBL=('production_loss_bbl','sum'),
        Wells_Total=('well_name','count'),
        Wells_Flowing=('well_status',
            lambda x: x.str.contains('Flowing',na=False,case=False).sum())
    ).reset_index()

    if not pressure_df.empty:
        pr = pressure_df.iloc[0]
        plat['Line_Pressure_KSC'] = plat['platform'].map({
            'R-7A':  pr.get('r7a_r10a_lp'),
            'R-10A': pr.get('r10a_mlp'),
            'R-9A':  pr.get('r9a_r10a_lp'),
            'R-12A': pr.get('r12a_hra_lp'),
            'R-12B': pr.get('r12b_mlp'),
            'R-13A': pr.get('r13a_r10a_lp'),
        })

    st.dataframe(plat.style.format({
        'Oil_BOPD':'{:,.0f}','Liquid_BLPD':'{:,.0f}',
        'Loss_BBL':'{:,.0f}','Line_Pressure_KSC':'{:.1f}'}),
        use_container_width=True, hide_index=True)

    st.divider()
    c1,c2 = st.columns(2)
    with c1:
        st.subheader("🛢️ Oil Contribution by Platform")
        fig = px.pie(plat, values='Oil_BOPD', names='platform',
                     color_discrete_sequence=px.colors.sequential.Blues_r)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(**CHART, showlegend=True, height=340)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("⚡ Well Status Distribution")
        sc = prod_df['well_status'].fillna('Unknown').value_counts().reset_index()
        sc.columns = ['Status','Count']
        fig2 = px.bar(sc, x='Status', y='Count', color='Status',
            color_discrete_map={
                'Flowing':'#2a9d8f','Non-Flowing':'#e63946',
                'Intermittent':'#f4a261','Self Flowing':'#457b9d',
                'Workover':'#e9c46a','ESP Downhole failure':'#c77dff',
                'Non-FLOWING (CD ESP)':'#e63946','Unknown':'#6c757d'})
        fig2.update_layout(**CHART, showlegend=False, height=340)
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    st.subheader("🔍 Well Level Detail")
    pf = st.selectbox("Filter by Platform", ['All'] + PLATFORMS)
    fdf = prod_df if pf == 'All' else prod_df[prod_df['platform'] == pf]
    dcols = [c for c in ['platform','well_name','liquid_rate_bpd',
             'oil_rate_bpd','production_loss_bbl','well_status','remarks']
             if c in fdf.columns]

    def color_status(val):
        if pd.isna(val): return ''
        v = str(val)
        if 'Non' in v or 'Failure' in v: return 'background-color:#e63946;color:white'
        if 'Intermittent' in v: return 'background-color:#f4a261;color:white'
        if 'Workover' in v: return 'background-color:#e9c46a'
        if 'Flowing' in v: return 'background-color:#2a9d8f;color:white'
        return ''

    st.dataframe(fdf[dcols].style.map(color_status, subset=['well_status']),
                 use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — PRODUCTION TRENDS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Production Trends":
    st.title("📈 Production Trends")

    prod_min, prod_max = get_db_date_range('oil_production')
    all_trend = load_all_production()
    all_plat  = load_all_platform_trend()

    if all_trend.empty:
        st.warning("No data available.")
        st.stop()

    # ── FIELD PRODUCTION TREND ────────────────────────────────────────────────
    st.markdown("### Field Production Trend")
    f_from, f_to = get_date_range("field_trend", prod_min, prod_max,
                                   default="3 Months")
    trend = filter_df_by_dates(all_trend.copy(), 'date', f_from, f_to)
    trend['date'] = pd.to_datetime(trend['date']).dt.normalize()

    if not trend.empty:
        st.caption(f"📅 {f_from.strftime('%d-%b-%Y')} → {f_to.strftime('%d-%b-%Y')} | {len(trend)} data points")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=trend['date'], y=trend['total_oil'],
            name='Oil (BOPD)', line=dict(color='#00b4d8', width=2),
            fill='tozeroy', fillcolor='rgba(0,180,216,0.08)',
            mode='lines+markers', marker=dict(size=6)))
        fig.add_trace(go.Scatter(x=trend['date'], y=trend['total_liquid'],
            name='Liquid (BLPD)',
            line=dict(color='#90e0ef', width=1.5, dash='dash'),
            mode='lines+markers', marker=dict(size=6)))
        fig.add_trace(go.Scatter(x=trend['date'], y=trend['total_loss'],
            name='Loss (BBL)', line=dict(color='#e63946', width=1.5),
            mode='lines+markers', marker=dict(size=6)))
        fig.update_layout(**CHART, height=420,
            xaxis_title='Date', yaxis_title='Barrels',
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45))
        fig.update_xaxes(**GRID); fig.update_yaxes(**GRID)
        st.plotly_chart(fig, use_container_width=True)

        # Daily summary follows same date range
        st.subheader("📋 Daily Production Summary")
        s = trend.copy()
        s['date'] = s['date'].dt.strftime('%d-%b-%Y')
        s.columns = ['Date','Oil (BOPD)','Liquid (BLPD)','Loss (BBL)']
        st.dataframe(s.sort_values('Date', ascending=False).style.format(
            {'Oil (BOPD)':'{:,.0f}','Liquid (BLPD)':'{:,.0f}','Loss (BBL)':'{:,.0f}'}),
            use_container_width=True, hide_index=True)
    else:
        st.info("No data in selected range.")

    st.divider()

    # ── PLATFORM WISE TREND ───────────────────────────────────────────────────
    st.markdown("### Platform wise Oil Trend")
    p_from, p_to = get_date_range("plat_trend", prod_min, prod_max,
                                   default="3 Months")
    pt = filter_df_by_dates(all_plat.copy(), 'date', p_from, p_to)

    if not pt.empty:
        pt['date'] = pd.to_datetime(pt['date']).dt.normalize()
        st.caption(f"📅 {p_from.strftime('%d-%b-%Y')} → {p_to.strftime('%d-%b-%Y')} | {len(pt)} data points")
        fig2 = px.line(pt, x='date', y='oil', color='platform',
                       title='Oil by Platform', markers=True)
        fig2.update_layout(**CHART, height=380,
                           xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45))
        fig2.update_xaxes(**GRID); fig2.update_yaxes(**GRID)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data in selected range.")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — ESP HEALTH
# ══════════════════════════════════════════════════════════════════════════════
elif page == "ESP Health":
    st.title("🔧 ESP Health Monitor")
    esp_df = load_esp_data()
    if esp_df.empty:
        st.warning("No ESP data found.")
        st.stop()

    wells = sorted(esp_df['well_name'].unique().tolist())
    sel   = st.selectbox("Select Well", wells)

    wdf = esp_df[esp_df['well_name'] == sel].copy()
    wdf['timestamp'] = pd.to_datetime(wdf['timestamp'])
    wdf    = wdf.sort_values('timestamp')
    latest = wdf.iloc[-1]

    nc    = ['motor_temp_1_c','vfd_output_frequency_hz',
             'pump_discharge_pressure_psi','pump_intake_pressure_psi',
             'motor_load_pct','motor_current_avg_amp',
             'motor_current_a_amp','motor_current_b_amp','motor_current_c_amp',
             'pump_intake_temp_c','vibration_x','vibration_y']
    avail = [c for c in nc if c in wdf.columns]

    # Latest metrics
    st.subheader(f"Latest Reading — {sel}")
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("🌡️ Motor Temp (°C)",    format_metric(latest.get('motor_temp_1_c')))
    c2.metric("⚡ VFD Frequency (Hz)", format_metric(latest.get('vfd_output_frequency_hz')))
    c3.metric("📊 Motor Load (%)",     format_metric(latest.get('motor_load_pct')))
    c4.metric("🔌 Motor Current (A)",  format_metric(latest.get('motor_current_avg_amp')))

    c5,c6,c7,c8 = st.columns(4)
    c5.metric("⬆️ Discharge (psi)",  format_metric(latest.get('pump_discharge_pressure_psi')))
    c6.metric("⬇️ Intake (psi)",     format_metric(latest.get('pump_intake_pressure_psi')))
    c7.metric("🌡️ Intake Temp (°C)", format_metric(latest.get('pump_intake_temp_c')))
    c8.metric("📳 Vibration X",      format_metric(latest.get('vibration_x'), decimals=3))

    st.divider()

    # ── SHARED TIME SELECTOR — PILL STYLE ────────────────────────────────────
    esp_ts_min = wdf['timestamp'].min().date()
    esp_ts_max = wdf['timestamp'].max().date()

    st.markdown("**📅 Chart Time Range**")

    # Use session state to track active pill
    if 'esp_pill' not in st.session_state:
        st.session_state.esp_pill = "1 Month"

    pill_cols = st.columns(len(TIME_OPTIONS))
    for i, opt in enumerate(TIME_OPTIONS):
        with pill_cols[i]:
            active = st.session_state.esp_pill == opt
            label  = f"**{opt}**" if active else opt
            if st.button(opt, key=f"esp_pill_{opt}",
                         use_container_width=True):
                st.session_state.esp_pill = opt
                st.rerun()

    selected_pill = st.session_state.esp_pill

    if selected_pill == "Custom":
        col1, col2 = st.columns(2)
        with col1:
            esp_from = st.date_input("From", value=esp_ts_min,
                min_value=esp_ts_min, max_value=esp_ts_max,
                key="esp_from", format="DD/MM/YYYY")
        with col2:
            esp_to = st.date_input("To", value=esp_ts_max,
                min_value=esp_ts_min, max_value=esp_ts_max,
                key="esp_to", format="DD/MM/YYYY")
    else:
        esp_to = esp_ts_max
        delta_map = {
            "1 Day":    timedelta(days=1),
            "5 Days":   timedelta(days=5),
            "1 Month":  relativedelta(months=1),
            "3 Months": relativedelta(months=3),
            "6 Months": relativedelta(months=6),
            "1 Year":   relativedelta(years=1),
        }
        esp_from = max(esp_ts_min, esp_to - delta_map[selected_pill])

    # Filter and resample
    wdf_filtered = wdf[
        (wdf['timestamp'].dt.date >= esp_from) &
        (wdf['timestamp'].dt.date <= esp_to)
    ].copy()

    st.caption(
        f"📊 12-hourly averages | Raw: {len(wdf_filtered):,} pts | "
        f"{esp_from.strftime('%d-%b-%Y')} → {esp_to.strftime('%d-%b-%Y')}")

    if wdf_filtered.empty:
        st.info("No ESP data in selected range.")
        st.stop()

    rs = (wdf_filtered.set_index('timestamp')[avail]
          .resample('12h').mean().dropna(how='all').reset_index())

    # Motor temp
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=rs['timestamp'], y=rs['motor_temp_1_c'],
        name='Motor Temp', line=dict(color='#e63946',width=2),
        mode='lines+markers', marker=dict(size=4)))
    fig.add_hline(y=135, line_dash="dash", line_color="orange",
                  annotation_text="Warning 135°C")
    fig.add_hline(y=150, line_dash="dash", line_color="red",
                  annotation_text="Trip 150°C")
    fig.update_layout(**CHART, title=f'Motor Temperature — {sel}', height=320,
                      xaxis=dict(tickformat='%d-%b-%Y',tickangle=-45),
                      yaxis_title='°C')
    fig.update_xaxes(**GRID); fig.update_yaxes(**GRID)
    st.plotly_chart(fig, use_container_width=True)

    # Phase current
    st.subheader("⚡ Phase Current Balance")
    fig2 = go.Figure()
    for col,name,color in [
        ('motor_current_a_amp','Phase A','#00b4d8'),
        ('motor_current_b_amp','Phase B','#f4a261'),
        ('motor_current_c_amp','Phase C','#2a9d8f')]:
        fig2.add_trace(go.Scatter(x=rs['timestamp'], y=rs[col],
            name=name, line=dict(color=color),
            mode='lines+markers', marker=dict(size=4)))
    fig2.update_layout(**CHART, title='3-Phase Current (12h avg)', height=300,
                       xaxis=dict(tickformat='%d-%b-%Y',tickangle=-45),
                       yaxis_title='Amps')
    fig2.update_xaxes(**GRID); fig2.update_yaxes(**GRID)
    st.plotly_chart(fig2, use_container_width=True)

    # Pump pressure
    st.subheader("🔄 Pump Pressure")
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=rs['timestamp'],
        y=rs['pump_intake_pressure_psi'], name='Intake (psi)',
        line=dict(color='#90e0ef'), mode='lines+markers', marker=dict(size=4)))
    fig3.add_trace(go.Scatter(x=rs['timestamp'],
        y=rs['pump_discharge_pressure_psi'], name='Discharge (psi)',
        line=dict(color='#0077b6'), mode='lines+markers', marker=dict(size=4)))
    fig3.update_layout(**CHART, title='Pump Pressure (12h avg)', height=300,
                       xaxis=dict(tickformat='%d-%b-%Y',tickangle=-45),
                       yaxis_title='psi')
    fig3.update_xaxes(**GRID); fig3.update_yaxes(**GRID)
    st.plotly_chart(fig3, use_container_width=True)

    # VFD
    st.subheader("📡 VFD Frequency")
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=rs['timestamp'],
        y=rs['vfd_output_frequency_hz'], name='VFD (Hz)',
        line=dict(color='#e9c46a'), mode='lines+markers', marker=dict(size=4)))
    fig4.add_hline(y=60, line_dash="dash", line_color="red",
                   annotation_text="Max 60 Hz")
    fig4.add_hline(y=40, line_dash="dash", line_color="orange",
                   annotation_text="Min 40 Hz")
    fig4.update_layout(**CHART, title='VFD Frequency (12h avg)', height=280,
                       xaxis=dict(tickformat='%d-%b-%Y',tickangle=-45),
                       yaxis_title='Hz')
    fig4.update_xaxes(**GRID); fig4.update_yaxes(**GRID)
    st.plotly_chart(fig4, use_container_width=True)

    # Motor load
    st.subheader("📊 Motor Load")
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(x=rs['timestamp'], y=rs['motor_load_pct'],
        name='Load (%)', line=dict(color='#c77dff'),
        fill='tozeroy', fillcolor='rgba(199,125,255,0.08)',
        mode='lines+markers', marker=dict(size=4)))
    fig5.add_hline(y=90, line_dash="dash", line_color="red",
                   annotation_text="Overload 90%")
    fig5.add_hline(y=30, line_dash="dash", line_color="orange",
                   annotation_text="Underload 30%")
    fig5.update_layout(**CHART, title='Motor Load (12h avg)', height=280,
                       xaxis=dict(tickformat='%d-%b-%Y',tickangle=-45),
                       yaxis_title='%')
    fig5.update_xaxes(**GRID); fig5.update_yaxes(**GRID)
    st.plotly_chart(fig5, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — WATER INJECTION
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Water Injection":
    st.title("💧 Water Injection Summary")
    wi = load_water_injection()
    if wi.empty:
        st.warning("No water injection data found.")
        st.stop()

    st.caption(f"📅 Data as of: {wi['date'].max()}")
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("💉 Total Injection (BPD)", f"{wi['flow_rate_bpd'].sum():,.0f}")
    c2.metric("🎯 Planned (BPD)",         f"{wi['planned_wi_bpd'].sum():,.0f}")
    c3.metric("📊 Cumulative (BBL)",      f"{wi['cumulative_flow_bbl'].sum():,.0f}")
    c4.metric("✅ Wells Injecting",       f"{len(wi[wi['status']=='Injection'])}")

    st.divider()
    st.subheader("Well Level Data")
    dcols = ['platform','well_name','header_pressure_ksc','choke_size',
             'ithp','status','flow_rate_sm3hr','flow_rate_bpd',
             'injecting_hours','cumulative_flow_bbl','planned_wi_bpd']
    st.dataframe(wi[[c for c in dcols if c in wi.columns]],
                 use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### 📈 Historical Injection Trend")

    wi_min, wi_max = get_db_date_range('water_injection')
    wi_from, wi_to = get_date_range("wi_trend", wi_min, wi_max,
                                     default="3 Months")

    conn = get_connection()
    wt = pd.read_sql("""SELECT date,
        SUM(flow_rate_bpd) as total_bpd,
        SUM(cumulative_flow_bbl) as cumulative
        FROM water_injection
        WHERE date >= ? AND date <= ?
        AND date >= '2000-01-01'
        GROUP BY date ORDER BY date""",
        conn, params=(str(wi_from), str(wi_to)))
    conn.close()

    if not wt.empty:
        wt['date'] = pd.to_datetime(wt['date']).dt.normalize()
        st.caption(f"📅 {wi_from.strftime('%d-%b-%Y')} → {wi_to.strftime('%d-%b-%Y')} | {len(wt)} data points")
        fig = px.line(wt, x='date', y='total_bpd',
                      title='Daily Water Injection Rate (BPD)', markers=True)
        fig.update_layout(**CHART, height=360,
                          xaxis=dict(tickformat='%d-%b-%Y', tickangle=-45),
                          yaxis_title='BPD')
        fig.update_xaxes(**GRID); fig.update_yaxes(**GRID)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data in selected range.")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — PRESSURE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Pressure Analysis":
    st.title("📊 Pipeline Pressure Analysis")

    pr_min, pr_max = get_db_date_range('pressure_data', date_col='timestamp')
    all_pdf = load_all_pressure()

    if all_pdf.empty:
        st.warning("No pressure data.")
        st.stop()

    all_pdf['timestamp'] = pd.to_datetime(all_pdf['timestamp'])

    # ── ROUTE 1 ───────────────────────────────────────────────────────────────
    st.markdown("### 🔵 Route 1 — R10A to Heera")
    st.caption("R9A + R13A + R10A → Heera via 10\" 45km")
    r1_from, r1_to = get_date_range("route1", pr_min, pr_max, default="1 Month")
    r1_df = all_pdf[
        (all_pdf['timestamp'].dt.date >= r1_from) &
        (all_pdf['timestamp'].dt.date <= r1_to)].copy()

    if not r1_df.empty:
        st.caption(f"📅 {r1_from.strftime('%d-%b-%Y')} → {r1_to.strftime('%d-%b-%Y')}")
        fig1 = go.Figure()
        for col,name,color in [
            ('r7a_r10a_lp','R7A L/P','#00b4d8'),
            ('r10a_r9a_rp','R9A→R10A R/P','#90e0ef'),
            ('r10a_hra_lp','R10A→HRA L/P','#0077b6'),
            ('r9a_r10a_lp','R9A L/P','#48cae4'),
            ('r13a_r10a_lp','R13A L/P','#ade8f4')]:
            fig1.add_trace(go.Scatter(x=r1_df['timestamp'], y=r1_df[col],
                                      name=name, line=dict(color=color)))
        fig1.update_layout(**CHART, height=380, yaxis_title='KSC',
                           xaxis=dict(tickformat='%d-%b-%Y',tickangle=-45))
        fig1.update_xaxes(**GRID); fig1.update_yaxes(**GRID)
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("No data in selected range.")

    st.divider()

    # ── ROUTE 2 ───────────────────────────────────────────────────────────────
    st.markdown("### 🟠 Route 2 — R12A to Heera")
    st.caption("R7A + R12B + R12A → Heera via 12\" 41km + 10\" 41km")
    r2_from, r2_to = get_date_range("route2", pr_min, pr_max, default="1 Month")
    r2_df = all_pdf[
        (all_pdf['timestamp'].dt.date >= r2_from) &
        (all_pdf['timestamp'].dt.date <= r2_to)].copy()

    if not r2_df.empty:
        st.caption(f"📅 {r2_from.strftime('%d-%b-%Y')} → {r2_to.strftime('%d-%b-%Y')}")
        fig2 = go.Figure()
        for col,name,color in [
            ('r10a_r12a_lp','R10A→R12A L/P','#f4a261'),
            ('r12a_r10a_rp','R12A R/P from R10A','#e76f51'),
            ('r12a_r12b_rp','R12A R/P from R12B','#e9c46a'),
            ('r12a_hra_lp','R12A→HRA L/P','#264653')]:
            fig2.add_trace(go.Scatter(x=r2_df['timestamp'], y=r2_df[col],
                                      name=name, line=dict(color=color)))
        fig2.update_layout(**CHART, height=380, yaxis_title='KSC',
                           xaxis=dict(tickformat='%d-%b-%Y',tickangle=-45))
        fig2.update_xaxes(**GRID); fig2.update_yaxes(**GRID)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data in selected range.")

    st.divider()

    # ── ΔP ANALYSIS ───────────────────────────────────────────────────────────
    st.markdown("### ⚠️ Pipeline ΔP Analysis")
    st.caption("ΔP = L/P - R/P. Rising ΔP = restriction building up")
    dp_from, dp_to = get_date_range("dp", pr_min, pr_max, default="1 Month")
    dp_df = all_pdf[
        (all_pdf['timestamp'].dt.date >= dp_from) &
        (all_pdf['timestamp'].dt.date <= dp_to)].copy()

    if not dp_df.empty:
        dp_df['r7a_dp']  = dp_df['r7a_r10a_lp']  - dp_df['r10a_r7a_rp']
        dp_df['r9a_dp']  = dp_df['r9a_r10a_lp']  - dp_df['r10a_r9a_rp']
        dp_df['r13a_dp'] = dp_df['r13a_r10a_lp'] - dp_df['r10a_r13a_rp']

        st.caption(f"📅 {dp_from.strftime('%d-%b-%Y')} → {dp_to.strftime('%d-%b-%Y')}")
        fig3 = go.Figure()
        for col,name,color in [
            ('r7a_dp','R7A ΔP','#00b4d8'),
            ('r9a_dp','R9A ΔP','#f4a261'),
            ('r13a_dp','R13A ΔP','#2a9d8f')]:
            fig3.add_trace(go.Scatter(x=dp_df['timestamp'], y=dp_df[col],
                                      name=name, line=dict(color=color)))
        fig3.update_layout(**CHART, title='ΔP (L/P − R/P)', height=340,
                           yaxis_title='ΔP (KSC)',
                           xaxis=dict(tickformat='%d-%b-%Y',tickangle=-45))
        fig3.update_xaxes(**GRID); fig3.update_yaxes(**GRID)
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No data in selected range.")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — EARLY WARNING
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Early Warning":
    st.title("⚠️ Early Warning System")
    st.caption("Automated alerts based on threshold monitoring")

    alerts = []
    esp_df = load_esp_data()
    if not esp_df.empty:
        le = esp_df.sort_values('timestamp').groupby(
            'well_name').last().reset_index()
        for _, row in le.iterrows():
            temp = row.get('motor_temp_1_c')
            it   = row.get('pump_intake_temp_c')

            if temp and not pd.isna(temp):
                if temp > 150:
                    alerts.append({'Level':'🔴 CRITICAL','Well':row['well_name'],
                        'Parameter':'Motor Temperature','Value':f"{temp:.1f}°C",
                        'Threshold':'>150°C','Action':'Check gas lock/overload immediately'})
                elif temp > 135:
                    alerts.append({'Level':'🟡 WARNING','Well':row['well_name'],
                        'Parameter':'Motor Temperature','Value':f"{temp:.1f}°C",
                        'Threshold':'>135°C','Action':'Monitor closely'})

            if temp and it and not pd.isna(temp) and not pd.isna(it):
                dt = temp - it
                if dt > 45:
                    alerts.append({'Level':'🔴 CRITICAL','Well':row['well_name'],
                        'Parameter':'Motor ΔT','Value':f"{dt:.1f}°C",
                        'Threshold':'>45°C','Action':'Possible gas lock or scale'})
                elif dt > 35:
                    alerts.append({'Level':'🟡 WARNING','Well':row['well_name'],
                        'Parameter':'Motor ΔT','Value':f"{dt:.1f}°C",
                        'Threshold':'>35°C','Action':'Motor running hot'})

            ca = row.get('motor_current_a_amp')
            cb = row.get('motor_current_b_amp')
            cc = row.get('motor_current_c_amp')
            if all(v and not pd.isna(v) for v in [ca,cb,cc]):
                avg = (ca+cb+cc)/3
                if avg > 0:
                    imb = max(abs(ca-avg),abs(cb-avg),abs(cc-avg))/avg*100
                    if imb > 10:
                        alerts.append({'Level':'🔴 CRITICAL','Well':row['well_name'],
                            'Parameter':'Phase Imbalance','Value':f"{imb:.1f}%",
                            'Threshold':'>10%','Action':'Plan megger test'})
                    elif imb > 5:
                        alerts.append({'Level':'🟡 WARNING','Well':row['well_name'],
                            'Parameter':'Phase Imbalance','Value':f"{imb:.1f}%",
                            'Threshold':'>5%','Action':'Monitor cable health'})

            freq = row.get('vfd_output_frequency_hz')
            if freq and not pd.isna(freq):
                if freq > 60:
                    alerts.append({'Level':'🔴 CRITICAL','Well':row['well_name'],
                        'Parameter':'VFD Frequency','Value':f"{freq:.1f} Hz",
                        'Threshold':'>60 Hz','Action':'Reduce frequency immediately'})
                elif 0 < freq < 40:
                    alerts.append({'Level':'🟡 WARNING','Well':row['well_name'],
                        'Parameter':'VFD Frequency','Value':f"{freq:.1f} Hz",
                        'Threshold':'<40 Hz','Action':'Check inflow'})

    # Pressure checks use last 3 days
    prf = load_all_pressure()
    if not prf.empty:
        prf['timestamp'] = pd.to_datetime(prf['timestamp'])
        prf = prf[prf['timestamp'] >= prf['timestamp'].max() - pd.Timedelta(days=3)]
        for col,label in [
            ('r7a_r10a_lp','R7A Launcher'),
            ('r9a_r10a_lp','R9A Launcher'),
            ('r13a_r10a_lp','R13A Launcher'),
            ('r10a_hra_lp','R10A→HRA'),
            ('r12a_hra_lp','R12A→HRA')]:
            if col in prf.columns:
                rc = prf[col].dropna()
                if len(rc) >= 4:
                    tr = rc.iloc[-1] - rc.iloc[-4]
                    if tr > 5:
                        alerts.append({'Level':'🔴 CRITICAL','Well':'Field',
                            'Parameter':f'{label} Rising Fast',
                            'Value':f"+{tr:.1f} KSC",'Threshold':'>5 KSC',
                            'Action':'Check for wax/hydrate'})
                    elif tr > 3:
                        alerts.append({'Level':'🟡 WARNING','Well':'Field',
                            'Parameter':f'{label} Rising',
                            'Value':f"+{tr:.1f} KSC",'Threshold':'>3 KSC',
                            'Action':'Monitor back pressure'})

    if alerts:
        adf = pd.DataFrame(alerts)
        adf['sort'] = adf['Level'].apply(lambda x: 0 if 'CRITICAL' in x else 1)
        adf = adf.sort_values('sort').drop('sort', axis=1)
        st.dataframe(adf, use_container_width=True, hide_index=True)
        crit = sum(1 for a in alerts if 'CRITICAL' in a['Level'])
        warn = sum(1 for a in alerts if 'WARNING'  in a['Level'])
        if crit: st.error(f"🔴 {crit} CRITICAL alert(s) — immediate action required!")
        if warn: st.warning(f"🟡 {warn} WARNING alert(s) — monitor closely")
    else:
        st.success("✅ All parameters within normal range.")

    st.divider()
    st.subheader("⚙️ Alert Thresholds")
    st.info("""
    **Motor Temperature:** Warning >135°C | Critical >150°C (Trip)
    **Motor ΔT:** Warning >35°C | Critical >45°C above intake
    **Phase Imbalance:** Warning >5% | Critical >10%
    **VFD Frequency:** Warning <40 Hz or >60 Hz
    **Pipeline Pressure:** Warning >3 KSC rise | Critical >5 KSC rise
    """)