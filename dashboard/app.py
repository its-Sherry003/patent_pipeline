"""
Global Patent Intelligence Dashboard
=====================================
Run from the project root with:
    streamlit run dashboard/dash.py

Project layout expected:
    PATENT_PIPELINE/
    ├── dashboard/
    │   └── dash.py          <- this file
    └── output/
        ├── clean_patents.csv
        ├── clean_inventors.csv
        ├── clean_companies.csv
        ├── country_trends.csv
        ├── top_inventors.csv
        ├── top_companies.csv
        └── report.json
"""

import streamlit as st
import pandas as pd
import json
import os
import numpy as np
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline

# ── Resolve output/ folder ────────────────────────────────────────────────────
_HERE       = os.path.dirname(os.path.abspath(__file__))
_PROJECT    = os.path.dirname(_HERE)
_OUTPUT_DIR = os.path.join(_PROJECT, "output")

def data_path(filename):
    return os.path.join(_OUTPUT_DIR, filename)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Global Patent Intelligence",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=DM+Mono:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'DM Mono', monospace; }

.stApp { background: #f5f2eb; color: #1e2a1e; }

section[data-testid="stSidebar"] {
    background: #1b3a2d !important;
    border-right: 3px solid #c9a84c;
}
section[data-testid="stSidebar"] * { color: #d4e8d8 !important; }
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 { color: #f0e0a0 !important; }

h1, h2, h3 { font-family: 'Syne', sans-serif !important; letter-spacing: -0.02em; }
h1 { color: #1b3a2d !important; font-weight: 800 !important; }
h2 { color: #1b3a2d !important; }
h3 { color: #2d5a3d !important; }

.stApp p, .stApp span { color: #3a4a3a; }

.stTabs [data-baseweb="tab-list"] {
    gap: 4px; background: #e8e2d4; border-radius: 10px; padding: 4px;
    border: 1px solid #d4c8a8;
}
.stTabs [data-baseweb="tab"] {
    background: transparent; color: #5a6a5a;
    border-radius: 8px; font-family: 'DM Mono', monospace; font-size: 12px;
}
.stTabs [aria-selected="true"] {
    background: #1b3a2d !important; color: #f0e0a0 !important;
}
.stTabs [aria-selected="true"] p,
.stTabs [aria-selected="true"] span,
.stTabs [aria-selected="true"] div {
    color: #f0e0a0 !important;
}

.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: #ffffff !important;
    border: 1px solid #c9a84c !important;
    color: #1e2a1e !important;
}

hr { border-color: #d4c8a8; }

.banner {
    background: linear-gradient(135deg, #1b3a2d 0%, #2d5a3d 100%);
    border-left: 5px solid #c9a84c;
    border-radius: 14px;
    padding: 32px 36px;
    margin-bottom: 28px;
    box-shadow: 0 4px 24px rgba(27,58,45,0.13);
}
.banner-title {
    font-family: 'Syne', sans-serif;
    font-size: 2.2rem; font-weight: 800;
    color: #f5f2eb; letter-spacing: -0.03em; margin: 0; line-height: 1.1;
}
.banner-sub {
    font-family: 'DM Mono', monospace; font-size: 0.78rem;
    color: #c9a84c; margin-top: 8px; letter-spacing: 0.1em; text-transform: uppercase;
}

.report-card {
    background: #ffffff;
    border: 1px solid #d4c8a8;
    border-top: 3px solid #c9a84c;
    border-radius: 12px;
    padding: 16px 20px;
    margin-top: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}
.report-label {
    font-family: 'DM Mono', monospace; font-size: 10px;
    color: #7a8a7a; text-transform: uppercase; letter-spacing: 0.12em;
}
.report-value {
    font-family: 'Syne', sans-serif; font-size: 1.5rem;
    font-weight: 800; color: #1b3a2d;
}

/* Leaderboard — always visible, light background */
.lb-wrap {
    background: #ffffff;
    border: 1px solid #d4c8a8;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}
.lb-row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 10px 16px; border-bottom: 1px solid #eee8d8;
    transition: background 0.15s;
}
.lb-row:last-child { border-bottom: none; }
.lb-row:hover { background: #f0ece2; }
.lb-rank { font-family: 'Syne', sans-serif; font-weight: 700; color: #c9a84c; min-width: 36px; font-size: 14px; }
.lb-name { font-family: 'DM Mono', monospace; font-size: 11px; color: #1e2a1e; flex: 1; padding: 0 10px; }
.lb-count { font-family: 'Syne', sans-serif; font-weight: 800; color: #1b3a2d; font-size: 14px; }

/* Predictive insight cards */
.insight-box {
    background: #ffffff;
    border: 1px solid #d4c8a8;
    border-left: 4px solid #1b3a2d;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 12px;
}
.insight-label { font-size: 10px; color: #7a8a7a; text-transform: uppercase; letter-spacing: 0.1em; font-family: 'DM Mono', monospace; }
.insight-value { font-family: 'Syne', sans-serif; font-size: 1.6rem; font-weight: 800; color: #1b3a2d; }
.insight-note  { font-size: 11px; color: #5a6a5a; margin-top: 4px; font-family: 'DM Mono', monospace; }
</style>
""", unsafe_allow_html=True)

# ── Plotly theme ──────────────────────────────────────────────────────────────
PLOT_BG  = "#faf8f2"
PAPER_BG = "#f5f2eb"
GRID_CLR = "#e0d8c8"
TEXT_CLR = "#3a4a3a"
ACCENT   = "#1b3a2d"   # forest green — ALL bars use this
ACCENT2  = "#c9a84c"   # gold
ACCENT3  = "#4a8c5c"   # medium green
ACCENT4  = "#7bb08a"   # light green
PALETTE  = [ACCENT, ACCENT2, ACCENT3, ACCENT4, "#b07a3a", "#5a7a6a", "#9a6a2a", "#3a6a4a"]

def apply_theme(fig, title=""):
    fig.update_layout(
        plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG,
        font=dict(family="DM Mono, monospace", color=TEXT_CLR, size=11),
        title=dict(text=title, font=dict(family="Syne, sans-serif", color="#1b3a2d", size=15), x=0.02),
        xaxis=dict(gridcolor=GRID_CLR, linecolor=GRID_CLR, tickcolor=GRID_CLR, color=TEXT_CLR),
        yaxis=dict(gridcolor=GRID_CLR, linecolor=GRID_CLR, tickcolor=GRID_CLR, color=TEXT_CLR),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=GRID_CLR, font=dict(color=TEXT_CLR)),
        margin=dict(l=16, r=16, t=48, b=16),
        colorway=PALETTE,
    )
    return fig

def leaderboard_html(df, name_col="name", count_col="patents", top=10):
    medals = ["🥇", "🥈", "🥉"]
    rows = ""
    for i, row in df.head(top).reset_index(drop=True).iterrows():
        rank = medals[i] if i < 3 else f"#{i+1}"
        rows += (
            f"<div class='lb-row'>"
            f"<span class='lb-rank'>{rank}</span>"
            f"<span class='lb-name'>{row[name_col]}</span>"
            f"<span class='lb-count'>{int(row[count_col]):,}</span>"
            f"</div>"
        )
    return f"<div class='lb-wrap'>{rows}</div>"

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_csv(name, required_cols):
    path = data_path(name)
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, low_memory=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        for target, aliases in required_cols.items():
            if target not in df.columns:
                for alias in aliases:
                    if alias in df.columns:
                        df = df.rename(columns={alias: target})
                        break
        return df
    except Exception as e:
        st.warning(f"Could not load {name}: {e}")
        return None

@st.cache_data(show_spinner=False)
def load_patent_yearly_counts():
    path = data_path("clean_patents.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    chunksize = 50000
    yearly_counts = {}
    for chunk in pd.read_csv(path, usecols=['year'], chunksize=chunksize, low_memory=False):
        chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]
        if 'year' in chunk.columns:
            years = chunk['year'].dropna().astype(int)
            for y in years:
                yearly_counts[y] = yearly_counts.get(y, 0) + 1
    if not yearly_counts:
        return pd.DataFrame()
    df = pd.DataFrame(list(yearly_counts.items()), columns=['year', 'patents'])
    return df.sort_values('year').reset_index(drop=True)

@st.cache_data(show_spinner=False)
def load_json(name):
    path = data_path(name)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading patent data…"):
    inventors = load_csv("clean_inventors.csv", {"inventor_id":["id"],"name":["inventor_name","full_name"],"country":["inventor_country","country_code"]})
    companies = load_csv("clean_companies.csv", {"company_id":["id"],"name":["company_name","assignee","assignee_name"]})
    top_inv   = load_csv("top_inventors.csv",   {"name":["inventor_name","full_name"],"patents":["patent_count","count","num_patents"]})
    top_comp  = load_csv("top_companies.csv",   {"name":["company_name","assignee_name","assignee"],"patents":["patent_count","count","num_patents"]})
    countries = load_csv("country_trends.csv",  {"country":["country_code","inventor_country"],"patents":["patent_count","count","num_patents"],"year":["grant_year","filing_year"]})
    report    = load_json("report.json")

for df in [top_inv, top_comp, countries]:
    if df is not None and 'patents' in df.columns:
        df['patents'] = df['patents'].astype(str).str.replace(',', '').str.strip()
        df['patents'] = pd.to_numeric(df['patents'], errors='coerce')
        df.dropna(subset=['patents'], inplace=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding:8px 0 20px 0'>
      <div style='font-family:Syne,sans-serif;font-size:1.1rem;font-weight:800;color:#c9a84c'>⚙ PatentScope</div>
      <div style='font-size:10px;color:#4a6a5a;letter-spacing:0.1em;text-transform:uppercase'>Global Intelligence</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**Navigation**")
    page = st.radio("Navigation", [
        "📊 Overview",
        "🏆 Top Inventors",
        "🏢 Top Companies",
        "🌍 Countries",
        "📈 Trends Over Time",
        "🔮 Predictive Analysis",
    ], label_visibility="hidden")

    st.markdown("---")
    st.markdown("""
    <div style='font-size:10px;color:#4a6a5a;line-height:1.8'>
    <b style='color:#8aaa9a'>Data source</b><br>
    USPTO PatentsView<br>
    1976 – 2025 <br><br>
    Namembwa Sherry
    </div>
    """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊 Overview":
    st.markdown("""
    <div class="banner">
      <div class="banner-title">Global Patent Intelligence</div>
      <div class="banner-sub">Data Pipeline · USPTO PatentsView · 1976 – 2025</div>
    </div>
    """, unsafe_allow_html=True)

    col_l, col_r = st.columns(2)

    with col_l:
        yr_counts = load_patent_yearly_counts()
        if not yr_counts.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=yr_counts["year"], y=yr_counts["patents"],
                mode="lines", fill="tozeroy",
                line=dict(color=ACCENT, width=2.5),
                fillcolor="rgba(27,58,45,0.10)",
                name="Grants/year"
            ))
            apply_theme(fig, "Patent Grants per Year")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("clean_patents.csv not found or missing 'year' column.")

    with col_r:
        if countries is not None and "country" in countries.columns and "patents" in countries.columns:
            cdf = countries.groupby("country")["patents"].sum().nlargest(10).reset_index()
            fig2 = go.Figure(go.Bar(
                x=cdf["patents"], y=cdf["country"],
                orientation="h",
                marker=dict(color=ACCENT, opacity=0.85)
            ))
            apply_theme(fig2, "Top 10 Countries by Patent Volume")
            fig2.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("country_trends.csv not found.")

    if report:
        st.markdown("---")
        st.markdown("### Report Snapshot")
        scalars = {k: v for k, v in report.items() if not isinstance(v, (list, dict))}
        if scalars:
            cols = st.columns(min(len(scalars), 4))
            for i, (k, v) in enumerate(scalars.items()):
                with cols[i % len(cols)]:
                    label = k.replace("_", " ").title()
                    val = f"{v:,}" if isinstance(v, int) else (f"{v:,.2f}" if isinstance(v, float) else str(v))
                    st.markdown(
                        f"<div class='report-card'>"
                        f"<div class='report-label'>{label}</div>"
                        f"<div class='report-value'>{val}</div>"
                        f"</div>", unsafe_allow_html=True
                    )
        list_fields = {k: v for k, v in report.items() if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict)}
        for field, items in list_fields.items():
            st.markdown(f"#### {field.replace('_', ' ').title()}")
            df_r = pd.DataFrame(items)
            name_col  = next((c for c in df_r.columns if c in ["name","country","inventor","company"]), df_r.columns[0])
            value_col = next((c for c in df_r.columns if c != name_col), None)
            if value_col:
                fig_r = go.Figure(go.Bar(
                    x=df_r[value_col], y=df_r[name_col],
                    orientation="h",
                    marker=dict(color=ACCENT2, opacity=0.85)
                ))
                apply_theme(fig_r, "")
                fig_r.update_layout(
                    height=max(180, len(df_r) * 30),
                    yaxis=dict(autorange="reversed"),
                    margin=dict(l=8, r=8, t=8, b=8)
                )
                st.plotly_chart(fig_r, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: TOP INVENTORS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🏆 Top Inventors":
    st.markdown("## 🏆 Top Inventors")
    st.caption("Ranked by total patent count")

    n = st.slider("Show top N inventors", 5, 50, 20, key="inv_n")

    if top_inv is not None and "name" in top_inv.columns and "patents" in top_inv.columns:
        df = top_inv.nlargest(n, "patents").reset_index(drop=True)

        col_l, col_r = st.columns([3, 2])

        with col_l:
            fig = go.Figure(go.Bar(
                x=df["patents"], y=df["name"],
                orientation="h",
                marker=dict(color=ACCENT, opacity=0.85),
                text=df["patents"].apply(lambda v: f"{int(v):,}"),
                textposition="outside",
                textfont=dict(color=TEXT_CLR, size=10)
            ))
            apply_theme(fig, f"Top {n} Inventors by Patent Count")
            fig.update_layout(yaxis=dict(autorange="reversed"), height=max(350, n * 26))
            st.plotly_chart(fig, use_container_width=True)
            st.download_button("⬇ Download top_inventors.csv",
                               df.to_csv(index=False), "top_inventors.csv", "text/csv")

        with col_r:
            st.markdown("### Leaderboard")
            st.markdown(leaderboard_html(df), unsafe_allow_html=True)
    else:
        st.warning("top_inventors.csv not found or missing 'name'/'patents' columns.")

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: TOP COMPANIES
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🏢 Top Companies":
    st.markdown("## 🏢 Top Companies (Assignees)")
    st.caption("Ranked by total patent ownership")

    n = st.slider("Show top N companies", 5, 50, 20, key="comp_n")

    if top_comp is not None and "name" in top_comp.columns and "patents" in top_comp.columns:
        df = top_comp.nlargest(n, "patents").reset_index(drop=True)

        col_l, col_r = st.columns([3, 2])

        with col_l:
            fig = go.Figure(go.Bar(
                x=df["patents"], y=df["name"],
                orientation="h",
                marker=dict(color=ACCENT, opacity=0.85),
                text=df["patents"].apply(lambda v: f"{int(v):,}"),
                textposition="outside",
                textfont=dict(color=TEXT_CLR, size=10)
            ))
            apply_theme(fig, f"Top {n} Companies by Patent Count")
            fig.update_layout(yaxis=dict(autorange="reversed"), height=max(350, n * 26))
            st.plotly_chart(fig, use_container_width=True)
            st.download_button("⬇ Download top_companies.csv",
                               df.to_csv(index=False), "top_companies.csv", "text/csv")

        with col_r:
            st.markdown("### Leaderboard")
            st.markdown(leaderboard_html(df), unsafe_allow_html=True)
    else:
        st.warning("top_companies.csv not found or missing 'name'/'patents' columns.")

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: COUNTRIES
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🌍 Countries":
    st.markdown("## 🌍 Country Intelligence")
    st.caption("Which countries drive global innovation?")

    if countries is not None and "country" in countries.columns and "patents" in countries.columns:
        agg = countries.groupby("country")["patents"].sum().reset_index()
        agg = agg.sort_values("patents", ascending=False)

        n = st.slider("Top N countries", 5, 30, 15, key="cnt_n")
        top = agg.head(n)

        fig = go.Figure(go.Bar(
            x=top["patents"], y=top["country"],
            orientation="h",
            marker=dict(color=ACCENT, opacity=0.85),
            text=top["patents"].apply(lambda v: f"{int(v):,}"),
            textposition="outside",
            textfont=dict(color=TEXT_CLR, size=10)
        ))
        apply_theme(fig, f"Top {n} Countries – Total Patents")
        fig.update_layout(yaxis=dict(autorange="reversed"), height=max(320, n * 26))
        st.plotly_chart(fig, use_container_width=True)

        st.download_button("⬇ Download country_trends.csv",
                           agg.to_csv(index=False), "country_trends_agg.csv", "text/csv")

        if "year" in countries.columns:
            st.markdown("### Country Trends Over Time")
            top_c_list = agg["country"].head(8).tolist()
            sel = st.multiselect("Select countries to compare", top_c_list, default=top_c_list[:5])
            if sel:
                trend = countries[countries["country"].isin(sel)]
                trend = trend.groupby(["year", "country"])["patents"].sum().reset_index()
                trend["year"] = trend["year"].astype(int)
                fig3 = go.Figure()
                for i, c in enumerate(sel):
                    d = trend[trend["country"] == c].sort_values("year")
                    fig3.add_trace(go.Scatter(
                        x=d["year"], y=d["patents"],
                        mode="lines", name=c,
                        line=dict(color=PALETTE[i % len(PALETTE)], width=2.5)
                    ))
                apply_theme(fig3, "Patent Output by Country Over Time")
                st.plotly_chart(fig3, use_container_width=True)
    else:
        st.warning("country_trends.csv not found or missing required columns.")

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: TRENDS OVER TIME
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📈 Trends Over Time":
    st.markdown("## 📈 Innovation Trends Over Time")
    st.caption("How global patent output has evolved from 1976 to 2025")

    yr = load_patent_yearly_counts()
    if not yr.empty:
        yr["year"] = yr["year"].astype(int)
        yr = yr.sort_values("year")
        yr["cumulative"] = yr["patents"].cumsum()
        yr["rolling_5"] = yr["patents"].rolling(5, center=True).mean()

        tab1, tab2, tab3 = st.tabs(["Annual Trend", "Cumulative Growth", "5-Year Rolling Average"])

        with tab1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=yr["year"], y=yr["patents"],
                mode="lines", fill="tozeroy",
                line=dict(color=ACCENT, width=2.5),
                fillcolor="rgba(27,58,45,0.10)",
                name="Annual grants"
            ))
            apply_theme(fig, "Annual Patent Grants")
            fig.update_layout(xaxis_title="Year", yaxis_title="Patents Granted", hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)

        with tab2:
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(
                x=yr["year"], y=yr["cumulative"],
                mode="lines", fill="tozeroy",
                line=dict(color=ACCENT2, width=2.5),
                fillcolor="rgba(201,168,76,0.10)",
                name="Cumulative total"
            ))
            apply_theme(fig2, "Cumulative Patent Portfolio Growth")
            fig2.update_layout(xaxis_title="Year", yaxis_title="Total Patents (All-time)", hovermode="x unified")
            st.plotly_chart(fig2, use_container_width=True)

        with tab3:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=yr["year"], y=yr["patents"],
                mode="lines", fill="tozeroy",
                line=dict(color=ACCENT4, width=0.8, dash="dot"),
                fillcolor="rgba(74,140,92,0.07)",
                name="Annual (actual)"
            ))
            fig3.add_trace(go.Scatter(
                x=yr["year"], y=yr["rolling_5"],
                mode="lines",
                line=dict(color=ACCENT3, width=3),
                name="5-year rolling avg"
            ))
            apply_theme(fig3, "5-Year Rolling Average Trend")
            fig3.update_layout(xaxis_title="Year", yaxis_title="Patents Granted", hovermode="x unified")
            st.plotly_chart(fig3, use_container_width=True)

        st.download_button("⬇ Download yearly trend data",
                           yr[["year", "patents", "cumulative"]].to_csv(index=False),
                           "yearly_trend.csv", "text/csv")
    else:
        st.warning("clean_patents.csv not found or missing 'year' column.")

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: PREDICTIVE ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🔮 Predictive Analysis":
    st.markdown("## 🔮 Predictive Analysis")
    st.caption("Machine learning forecasts of future patent activity based on historical trends (1976–2025)")

    yr = load_patent_yearly_counts()

    if not yr.empty:
        yr["year"] = yr["year"].astype(int)
        yr = yr.sort_values("year")

        col_ctrl1, col_ctrl2 = st.columns(2)
        with col_ctrl1:
            horizon = st.slider("Forecast horizon (years ahead)", 3, 15, 10)
        with col_ctrl2:
            model_type = st.selectbox("Forecast model", [
                "Linear Regression",
                "Polynomial (degree 2)",
                "Polynomial (degree 3)",
            ])

        X = yr["year"].values.reshape(-1, 1)
        y = yr["patents"].values
        future_years = np.arange(yr["year"].max() + 1, yr["year"].max() + horizon + 1).reshape(-1, 1)

        if model_type == "Linear Regression":
            model = LinearRegression()
        elif model_type == "Polynomial (degree 2)":
            model = make_pipeline(PolynomialFeatures(2), LinearRegression())
        else:
            model = make_pipeline(PolynomialFeatures(3), LinearRegression())

        model.fit(X, y)
        y_pred_hist = model.predict(X)
        y_pred_fut  = model.predict(future_years)

        residuals = y - y_pred_hist
        std_resid = np.std(residuals)
        upper     = y_pred_fut + 1.5 * std_resid
        lower     = np.maximum(y_pred_fut - 1.5 * std_resid, 0)
        fut_flat  = future_years.flatten()

        # ── Insight cards ──
        next_pred  = int(y_pred_fut[0])
        end_pred   = int(y_pred_fut[-1])
        avg_growth = float(np.mean(np.diff(y_pred_fut)))
        r2         = model.score(X, y)
        direction  = "📈 Growing" if avg_growth > 0 else "📉 Declining"

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""<div class='insight-box'>
                <div class='insight-label'>Predicted {int(yr['year'].max())+1}</div>
                <div class='insight-value'>{next_pred:,}</div>
                <div class='insight-note'>patents expected</div>
            </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(f"""<div class='insight-box'>
                <div class='insight-label'>End of Horizon ({int(yr['year'].max())+horizon})</div>
                <div class='insight-value'>{end_pred:,}</div>
                <div class='insight-note'>patents forecast</div>
            </div>""", unsafe_allow_html=True)
        with c3:
            st.markdown(f"""<div class='insight-box'>
                <div class='insight-label'>Trend Direction</div>
                <div class='insight-value' style='font-size:1.1rem'>{direction}</div>
                <div class='insight-note'>avg {abs(avg_growth):,.0f} patents/yr change</div>
            </div>""", unsafe_allow_html=True)
        with c4:
            fit_label = "Excellent" if r2 > 0.9 else ("Good" if r2 > 0.7 else "Moderate")
            st.markdown(f"""<div class='insight-box'>
                <div class='insight-label'>Model Fit (R²)</div>
                <div class='insight-value'>{r2:.3f}</div>
                <div class='insight-note'>{fit_label} fit on historical data</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("---")

        # ── Main forecast chart ──
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=yr["year"], y=yr["patents"],
            mode="lines",
            line=dict(color=ACCENT, width=2),
            name="Historical (actual)"
        ))
        fig.add_trace(go.Scatter(
            x=yr["year"].tolist(), y=y_pred_hist.tolist(),
            mode="lines",
            line=dict(color=ACCENT2, width=1.5, dash="dash"),
            name="Model fit"
        ))
        fig.add_trace(go.Scatter(
            x=np.concatenate([fut_flat, fut_flat[::-1]]).tolist(),
            y=np.concatenate([upper, lower[::-1]]).tolist(),
            fill="toself",
            fillcolor="rgba(201,168,76,0.15)",
            line=dict(color="rgba(0,0,0,0)"),
            name="Confidence band"
        ))
        fig.add_trace(go.Scatter(
            x=fut_flat.tolist(), y=y_pred_fut.tolist(),
            mode="lines+markers",
            line=dict(color=ACCENT2, width=2.5),
            marker=dict(size=5, color=ACCENT2),
            name=f"Forecast ({model_type})"
        ))
        fig.add_vline(
            x=int(yr["year"].max()),
            line_dash="dot", line_color=GRID_CLR, line_width=1.5,
            annotation_text="Forecast starts",
            annotation_font=dict(color=TEXT_CLR, size=10),
            annotation_position="top right"
        )
        apply_theme(fig, f"Patent Grant Forecast — {int(yr['year'].max())+1} to {int(yr['year'].max())+horizon}")
        fig.update_layout(
            xaxis_title="Year", yaxis_title="Estimated Patents Granted",
            hovermode="x unified", height=450,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Year-over-year change forecast ──
        st.markdown("### Forecasted Year-by-Year Change")
        pred_df = pd.DataFrame({
            "Year": fut_flat,
            "Forecast": y_pred_fut.astype(int),
            "Lower": lower.astype(int),
            "Upper": upper.astype(int),
        })
        pred_df["YoY Change"] = pred_df["Forecast"].diff().fillna(0).astype(int)

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=pred_df["Year"], y=pred_df["YoY Change"],
            mode="lines+markers",
            line=dict(color=ACCENT3, width=2.5),
            marker=dict(size=6),
            fill="tozeroy",
            fillcolor="rgba(74,140,92,0.10)",
            name="Year-over-year change"
        ))
        fig2.add_hline(y=0, line_dash="dot", line_color=GRID_CLR)
        apply_theme(fig2, "Forecasted Annual Change in Patent Grants")
        fig2.update_layout(xaxis_title="Year", yaxis_title="Change vs Prior Year", hovermode="x unified")
        st.plotly_chart(fig2, use_container_width=True)

        st.download_button(
            "⬇ Download forecast data",
            pred_df.to_csv(index=False), "patent_forecast.csv", "text/csv"
        )

        st.markdown("""
        <div style='background:#fff;border:1px solid #d4c8a8;border-radius:10px;
        padding:14px 18px;margin-top:8px;font-size:12px;color:#5a6a5a'>
        <b style='color:#1b3a2d'>About this forecast</b><br>
        Trained on USPTO patent grant data (1976–2025).
        The confidence band shows ±1.5 standard deviations of the model's historical residuals.
        Polynomial models capture curves in the data but may drift over long horizons —
        Linear Regression is the most conservative and stable choice.
        </div>
        """, unsafe_allow_html=True)
    else:
        st.warning("clean_patents.csv not found or missing 'year' column — needed for forecasting.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center;font-size:10px;color:#8a9a8a;"
    "font-family:DM Mono,monospace;letter-spacing:0.08em'>"
    "GLOBAL PATENT INTELLIGENCE DASHBOARD · USPTO PatentsView · "
    "Built with Streamlit &amp; Plotly"
    "</div>",
    unsafe_allow_html=True
)