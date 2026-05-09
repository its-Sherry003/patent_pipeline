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
import plotly.express as px
import plotly.graph_objects as go

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

/* Warm off-white background, dark slate text */
.stApp { background: #f5f2eb; color: #1e2a1e; }

/* Sidebar — deep forest green */
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

/* Caption / small text */
.stApp p, .stApp span { color: #3a4a3a; }

/* Tabs */
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

/* Multiselect / selectbox */
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: #ffffff !important;
    border: 1px solid #c9a84c !important;
    color: #1e2a1e !important;
}

/* Slider */
.stSlider [data-baseweb="slider"] { color: #1b3a2d; }

hr { border-color: #d4c8a8; }

/* Banner */
.banner {
    background: linear-gradient(135deg, #1b3a2d 0%, #2d5a3d 100%);
    border: none;
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

/* Report card */
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

/* Leaderboard */
.lb-row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 9px 14px; border-bottom: 1px solid #e8e2d4;
    transition: background 0.15s;
}
.lb-row:hover { background: #f0ece2; }
.lb-rank { font-family: 'Syne', sans-serif; font-weight: 700; color: #c9a84c; min-width: 32px; }
.lb-name { font-family: 'DM Mono', monospace; font-size: 12px; color: #2d3a2d; flex: 1; padding: 0 8px; }
.lb-count { font-family: 'Syne', sans-serif; font-weight: 700; color: #1b3a2d; font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# ── Plotly theme ──────────────────────────────────────────────────────────────
PLOT_BG  = "#faf8f2"   # warm white
PAPER_BG = "#f5f2eb"   # matches app bg
GRID_CLR = "#e0d8c8"   # soft tan grid
TEXT_CLR = "#3a4a3a"   # dark green-grey text
ACCENT   = "#1b3a2d"   # forest green — primary bars / lines
ACCENT2  = "#c9a84c"   # gold — highlights
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
def load_json(name):
    path = data_path(name)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading patent data…"):
    patents   = load_csv("clean_patents.csv",   {"patent_id":["id"],"title":["patent_title"],"year":["grant_year","filing_year"],"filing_date":["date"]})
    inventors = load_csv("clean_inventors.csv", {"inventor_id":["id"],"name":["inventor_name","full_name"],"country":["inventor_country","country_code"]})
    companies = load_csv("clean_companies.csv", {"company_id":["id"],"name":["company_name","assignee","assignee_name"]})
    top_inv   = load_csv("top_inventors.csv",   {"name":["inventor_name","full_name"],"patents":["patent_count","count","num_patents"]})
    top_comp  = load_csv("top_companies.csv",   {"name":["company_name","assignee_name","assignee"],"patents":["patent_count","count","num_patents"]})
    countries = load_csv("country_trends.csv",  {"country":["country_code","inventor_country"],"patents":["patent_count","count","num_patents"],"year":["grant_year","filing_year"]})
    report    = load_json("report.json")

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
    ], label_visibility="hidden")

    st.markdown("---")

    # Year filter
    year_range = None
    if patents is not None and "year" in patents.columns:
        yrs = patents["year"].dropna().astype(int)
        mn, mx = int(yrs.min()), int(yrs.max())
        if mn < mx:
            year_range = st.slider("Year range", mn, mx, (mn, mx))

    st.markdown("---")
    st.markdown("""
    <div style='font-size:10px;color:#4a6a5a;line-height:1.8'>
    <b style='color:#8aaa9a'>Data source</b><br>
    USPTO PatentsView<br>
    1976 – 2025
    </div>
    """, unsafe_allow_html=True)

def filter_by_year(df):
    if df is None or year_range is None or "year" not in df.columns:
        return df
    return df[(df["year"].astype(float) >= year_range[0]) & (df["year"].astype(float) <= year_range[1])]

patents_f = filter_by_year(patents)

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

    # ── Patents per year – area/line chart ──
    with col_l:
        if patents_f is not None and "year" in patents_f.columns:
            yr_counts = patents_f.groupby("year").size().reset_index(name="patents")
            yr_counts["year"] = yr_counts["year"].astype(int)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=yr_counts["year"], y=yr_counts["patents"],
                mode="lines", fill="tozeroy",
                line=dict(color=ACCENT, width=2.5),
                fillcolor="rgba(56,189,248,0.10)",
                name="Grants/year"
            ))
            apply_theme(fig, "Patent Grants per Year")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("clean_patents.csv not found or missing 'year' column.")

    # ── Top countries – horizontal bar ──
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

    # ── Report snapshot — always expanded ──
    if report:
        st.markdown("---")
        st.markdown("### Report Snapshot")

        def _fmt(v):
            if isinstance(v, (int, float)):
                return f"{v:,.0f}" if isinstance(v, int) or v == int(v) else f"{v:,.4f}"
            return str(v)

        # Top-level scalar fields
        scalars = {k: v for k, v in report.items() if not isinstance(v, (list, dict))}
        if scalars:
            cols = st.columns(min(len(scalars), 4))
            for i, (k, v) in enumerate(scalars.items()):
                with cols[i % len(cols)]:
                    label = k.replace("_", " ").title()
                    st.markdown(
                        f"<div class='report-card' style='padding:14px 18px'>"
                        f"<div class='report-label'>{label}</div>"
                        f"<div class='report-value'>{_fmt(v)}</div>"
                        f"</div>",
                        unsafe_allow_html=True
                    )

        # List fields — render as mini horizontal bar charts
        list_fields = {k: v for k, v in report.items() if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict)}
        for field, items in list_fields.items():
            st.markdown(f"#### {field.replace('_', ' ').title()}")
            df_r = pd.DataFrame(items)
            # find name & value columns
            name_col  = next((c for c in df_r.columns if c in ["name","country","inventor","company"]), df_r.columns[0])
            value_col = next((c for c in df_r.columns if c not in [name_col]), None)
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
            else:
                for item in items[:10]:
                    st.markdown(f"- {item}")

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
                marker=dict(
                    color=df["patents"],
                    colorscale=[[0, "#1e2060"], [1, ACCENT2]],
                    showscale=False
                ),
                text=df["patents"].apply(lambda v: f"{int(v):,}"),
                textposition="outside",
                textfont=dict(color=TEXT_CLR, size=10)
            ))
            apply_theme(fig, f"Top {n} Inventors by Patent Count")
            fig.update_layout(
                yaxis=dict(autorange="reversed"),
                height=max(350, n * 26)
            )
            st.plotly_chart(fig, use_container_width=True)

            st.download_button(
                "⬇ Download top_inventors.csv",
                df.to_csv(index=False), "top_inventors.csv", "text/csv"
            )

        with col_r:
            st.markdown("### Leaderboard")
            medals = ["🥇", "🥈", "🥉"]
            html_rows = ""
            for i, row in df.head(10).iterrows():
                rank = medals[i] if i < 3 else f"#{i+1}"
                html_rows += (
                    f"<div class='lb-row'>"
                    f"<span class='lb-rank'>{rank}</span>"
                    f"<span class='lb-name'>{row['name']}</span>"
                    f"<span class='lb-count'>{int(row['patents']):,}</span>"
                    f"</div>"
                )
            st.markdown(
                f"<div style='background:#0f1629;border:1px solid #1e3a5f;"
                f"border-radius:12px;overflow:hidden'>{html_rows}</div>",
                unsafe_allow_html=True
            )

            # Donut – share of top 5 vs rest
            top5 = df.head(5)
            rest_val = df.iloc[5:]["patents"].sum() if len(df) > 5 else 0
            pie_names = list(top5["name"]) + (["Others"] if rest_val > 0 else [])
            pie_vals  = list(top5["patents"]) + ([rest_val] if rest_val > 0 else [])
            fig_p = go.Figure(go.Pie(
                labels=pie_names, values=pie_vals,
                hole=0.5,
                marker=dict(colors=PALETTE),
                textfont=dict(size=10)
            ))
            apply_theme(fig_p, "Share — Top 5 vs Others")
            fig_p.update_layout(
                showlegend=True,
                legend=dict(font=dict(size=9)),
                height=280, margin=dict(l=8, r=8, t=40, b=8)
            )
            st.plotly_chart(fig_p, use_container_width=True)

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
                marker=dict(
                    color=df["patents"],
                    colorscale=[[0, "#0d2a1a"], [1, ACCENT3]],
                    showscale=False
                ),
                text=df["patents"].apply(lambda v: f"{int(v):,}"),
                textposition="outside",
                textfont=dict(color=TEXT_CLR, size=10)
            ))
            apply_theme(fig, f"Top {n} Companies by Patent Count")
            fig.update_layout(
                yaxis=dict(autorange="reversed"),
                height=max(350, n * 26)
            )
            st.plotly_chart(fig, use_container_width=True)

            st.download_button(
                "⬇ Download top_companies.csv",
                df.to_csv(index=False), "top_companies.csv", "text/csv"
            )

        with col_r:
            # Leaderboard
            st.markdown("### Leaderboard")
            medals = ["🥇", "🥈", "🥉"]
            html_rows = ""
            for i, row in df.head(10).iterrows():
                rank = medals[i] if i < 3 else f"#{i+1}"
                html_rows += (
                    f"<div class='lb-row'>"
                    f"<span class='lb-rank'>{rank}</span>"
                    f"<span class='lb-name'>{row['name']}</span>"
                    f"<span class='lb-count'>{int(row['patents']):,}</span>"
                    f"</div>"
                )
            st.markdown(
                f"<div style='background:#0f1629;border:1px solid #1e3a5f;"
                f"border-radius:12px;overflow:hidden'>{html_rows}</div>",
                unsafe_allow_html=True
            )

            # Donut – share of top 5
            top5 = df.head(5)
            rest_val = df.iloc[5:]["patents"].sum() if len(df) > 5 else 0
            pie_names = list(top5["name"]) + (["Others"] if rest_val > 0 else [])
            pie_vals  = list(top5["patents"]) + ([rest_val] if rest_val > 0 else [])
            fig_p = go.Figure(go.Pie(
                labels=pie_names, values=pie_vals,
                hole=0.5,
                marker=dict(colors=PALETTE),
                textfont=dict(size=10)
            ))
            apply_theme(fig_p, "Share — Top 5 vs Others")
            fig_p.update_layout(
                showlegend=True,
                legend=dict(font=dict(size=9)),
                height=280, margin=dict(l=8, r=8, t=40, b=8)
            )
            st.plotly_chart(fig_p, use_container_width=True)

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

        col_l, col_r = st.columns([3, 2])

        with col_l:
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

            st.download_button(
                "⬇ Download country_trends.csv",
                agg.to_csv(index=False), "country_trends_agg.csv", "text/csv"
            )

        with col_r:
            top10 = agg.head(10).copy()
            other = agg.iloc[10:]["patents"].sum()
            if other > 0:
                top10 = pd.concat([top10, pd.DataFrame([{"country": "Other", "patents": other}])], ignore_index=True)
            fig2 = go.Figure(go.Pie(
                labels=top10["country"], values=top10["patents"],
                hole=0.45,
                marker=dict(colors=PALETTE),
                textfont=dict(size=10)
            ))
            apply_theme(fig2, "Global Patent Share")
            fig2.update_layout(height=340, margin=dict(l=8, r=8, t=48, b=8))
            st.plotly_chart(fig2, use_container_width=True)

        # Country trend lines over time
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

    if patents_f is not None and "year" in patents_f.columns:
        yr = patents_f.groupby("year").size().reset_index(name="patents")
        yr["year"] = yr["year"].astype(int)
        yr = yr.sort_values("year")
        yr["cumulative"] = yr["patents"].cumsum()
        yr["rolling_5"] = yr["patents"].rolling(5, center=True).mean()

        tab1, tab2, tab3 = st.tabs(["Annual Trend", "Cumulative Growth", "5-Year Rolling Average"])

        # ── Tab 1: Annual line + area ──
        with tab1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=yr["year"], y=yr["patents"],
                mode="lines", fill="tozeroy",
                line=dict(color=ACCENT, width=2.5),
                fillcolor="rgba(56,189,248,0.10)",
                name="Annual grants"
            ))
            apply_theme(fig, "Annual Patent Grants (Line)")
            fig.update_layout(
                xaxis_title="Year", yaxis_title="Patents Granted",
                hovermode="x unified"
            )
            st.plotly_chart(fig, use_container_width=True)

        # ── Tab 2: Cumulative area ──
        with tab2:
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(
                x=yr["year"], y=yr["cumulative"],
                mode="lines", fill="tozeroy",
                line=dict(color=ACCENT2, width=2.5),
                fillcolor="rgba(129,140,248,0.10)",
                name="Cumulative total"
            ))
            apply_theme(fig2, "Cumulative Patent Portfolio Growth")
            fig2.update_layout(
                xaxis_title="Year", yaxis_title="Total Patents (All-time)",
                hovermode="x unified"
            )
            st.plotly_chart(fig2, use_container_width=True)

        # ── Tab 3: Rolling average smooth line ──
        with tab3:
            fig3 = go.Figure()
            # Actual as faint fill
            fig3.add_trace(go.Scatter(
                x=yr["year"], y=yr["patents"],
                mode="lines", fill="tozeroy",
                line=dict(color=ACCENT3, width=0.8, dash="dot"),
                fillcolor="rgba(52,211,153,0.06)",
                name="Annual (actual)"
            ))
            # Rolling average bold line
            fig3.add_trace(go.Scatter(
                x=yr["year"], y=yr["rolling_5"],
                mode="lines",
                line=dict(color=ACCENT3, width=3),
                name="5-year rolling avg"
            ))
            apply_theme(fig3, "5-Year Rolling Average Trend")
            fig3.update_layout(
                xaxis_title="Year", yaxis_title="Patents Granted",
                hovermode="x unified"
            )
            st.plotly_chart(fig3, use_container_width=True)

        st.download_button(
            "⬇ Download yearly trend data",
            yr[["year", "patents", "cumulative"]].to_csv(index=False),
            "yearly_trend.csv", "text/csv"
        )
    else:
        st.warning("clean_patents.csv not found or missing 'year' column.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center;font-size:10px;color:#334155;"
    "font-family:DM Mono,monospace;letter-spacing:0.08em'>"
    "GLOBAL PATENT INTELLIGENCE DASHBOARD · USPTO PatentsView · "
    "Built with Streamlit &amp; Plotly"
    "</div>",
    unsafe_allow_html=True
)