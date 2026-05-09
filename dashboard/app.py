import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Page config
st.set_page_config(page_title="Patent Insights", layout="wide", page_icon="📘")

# Custom CSS – clean, magazine style
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 300;
        letter-spacing: -0.5px;
        margin-bottom: 0;
        color: #1e3a8a;
    }
    .subheader {
        font-size: 1rem;
        color: #4b5563;
        margin-top: 0;
        border-bottom: 1px solid #e5e7eb;
        padding-bottom: 1rem;
    }
    .insight-box {
        background-color: #f0f9ff;
        border-left: 4px solid #1e3a8a;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1.5rem;
    }
    .stat-number {
        font-size: 1.8rem;
        font-weight: 600;
        color: #1e3a8a;
        line-height: 1;
    }
    .stat-label {
        font-size: 0.8rem;
        color: #6b7280;
        text-transform: uppercase;
    }
</style>
""", unsafe_allow_html=True)

# -------------------- DATA LOADING --------------------
@st.cache_data
def load_data():
    base_path = Path(__file__).parent.parent / "output"
    patents = pd.read_csv(base_path / "clean_patents.csv")
    inventors = pd.read_csv(base_path / "clean_inventors.csv")
    companies = pd.read_csv(base_path / "clean_companies.csv")
    top_inventors = pd.read_csv(base_path / "top_inventors.csv")
    top_companies = pd.read_csv(base_path / "top_companies.csv")
    country_trends = pd.read_csv(base_path / "country_trends.csv")
    return patents, inventors, companies, top_inventors, top_companies, country_trends

patents, inventors, companies, top_inventors, top_companies, country_trends = load_data()

# Data prep
patents['year'] = pd.to_numeric(patents['year'], errors='coerce')
patents = patents.dropna(subset=['year'])
patents['year'] = patents['year'].astype(int)
min_year, max_year = patents['year'].min(), patents['year'].max()

# Year slider
st.sidebar.title("Filter")
year_range = st.sidebar.slider("Select Year Range", min_year, max_year, (min_year, max_year))
filtered_patents = patents[(patents['year'] >= year_range[0]) & (patents['year'] <= year_range[1])]
yearly = filtered_patents.groupby('year').size().reset_index(name='count')

# -------------------- HEADER (story) --------------------
st.markdown('<div class="main-header">Patent Insights</div>', unsafe_allow_html=True)
st.markdown('<div class="subheader">A data‑driven look at innovation trends</div>', unsafe_allow_html=True)

# Insight box – key takeaway
# -------------------- REAL INSIGHT (data driven) --------------------
if len(yearly) > 1:
    # Insight 1: Year with highest patent count
    peak_year = yearly.loc[yearly['count'].idxmax(), 'year']
    peak_count = yearly['count'].max()
    
    # Insight 2: Country with most patents overall (from country_trends)
    top_country = country_trends.iloc[0]['country']
    top_country_patents = country_trends.iloc[0]['patent_count']
    
    # Insight 3: Top inventor overall (from top_inventors)
    top_inventor = top_inventors.iloc[0]['name']
    top_inventor_patents = top_inventors.iloc[0]['patent_count']
    
    # Choose the most interesting insight dynamically
    # For example, if peak_year is not the last year, highlight the peak
    if peak_year < year_range[1]:
        insight_text = f"Patent activity peaked in {peak_year} – with {peak_count/yearly['count'].sum()*100:.1f}% of all patents in the selected period."
    else:
        insight_text = f"The most prolific inventor in this period is {top_inventor} with {top_inventor_patents} patents."
    
    # Additional context: top country
    insight_text += f"The leading country is {top_country} with {top_country_patents} patents."
    
else:
    insight_text = "Only one year of data available. Select a wider year range to see trends."

st.markdown(f"""
<div class="insight-box">
    <strong>Key insight</strong><br>
    {insight_text}
</div>
""", unsafe_allow_html=True)

# -------------------- ROW 1: Line chart + annotations --------------------
st.subheader("Patent volume over time")
fig_line = px.line(yearly, x='year', y='count', markers=True,
                   labels={'year': 'Year', 'count': 'Number of patents'})
fig_line.update_traces(line=dict(color='#1e3a8a', width=2), marker=dict(size=8, color='#1e3a8a'))
if len(yearly) > 1:
    # Annotate the highest year
    max_count = yearly['count'].max()
    max_year_anno = yearly[yearly['count'] == max_count]['year'].values[0]
    fig_line.add_annotation(x=max_year_anno, y=max_count,
                            text=f"Peak: {max_count} patents",
                            showarrow=True, arrowhead=1,
                            ax=20, ay=-30, font=dict(size=11))
fig_line.update_layout(plot_bgcolor='white', title_font_size=14, margin=dict(l=40, r=40, t=40, b=40))
st.plotly_chart(fig_line, use_container_width=True)

# -------------------- ROW 2: Two columns – Inventors & Companies (side by side) --------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 10 inventors")
    fig_inv = px.bar(top_inventors.head(10), x='patent_count', y='name', orientation='h',
                     text='patent_count', labels={'patent_count': 'Patents', 'name': ''})
    fig_inv.update_traces(marker_color='#1e3a8a', textposition='outside')
    fig_inv.update_layout(plot_bgcolor='white', height=400, margin=dict(l=0, r=0))
    st.plotly_chart(fig_inv, use_container_width=True)

with col2:
    st.subheader("Top 10 companies")
    fig_comp = px.bar(top_companies.head(10), x='patent_count', y='name', orientation='h',
                      text='patent_count', labels={'patent_count': 'Patents', 'name': ''})
    fig_comp.update_traces(marker_color='#1e3a8a', textposition='outside')
    fig_comp.update_layout(plot_bgcolor='white', height=400, margin=dict(l=0, r=0))
    st.plotly_chart(fig_comp, use_container_width=True)

# -------------------- ROW 3: Top countries (bar chart, but with a twist: show share) --------------------
st.subheader("Where inventors come from")
# Add share percentage
country_trends['share'] = 100 * country_trends['patent_count'] / country_trends['patent_count'].sum()
top10_countries = country_trends.head(10).copy()
fig_country = px.bar(top10_countries, x='patent_count', y='country', orientation='h',
                     text='patent_count', labels={'patent_count': 'Patents', 'country': ''})
fig_country.update_traces(marker_color='#1e3a8a', textposition='outside')
fig_country.update_layout(plot_bgcolor='white', height=450)
st.plotly_chart(fig_country, use_container_width=True)
# Small note
st.caption(f"The top 10 countries account for {top10_countries['share'].sum():.1f}% of all inventor‑linked patents.")

# -------------------- ROW 4: Raw data preview (required) --------------------
st.subheader("Raw data samples")
tab1, tab2, tab3 = st.tabs(["Patents", "Inventors", "Companies"])
with tab1:
    st.dataframe(filtered_patents.head(100), use_container_width=True)
with tab2:
    st.dataframe(inventors.head(100), use_container_width=True)
with tab3:
    st.dataframe(companies.head(100), use_container_width=True)

st.markdown("---")
st.caption("Source: USPTO PatentsView (disambiguated grant data) | Dashboard: Streamlit + Plotly")