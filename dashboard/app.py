import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Patent Dashboard", layout="wide")
st.title("📊 Patent Data Dashboard")

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

# --- Validate data ---
if patents.empty or 'year' not in patents.columns or patents['year'].isna().all():
    st.error("Patents data is empty or missing 'year' column. Please check clean_patents.csv")
    st.stop()

min_year = int(patents['year'].min())
max_year = int(patents['year'].max())

# --- Sidebar filter ---
st.sidebar.header("Filters")

if min_year == max_year:
    year_range = (min_year, min_year)
    st.sidebar.info(f"Data only available for {min_year}")
else:
    year_range = st.sidebar.slider(
        "Select Year Range",
        min_year, max_year,
        (min_year, max_year)
    )

filtered_patents = patents[(patents['year'] >= year_range[0]) & (patents['year'] <= year_range[1])]

# --- Plots (unchanged beyond this point) ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("--Patents Over Time--")
    yearly_counts = filtered_patents.groupby('year').size().reset_index(name='count')
    fig = px.line(yearly_counts, x='year', y='count', markers=True, title="Patents per Year")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("--Top 10 Inventors--")
    fig = px.bar(top_inventors.head(10), x='name', y='patent_count', title="Most Patents")
    st.plotly_chart(fig, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("--Top 10 Companies--")
    fig = px.bar(top_companies.head(10), x='name', y='patent_count', title="Most Patents Owned")
    st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("--Top 10 Countries--")
    fig = px.bar(country_trends.head(10), x='country', y='patent_count', title="Patents by Inventor Country")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("--Raw Data Preview--")
tab1, tab2, tab3 = st.tabs(["Patents", "Inventors", "Companies"])
with tab1:
    st.dataframe(filtered_patents.head(100))
with tab2:
    st.dataframe(inventors.head(100))
with tab3:
    st.dataframe(companies.head(100))

st.markdown("---")
st.caption("NAMEMBWA SHERRY | Dashboard built with Streamlit")