import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery
import os

# Set page config
st.set_page_config(page_title="NYC 311 Insights", layout="wide")

# Initialize BigQuery Client
project_id = os.getenv("GCP_PROJECT_ID")
region = os.getenv("GCP_REGION", "EU") 
client = bigquery.Client(project=project_id)

dataset_id = os.getenv("BQ_DATASET_ID", "NY_311_SERVICE_REQUESTS_DATASET")

@st.cache_data(ttl=600)
def load_data(table_name):
    # Fetch data from BigQuery with explicit location routing
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
    query_job = client.query(query, location=region)
    return query_job.to_dataframe()

st.title("NYC 311 Service Requests Dashboard")

# Sidebar navigation
report_type = st.sidebar.selectbox("Select Report Level", ["Monthly", "Yearly"])
table_map = {
    "Monthly": "rpt_ny_311_monthly_performance",
    "Yearly": "rpt_ny_311_annual_performance"
}

try:
    df = load_data(table_map[report_type])
    
    if df.empty:
        st.warning(f"Table {table_map[report_type]} is empty.")
    else:
        # Agency Filter - Best Practice: Default to empty = All
        st.sidebar.subheader("Filters")
        all_agencies = sorted(df['agency'].unique())
        selected_agencies = st.sidebar.multiselect(
            "Filter by Agency (Leave empty for All)",
            options=all_agencies,
            default=[]
        )

        # Date column logic
        date_col = 'report_month' if report_type == "Monthly" else 'report_year'

        # Filter Logic: If nothing selected, show all. Otherwise filter.
        if selected_agencies:
            df_filtered = df[df['agency'].isin(selected_agencies)].copy()
        else:
            df_filtered = df.copy()

        # Date Filtering
        if report_type != "Yearly":
            df_filtered[date_col] = pd.to_datetime(df_filtered[date_col])
            
            min_date = df_filtered[date_col].min().date()
            max_date = df_filtered[date_col].max().date()
            
            start_date, end_date = st.sidebar.slider(
                "Date Range",
                min_value=min_date,
                max_value=max_date,
                value=(min_date, max_date),
                format="YYYY-MM"
            )
            
            mask = (df_filtered[date_col].dt.date >= start_date) & (df_filtered[date_col].dt.date <= end_date)
            df_filtered = df_filtered[mask]

        # KPI Metrics
        total_reqs = df_filtered['total_requests'].sum()
        avg_res = df_filtered['avg_resolution_hours'].mean()
        
        col1, col2 = st.columns(2)
        col1.metric("Total Requests", f"{total_reqs:,}")
        col2.metric("Avg Resolution Time", f"{avg_res:.2f} Hours")

        # Visualizations
        st.subheader("Requests Over Time")
        time_df = df_filtered.groupby(date_col)['total_requests'].sum().reset_index().sort_values(date_col)
        st.line_chart(data=time_df, x=date_col, y='total_requests')

        st.subheader("Requests by Borough")
        boro_df = df_filtered.groupby('borough')['total_requests'].sum().sort_values(ascending=False)
        st.bar_chart(boro_df)

except Exception as e:
    st.error(f"BigQuery Error: {e}")