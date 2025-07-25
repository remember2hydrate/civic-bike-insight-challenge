import base64, json
import streamlit as st
import pandas as pd
import os
import time
import pytz
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from data_pipeline import fetch_raw_data, clean_data, upload_to_bigquery, load_credentials

PROJECT_ID = "civic-bike-data-challenge"
DATASET_ID = "bike_data"
TABLE_ID = "traffic_counts"

def load_data_with_retries(max_retries=4):
    for attempt in range(1, max_retries + 1):
        try:
            encoded_key = st.secrets["GCP_CREDENTIALS_B64"]
            credentials_info = json.loads(base64.b64decode(encoded_key).decode("utf-8"))
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

            query = f"""
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE timestamp IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 10000
            """
            return client.query(query).to_dataframe()
        except NotFound:
            st.warning(f"Attempt {attempt}: BigQuery table not found. Running ETL pipeline...")
            try:
                creds = load_credentials()
                df_raw = fetch_raw_data()
                df_clean = clean_data(df_raw)
                upload_to_bigquery(df_clean, creds)
                time.sleep(2)
            except Exception as e:
                st.error(f"ETL failed: {e}")
        except Exception as e:
            st.warning(f"Attempt {attempt}: Load failed: {e}")
            time.sleep(2)
    st.error("All attempts to load data failed.")
    return pd.DataFrame()

st.title("🚲 Copenhagen Bike Traffic Dashboard")
st.markdown("This dashboard visualizes bike count data from open civic datasets.")

df = load_data_with_retries()

if df.empty:
    st.stop()

min_date = df["timestamp"].min()
max_date = df["timestamp"].max()
date_range = st.date_input("Select date range", [min_date, max_date])

if len(date_range) == 2:
    # Make sure both ends of the range are timezone-aware in UTC
    start_date = pd.to_datetime(date_range[0]).tz_localize("UTC")
    end_date = pd.to_datetime(date_range[1]).tz_localize("UTC")
    df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= end_date)]

st.subheader("📊 Key Stats")
col1, col2 = st.columns(2)
col1.metric("Total Records", len(df))
col2.metric("Total Bike Count", int(df["bike_count"].sum()))

st.subheader("📈 Bike Counts Over Time")
df_hourly = df.copy()
df_hourly["hour"] = df_hourly["timestamp"].dt.floor("H")
hourly_counts = df_hourly.groupby("hour")["bike_count"].sum().reset_index()
st.line_chart(hourly_counts.rename(columns={"hour": "index"}).set_index("index"))

st.subheader("🚦 Top Streets by Bike Count")
top_streets = df.groupby("street_name")["bike_count"].sum().sort_values(ascending=False).head(10)
st.bar_chart(top_streets)

st.download_button("Download Displayed Data", df.to_csv(index=False), file_name="bike_data_filtered.csv")
st.caption("Data source: [Opendata.dk](https://admin.opendata.dk)")