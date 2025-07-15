import base64, json
import requests
import pandas as pd
import os
import time
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st

# Constants
RESOURCE_ID = "50f7a383-653a-4860-bb4e-306f221a2d2a"
DATA_URL = f"https://admin.opendata.dk/api/3/action/datastore_search?resource_id={RESOURCE_ID}&limit=10000"
RAW_DATA_DIR = "data/raw"
CLEAN_DATA_DIR = "data/cleaned"
PROJECT_ID = "civic-bike-data-challenge"
DATASET_ID = "bike_data"
TABLE_ID = "traffic_counts"

# Ensure local folders exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

def load_credentials():
    try:
        encoded_key = os.environ.get("GCP_CREDENTIALS_B64")
        if encoded_key:
            credentials_info = json.loads(base64.b64decode(encoded_key).decode("utf-8"))
            return service_account.Credentials.from_service_account_info(credentials_info)
        else:
            return service_account.Credentials.from_service_account_file("gcp_credentials.json")
    except Exception as e:
        raise RuntimeError(f"Failed to load credentials: {e}")

def fetch_raw_data():
    st.info("Fetching raw data...")
    response = requests.get(DATA_URL)
    response.raise_for_status()
    records = response.json()["result"]["records"]
    df = pd.DataFrame(records)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = f"{RAW_DATA_DIR}/bike_raw_{timestamp}.csv"
    df.to_csv(raw_path, index=False)
    st.info(f"Saved raw data to {raw_path}")
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    st.info("Cleaning data...")
    st.info(f"Dataframe columns: {df.columns.tolist()}")
    required_columns = ["_id", "antalcykler", "tidsstempel"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        st.warning(f"ETL failed: missing expected columns: {missing}")
    st.info(f"Dataframe columns: {df.columns.tolist()}")
    df = df.rename(columns={
        "antalcykler": "bike_count",
        "tidsstempel": "timestamp",
        "vejnavn": "street_name",
        "retning": "direction"
    })
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["bike_count"] = pd.to_numeric(df["bike_count"], errors="coerce")
    df = df.dropna(subset=["timestamp", "bike_count"])
    df = df[["timestamp", "street_name", "direction", "bike_count"]]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    clean_path = f"{CLEAN_DATA_DIR}/bike_cleaned_{timestamp}.csv"
    df.to_csv(clean_path, index=False)
    st.info("Saved cleaned data to {clean_path}")
    return df

def upload_to_bigquery(df: pd.DataFrame, credentials, max_retries=4):
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("street_name", "STRING"),
            bigquery.SchemaField("direction", "STRING"),
            bigquery.SchemaField("bike_count", "INTEGER"),
        ]
    )

    for attempt in range(1, max_retries + 1):
        try:
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config, location="EU")
            job.result()
            st.info(f"✅ Upload successful on attempt {attempt}: {len(df)} rows to {table_ref}")
            return
        except Exception as e:
            st.info(f"❌ Upload attempt {attempt} failed: {e}")
            time.sleep(2)
    raise RuntimeError("Upload failed after max retries")

def main():
    try:
        credentials = load_credentials()
        df_raw = fetch_raw_data()
        df_clean = clean_data(df_raw)
        upload_to_bigquery(df_clean, credentials)
    except Exception as e:
        st.info(f"❌ Pipeline failed: {e}")

if __name__ == "__main__":
    main()