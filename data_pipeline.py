import base64, json
import requests
import pandas as pd
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

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

def fetch_raw_data():
    print("Fetching raw data...")
    response = requests.get(DATA_URL)
    response.raise_for_status()
    records = response.json()["result"]["records"]
    df = pd.DataFrame(records)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = f"{RAW_DATA_DIR}/bike_raw_{timestamp}.csv"
    df.to_csv(raw_path, index=False)
    print(f"Saved raw data to {raw_path}")
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Cleaning data...")
    # Drop rows with missing key fields
    df = df.dropna(subset=["_id", "antalcykler", "tidsstempel"])
    # Rename for clarity
    df = df.rename(columns={
        "antalcykler": "bike_count",
        "tidsstempel": "timestamp",
        "vejnavn": "street_name",
        "retning": "direction"
    })
    # Convert types
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["bike_count"] = pd.to_numeric(df["bike_count"], errors="coerce")
    df = df.dropna(subset=["timestamp", "bike_count"])
    df = df[["timestamp", "street_name", "direction", "bike_count"]]
    print(f"Cleaned data: {len(df)} records")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    clean_path = f"{CLEAN_DATA_DIR}/bike_cleaned_{timestamp}.csv"
    df.to_csv(clean_path, index=False)
    print(f"Saved cleaned data to {clean_path}")
    return df

def upload_to_bigquery(df: pd.DataFrame):
    print("Uploading to BigQuery...")
    try:
        	encoded_key = st.secrets["GCP_CREDENTIALS_B64"]
            st.text(f"Key length: {len(encoded_key)}")
            decoded_json = base64.b64decode(encoded_key).decode("utf-8")
            credentials_info = json.loads(decoded_json)

            credentials = service_account.Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    except Exception as e:
        print("❌ Skipping BigQuery upload. Reason:", e)
        return

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,  
        source_format=bigquery.SourceFormat.PARQUET,  
        schema=[
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("street_name", "STRING"),
            bigquery.SchemaField("direction", "STRING"),
            bigquery.SchemaField("bike_count", "INTEGER"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config, location=EU)
    job.result()  # Wait for completion
    print(f"✅ Uploaded {len(df)} records to BigQuery table `{table_ref}`.")

def main():
    try:
        raw_df = fetch_raw_data()
        cleaned_df = clean_data(raw_df)
        upload_to_bigquery(cleaned_df)
        print("✅ Pipeline completed successfully.")
    except Exception as e:
        print("❌ Pipeline failed:", e)

if __name__ == "__main__":
    main()
