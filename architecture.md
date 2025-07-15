# 🏗️ Architecture Overview

This project simulates a real-world cloud-based data platform designed for public sector insights — specifically, Copenhagen’s bike traffic data. The stack is tailored to align with Senior Data Engineer job expectations.

---

## ⚙️ Stack Overview

| Component           | Tool                                        |
|---------------------|---------------------------------------------|
| Ingestion           | Python (`requests`, `pandas`)              |
| Processing          | PySpark                                     |
| Cloud Platform      | Google Cloud (Free Tier)                    |
| Storage             | Google Cloud Storage (raw) + BigQuery       |
| Visualization       | Streamlit (or Google Looker Studio optional)|
| Orchestration       | Manual run / simulated via cron             |
| Testing             | Python `unittest`                           |

---

## 🔁 Data Flow

1. **Fetch Data**
   - Python ingests from [Opendata.dk](https://admin.opendata.dk) using public API

2. **Store Raw Files**
   - Raw data stored in GCS (`/raw/bike/YYYY/MM/DD/`)

3. **Transform**
   - Cleaned and normalized with PySpark
   - Saved locally and loaded into BigQuery (`bike_data.traffic_counts`)

4. **Query & Analyze**
   - SQL run on BigQuery
   - Visualized via Streamlit app

5. **Automate (Optional)**
   - Manual run via CLI for demo
   - Simulates Airflow via structured ETL script

---

## 🗄️ GCP Setup

| Resource Type      | Name                          |
|--------------------|-------------------------------|
| Project ID         | `civic-bike-insights`         |
| GCS Bucket         | `civic-bike-data-raw`         |
| BigQuery Dataset   | `bike_data`                   |
| BigQuery Table     | `traffic_counts`              |

- IAM: Use a single service account with **Storage Admin** and **BigQuery Data Editor** roles
- All cloud components can run under free-tier quotas

---

## 🚀 Why This Stack?

- **BigQuery** replaces MS Fabric-style data warehousing
- **PySpark** offers scalable data cleaning — ready for Databricks/Hadoop
- **GCP** selected for easy onboarding + generous free tier
- **Streamlit** simulates a client-facing analytics dashboard

---

> All tools were chosen to demonstrate full-stack data engineering in a cloud-native, cost-effective, and testable environment.
> 
> Created and revised with AI assistance for clarity and structure
