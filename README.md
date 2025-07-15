

# civic-bike-data-challenge
Simulated delivery of a cloud data platform for a public sector client (e.g., Copenhagen Municipality), aligned with Senior Data Engineer job expectations.

## Stack

 > Full architecture: [architecture.md](architecture.md)

---

## 🧪 How to Run

### 1. Setup

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Run ETL pipeline

```bash
python data_pipeline.py
```

### 3. Launch dashboard

```bash
streamlit run dashboard_app.py
```

---

## 🧭 Requirement Checklist

|  Requirement                                       | Covered in This Project                   |
|----------------------------------------------------|-------------------------------------------|
| Data platform (MS Fabric / SAS)                    | Simulated via BigQuery                    |
| Cloud platform (Azure / AWS / GCP)                 | GCP used throughout                       |
| Databricks / Hadoop / Spark / Kafka                | PySpark used for data processing          |
| SQL, Python, R                                     | SQL (BigQuery) + Python implemented       |
| Agile / Scrum methodology                          | [`mock_user_stories.md`](mock_user_stories.md) included           |
| Consultancy project mindset                        | Framed as public sector client delivery   |
| Experience in financial/public sector              | Uses civic (public) bike traffic data     |

---

## 📚 Files & Folders

| File/Folder              | Purpose                                        |
|--------------------------|------------------------------------------------|
| [`data_pipeline.py`](data_pipeline.py)     | Ingests and processes raw civic data           |
| [`dashboard_app.py`](dashboard_app.py)     | Interactive visual dashboard (Streamlit)       |
| [`architecture.md`](architecture.md)   | System architecture with tools and rationale   |
| [`mock_user_stories.md`](mock_user_stories.md)  | Simulated Agile stories for delivery planning  |
| [`test_data_validation.py`](test_data_validation.py) | Basic unit tests for data integrity            |
| [`requirements.txt`](requirements.txt)       | Dependencies for pipeline and dashboard        |

---

## ✍️ Notes

- **Data Source**: [Opendata.dk – Bike Traffic](https://admin.opendata.dk/api/3/action/datastore_search?resource_id=50f7a383-653a-4860-bb4e-306f221a2d2a)
- Queries can be modified using either API calls or SQL endpoints
- The project simulates a real-world consulting delivery in a hybrid cloud/data engineering role
  
---

> Created and revised with AI assistance for clarity and structure.
