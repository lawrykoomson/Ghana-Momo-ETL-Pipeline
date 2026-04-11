# 🇬🇭 Ghana Mobile Money ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791?style=flat-square&logo=postgresql)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?style=flat-square&logo=dbt)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat-square&logo=powerbi)
![Tests](https://img.shields.io/badge/Tests-21%2F21%20Passing-brightgreen?style=flat-square)
![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=flat-square)

A production-grade **Extract → Transform → Load (ETL)** pipeline that processes Ghana Mobile Money (MoMo) transaction data, cleans it, flags anomalies, and loads results into a live **PostgreSQL** data warehouse — with a full **dbt analytical layer**, **Power BI dashboard**, **Airflow DAG**, and **Kafka stream simulator**.

Built to mirror real-world data engineering workflows used at fintech companies like **Hubtel** and telecoms like **MTN Ghana**.

---

## 🏗️ System Architecture

```
[Raw CSV / Synthetic Data Source]
           │
           ▼
     ┌───────────┐
     │  EXTRACT  │  ← Reads CSV or generates 10,000 synthetic MoMo transactions
     └───────────┘
           │
           ▼
     ┌───────────┐
     │ TRANSFORM │  ← Cleans, validates, enriches, flags anomalies
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   LOAD    │  ← PostgreSQL warehouse (momo_dw schema)
     └───────────┘
           │
           ▼
     ┌───────────┐
     │    dbt    │  ← Analytical layer: 1 staging view + 4 mart tables
     └───────────┘
           │
           ▼
     ┌───────────┐
     │  Power BI │  ← 4-page live dashboard connected to PostgreSQL
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   Kafka   │  ← Real-time stream simulator: Producer + 3 Consumers
     └───────────┘
```

---

## ✅ What The Pipeline Does

### Extract
- Reads raw MoMo transaction CSV files
- Auto-generates 10,000 synthetic Ghana MoMo transactions if no file provided
- Injects dirty records to demonstrate real data cleaning capability

### Transform
- Removes duplicate and null transaction IDs
- Parses and validates timestamps
- Cleans and casts transaction amounts
- Standardises text fields (operator, region, status)
- Enriches with time dimensions (hour, day, month, quarter, weekend flag)
- Categorises amounts into business buckets
- Flags high-value transactions (top 5%) and rapid succession activity

### Load
- Batch upserts into PostgreSQL (momo_dw schema)
- Creates schema and tables automatically on first run
- Auto-falls back to CSV export if database is unavailable
- Logs every pipeline run to audit table

---

## 🔁 dbt Analytical Layer

5 models built on top of the PostgreSQL warehouse:

| Model | Type | Description |
|---|---|---|
| stg_momo_transactions | View | Cleaned and standardised transactions |
| mart_daily_revenue | Table | Revenue aggregated by day |
| mart_operator_performance | Table | MTN vs Vodafone vs AirtelTigo |
| mart_region_summary | Table | Revenue and customers by Ghana region |
| mart_high_value_transactions | Table | 485 flagged high-risk transactions |

```bash
cd dbt
dbt run --profiles-dir .     # Run all 5 models
dbt test --profiles-dir .    # Run 5 data quality tests
dbt docs generate            # Generate documentation
```

---

## 📊 Power BI Dashboard — 4 Pages

Connected live to PostgreSQL via dbt mart tables:

| Page | Key Metrics |
|---|---|
| Executive Summary | GHS 2.97M revenue, 9,700 transactions, 88.02% success rate |
| Operator Performance | MTN leads at 53.99% market share |
| Regional Analysis | Greater Accra dominates at 34.13% revenue share |
| Fraud and Compliance | 485 high-value transactions, GHS 1.82M flagged |

---

## 🌊 Kafka Stream Simulator

Real-time MoMo transaction streaming with Kafka architecture:

```bash
python kafka_simulator.py
```

```
Topic          : momo.transactions.live
Partitions     : 3
Producer Rate  : 5 transactions/sec
Duration       : 60 seconds

Producer       → generates live MoMo transaction events
FraudDetector  → detects high-value and rapid succession (partition 0)
MetricsAgg     → aggregates real-time KPIs (partition 1)
TxnLogger      → logs all events to JSONL file (partition 2)

Final Results:
  Total Produced         : 298 transactions
  Total Volume Streamed  : GHS 46,623.72
  Fraud Alerts Generated : 8
  Final Success Rate     : 62.6%
  Top Operator           : MTN
  Top Region             : Greater Accra
```

---

## 🧪 Unit Tests — 21/21 Passing

```bash
pytest test_pipeline.py -v
# 21 passed in 1.89s
```

Tests cover Extract, Transform, and Integration stages.

---

## 📋 Airflow DAG

Scheduled pipeline at `dags/momo_etl_dag.py`:
- Runs daily at 06:00 AM UTC
- 5 tasks: extract, transform, load, report, notify
- XCom communication between tasks
- Email alerts on failure with 2 retries

---

## 📊 Sample Pipeline Output

```
============================================================
   GHANA MOMO ETL PIPELINE — RUN SUMMARY
============================================================
  Raw Records Extracted    : 10,000
  Clean Records Loaded     : 9,700
  Records Dropped          : 300
  Transaction Success Rate : 88.02%
  High Value Flagged       : 485
  Rapid Succession Flagged : 0
------------------------------------------------------------
  TRANSACTIONS BY TYPE:
                        Count     Total GHS
  Peer to Peer Transfer  3825  1,332,787.53
  Merchant Bill Payment  1934    619,115.17
  Deposit                1488    547,121.35
  Withdrawal             1491    539,418.01
  Airtime Top-Up          498    178,323.52
  International Transfer  464    174,667.59
------------------------------------------------------------
  TOP REGIONS BY VOLUME (GHS):
  Greater Accra    1,157,442.11
  Ashanti            840,811.16
  Western            500,866.06
  Eastern            447,780.76
  Brong-Ahafo        275,240.32
============================================================
```

---

## 🚀 How To Run

```bash
# 1. Clone the repo
git clone https://github.com/lawrykoomson/Ghana-Momo-ETL-Pipeline.git
cd Ghana-Momo-ETL-Pipeline

# 2. Create virtual environment with Python 3.11
py -3.11 -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create PostgreSQL database
psql -U postgres -c "CREATE DATABASE momo_warehouse;"

# 5. Configure environment
copy .env.example .env
# Edit .env with your PostgreSQL credentials

# 6. Run the ETL pipeline
python etl_pipeline.py

# 7. Run unit tests
pytest test_pipeline.py -v

# 8. Run dbt models
cd dbt
dbt run --profiles-dir .
dbt test --profiles-dir .

# 9. Run Kafka stream simulator
cd ..
python kafka_simulator.py
```

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core pipeline language |
| Pandas | Data extraction and transformation |
| NumPy | Numerical operations |
| psycopg2 | PostgreSQL database connector |
| dbt-postgres | Analytical transformation layer |
| Apache Airflow | Pipeline orchestration DAG |
| Power BI | Business intelligence dashboard |
| pytest | Unit testing framework |
| python-dotenv | Environment variable management |

---

## 🔮 Roadmap

- [x] ETL pipeline with PostgreSQL live load
- [x] 21 unit tests — all passing
- [x] dbt analytical layer — 5 models, 5 tests passing
- [x] Apache Airflow DAG for scheduled runs
- [x] Power BI dashboard — 4 pages live
- [x] Kafka stream simulator — 3 consumer groups
- [ ] Docker containerisation

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)