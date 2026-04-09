<<<<<<< HEAD
# 🇬🇭 Ghana Mobile Money ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.14-blue?style=flat-square&logo=python)
![Pandas](https://img.shields.io/badge/Pandas-3.0.2-150458?style=flat-square&logo=pandas)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

A production-grade **Extract → Transform → Load (ETL)** pipeline that processes Ghana Mobile Money (MoMo) transaction data, cleans it, flags anomalies, and exports results to PostgreSQL or CSV.

Built to mirror real-world data engineering workflows used at fintech companies like **Hubtel** and telecoms like **MTN Ghana**.

---

## 🏗️ Pipeline Architecture
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
│   LOAD    │  ← PostgreSQL warehouse or CSV fallback
└───────────┘
│
▼
┌───────────┐
│  REPORT   │  ← Business summary by type, region, operator
└───────────┘

---

## ✅ What The Pipeline Does

### Extract
- Reads raw MoMo transaction CSV files
- Auto-generates **10,000 synthetic Ghana MoMo transactions** if no file is provided
- Injects 300 dirty records to demonstrate real data cleaning

### Transform
- Removes duplicate and null transaction IDs
- Parses and validates timestamps — drops invalid rows
- Cleans and casts transaction amounts
- Standardises text fields (operator, region, status)
- Enriches data with time dimensions (hour, day, month, quarter, weekend flag)
- Categorises amounts into business buckets (Micro / Small / Medium / Large / High Value)
- Flags high-value transactions (top 5% by amount)
- Flags rapid succession transactions (same sender within 2 minutes)

### Load
- Batch upserts into PostgreSQL (`momo_dw.fact_transactions`)
- Auto-falls back to CSV export if database is unavailable
- Logs every pipeline run to audit table

---

## 📊 Sample Pipeline Output
============================================================
GHANA MOMO ETL PIPELINE — RUN SUMMARY
Raw Records Extracted    : 10,000
Clean Records Loaded     : 9,700
Records Dropped          : 300
Transaction Success Rate : 88.02%
High Value Flagged       : 485
Rapid Succession Flagged : 0
TRANSACTIONS BY TYPE:
Count   Total_GHS
Peer to Peer Transfer  3825  1,332,787.53
Merchant Bill Payment  1934    619,115.17
Deposit                1488    547,121.35
Withdrawal             1491    539,418.01
Airtime Top-Up          498    178,323.52
International Transfer  464    174,667.59
TOP REGIONS BY VOLUME (GHS):
Greater Accra    1,157,442.11
Ashanti            840,811.16
Western            500,866.06
Eastern            447,780.76
Brong-Ahafo        275,240.32

---

## 🚀 How To Run

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/ghana-momo-etl-pipeline.git
cd ghana-momo-etl-pipeline
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment
```bash
copy .env.example .env
# Edit .env with your PostgreSQL credentials (optional)
```

### 5. Run the pipeline
```bash
python etl_pipeline.py
```
> No database? No problem — results auto-save to `data/processed/` as CSV.

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.14 | Core pipeline language |
| Pandas | Data extraction and transformation |
| NumPy | Numerical operations and synthetic data |
| psycopg2 | PostgreSQL database connector |
| python-dotenv | Environment variable management |

---

## 🔮 Future Improvements
- [ ] Apache Airflow DAG for scheduled pipeline runs
- [ ] Real-time streaming with Apache Kafka
- [ ] Power BI dashboard connected to PostgreSQL
- [ ] dbt models for analytical layer
- [ ] Unit tests with pytest

---

## 👨‍💻 Author

**Lawrence**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/YOUR_LINKEDIN) | [GitHub](https://github.com/YOUR_USERNAME)
=======
# Ghana-Momo-ETL-Pipeline
>>>>>>> 3ea5197148ee71dd7a4504538067fc91fa6484ea
