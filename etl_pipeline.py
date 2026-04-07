"""
Ghana Mobile Money ETL Pipeline
================================
Simulates a production-grade ETL pipeline for Mobile Money transaction data.
Targets: Hubtel, MTN Ghana

Pipeline Flow:
    Extract  -> raw CSV transaction data (or generates synthetic data)
    Transform -> clean, validate, enrich, flag anomalies
    Load     -> PostgreSQL data warehouse (or CSV fallback)

Author: Lawrence
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
#  LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", 5432),
    "database": os.getenv("DB_NAME", "momo_warehouse"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

RAW_DATA_PATH = Path("data/raw/transactions.csv")
PROCESSED_PATH = Path("data/processed/")

TRANSACTION_TYPES = {
    "P2P":    "Peer to Peer Transfer",
    "MBP":    "Merchant Bill Payment",
    "WDR":    "Withdrawal",
    "DEP":    "Deposit",
    "INTL":   "International Transfer",
    "AIRTIME":"Airtime Top-Up",
}

OPERATORS = ["MTN", "Vodafone", "AirtelTigo"]


# ─────────────────────────────────────────────
#  EXTRACT
# ─────────────────────────────────────────────
def extract(filepath: Path) -> pd.DataFrame:
    logger.info(f"[EXTRACT] Reading data from {filepath}")
    try:
        df = pd.read_csv(filepath, dtype=str)
        logger.info(f"[EXTRACT] Loaded {len(df):,} raw records")
        return df
    except FileNotFoundError:
        logger.warning("[EXTRACT] No CSV found. Generating 10,000 synthetic transactions...")
        return generate_synthetic_data(n=10000)


def generate_synthetic_data(n: int = 10000) -> pd.DataFrame:
    np.random.seed(42)

    transaction_ids = [f"TXN{str(i).zfill(8)}" for i in range(1, n + 1)]
    timestamps = pd.date_range("2024-01-01", periods=n, freq="1min")

    amounts = np.abs(
        np.where(
            np.random.rand(n) < 0.6,
            np.random.uniform(1, 50, n),
            np.where(
                np.random.rand(n) < 0.75,
                np.random.uniform(50, 500, n),
                np.random.uniform(500, 5000, n)
            )
        )
    ).round(2)

    df = pd.DataFrame({
        "transaction_id":   transaction_ids,
        "timestamp":        timestamps.astype(str),
        "sender_msisdn":    [f"024{''.join(np.random.choice(list('0123456789'), 7).tolist())}" for _ in range(n)],
        "receiver_msisdn":  [f"055{''.join(np.random.choice(list('0123456789'), 7).tolist())}" for _ in range(n)],
        "amount":           amounts.astype(str),
        "currency":         np.random.choice(["GHS", "USD"], n, p=[0.97, 0.03]),
        "transaction_type": np.random.choice(list(TRANSACTION_TYPES.keys()), n,
                                             p=[0.40, 0.20, 0.15, 0.15, 0.05, 0.05]),
        "operator":         np.random.choice(OPERATORS, n, p=[0.55, 0.25, 0.20]),
        "status":           np.random.choice(["SUCCESS","FAILED","PENDING","REVERSED"], n,
                                             p=[0.88, 0.06, 0.04, 0.02]),
        "region":           np.random.choice(
                                ["Greater Accra","Ashanti","Western","Eastern","Brong-Ahafo","Northern"], n,
                                p=[0.35, 0.25, 0.15, 0.12, 0.08, 0.05]),
        "fee":              (amounts * np.random.uniform(0.005, 0.015, n)).round(2).astype(str),
    })

    # Inject dirty records deliberately (to show the cleaning step)
    dirty = np.random.choice(n, 300, replace=False)
    df.loc[dirty[:100], "amount"]         = "N/A"
    df.loc[dirty[100:200], "transaction_id"] = None
    df.loc[dirty[200:], "timestamp"]      = "invalid_date"

    logger.info(f"[EXTRACT] Generated {n:,} synthetic records (300 dirty records injected for demo)")
    return df


# ─────────────────────────────────────────────
#  TRANSFORM
# ─────────────────────────────────────────────
def transform(df: pd.DataFrame) -> tuple:
    logger.info(f"[TRANSFORM] Starting on {len(df):,} raw records...")
    report = {"raw_count": len(df), "issues": {}}

    # 1. Drop duplicate / null transaction IDs
    before = len(df)
    df = df.dropna(subset=["transaction_id"]).drop_duplicates(subset=["transaction_id"])
    report["issues"]["duplicate_or_null_ids"] = before - len(df)
    logger.info(f"[TRANSFORM] Removed {before - len(df)} duplicate/null IDs")

    # 2. Parse timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    bad_ts = df["timestamp"].isna().sum()
    report["issues"]["invalid_timestamps"] = int(bad_ts)
    df = df.dropna(subset=["timestamp"])
    logger.info(f"[TRANSFORM] Dropped {bad_ts} rows with invalid timestamps")

    # 3. Clean amounts
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["fee"]    = pd.to_numeric(df["fee"],    errors="coerce").fillna(0)
    bad_amt = df["amount"].isna().sum()
    report["issues"]["invalid_amounts"] = int(bad_amt)
    df = df.dropna(subset=["amount"])
    df = df[df["amount"] > 0]
    logger.info(f"[TRANSFORM] Removed {bad_amt} rows with invalid amounts")

    # 4. Standardise text columns
    for col in ["transaction_type", "status", "operator", "currency", "region"]:
        df[col] = df[col].str.strip().str.upper()

    # 5. Enrich — readable labels
    df["transaction_type_label"] = df["transaction_type"].map(TRANSACTION_TYPES).fillna("Unknown")

    # 6. Enrich — time dimensions
    df["txn_date"]        = df["timestamp"].dt.date
    df["txn_hour"]        = df["timestamp"].dt.hour
    df["txn_day_of_week"] = df["timestamp"].dt.day_name()
    df["txn_month"]       = df["timestamp"].dt.month
    df["txn_quarter"]     = df["timestamp"].dt.quarter
    df["is_weekend"]      = df["timestamp"].dt.dayofweek >= 5

    # 7. Enrich — amount buckets
    df["amount_bucket"] = pd.cut(
        df["amount"],
        bins=[0, 50, 200, 500, 1000, float("inf")],
        labels=["Micro (<50)", "Small (50-200)", "Medium (200-500)", "Large (500-1k)", "High Value (1k+)"]
    ).astype(str)

    # 8. Flag — high value transactions (top 5%)
    threshold = df["amount"].quantile(0.95)
    df["is_high_value"] = df["amount"] >= threshold

    # 9. Flag — rapid succession (same sender within 2 minutes)
    df = df.sort_values(["sender_msisdn", "timestamp"])
    df["prev_ts"] = df.groupby("sender_msisdn")["timestamp"].shift(1)
    df["time_since_last_txn_sec"] = (df["timestamp"] - df["prev_ts"]).dt.total_seconds()
    df["is_rapid_succession"] = df["time_since_last_txn_sec"] < 120
    df = df.drop(columns=["prev_ts"])

    # 10. Total debit
    df["total_debit"] = (df["amount"] + df["fee"]).round(2)

    report["clean_count"]             = len(df)
    report["high_value_flagged"]      = int(df["is_high_value"].sum())
    report["rapid_succession_flagged"]= int(df["is_rapid_succession"].sum())
    report["success_rate_pct"]        = round((df["status"] == "SUCCESS").mean() * 100, 2)

    logger.info(f"[TRANSFORM] Done. Clean records: {len(df):,} | Success rate: {report['success_rate_pct']}%")
    return df, report


# ─────────────────────────────────────────────
#  LOAD
# ─────────────────────────────────────────────
def load(df: pd.DataFrame, report: dict):
    logger.info("[LOAD] Attempting PostgreSQL connection...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS momo_dw;
                CREATE TABLE IF NOT EXISTS momo_dw.fact_transactions (
                    transaction_id          VARCHAR(20) PRIMARY KEY,
                    timestamp               TIMESTAMP,
                    txn_date                DATE,
                    txn_hour                SMALLINT,
                    txn_day_of_week         VARCHAR(12),
                    txn_month               SMALLINT,
                    txn_quarter             SMALLINT,
                    is_weekend              BOOLEAN,
                    sender_msisdn           VARCHAR(15),
                    receiver_msisdn         VARCHAR(15),
                    amount                  NUMERIC(12,2),
                    fee                     NUMERIC(8,2),
                    total_debit             NUMERIC(12,2),
                    currency                VARCHAR(5),
                    transaction_type        VARCHAR(10),
                    transaction_type_label  VARCHAR(50),
                    amount_bucket           VARCHAR(30),
                    operator                VARCHAR(20),
                    status                  VARCHAR(15),
                    region                  VARCHAR(50),
                    is_high_value           BOOLEAN,
                    is_rapid_succession     BOOLEAN,
                    time_since_last_txn_sec NUMERIC(10,2),
                    loaded_at               TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS momo_dw.pipeline_runs (
                    run_id                   SERIAL PRIMARY KEY,
                    run_timestamp            TIMESTAMP DEFAULT NOW(),
                    raw_count                INTEGER,
                    clean_count              INTEGER,
                    high_value_flagged       INTEGER,
                    rapid_succession_flagged INTEGER,
                    success_rate_pct         NUMERIC(5,2),
                    issues                   JSONB
                );
            """)
            conn.commit()

        cols = [
            "transaction_id","timestamp","txn_date","txn_hour","txn_day_of_week",
            "txn_month","txn_quarter","is_weekend","sender_msisdn","receiver_msisdn",
            "amount","fee","total_debit","currency","transaction_type",
            "transaction_type_label","amount_bucket","operator","status","region",
            "is_high_value","is_rapid_succession","time_since_last_txn_sec"
        ]
        records = [tuple(r) for r in df[cols].itertuples(index=False)]

        with conn.cursor() as cur:
            execute_values(cur,
                f"INSERT INTO momo_dw.fact_transactions ({','.join(cols)}) VALUES %s "
                f"ON CONFLICT (transaction_id) DO UPDATE SET status=EXCLUDED.status, loaded_at=NOW()",
                records, page_size=500
            )
            cur.execute("""
                INSERT INTO momo_dw.pipeline_runs
                (raw_count,clean_count,high_value_flagged,rapid_succession_flagged,success_rate_pct,issues)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                report["raw_count"], report["clean_count"],
                report["high_value_flagged"], report["rapid_succession_flagged"],
                report["success_rate_pct"], json.dumps(report["issues"])
            ))
            conn.commit()

        conn.close()
        logger.info(f"[LOAD] Successfully loaded {len(df):,} records into PostgreSQL.")

    except Exception as e:
        logger.warning(f"[LOAD] PostgreSQL unavailable ({e})")
        logger.info("[LOAD] Falling back to CSV export...")
        fallback = PROCESSED_PATH / f"transactions_clean_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(fallback, index=False)
        logger.info(f"[LOAD] Saved to {fallback}")


# ─────────────────────────────────────────────
#  SUMMARY REPORT
# ─────────────────────────────────────────────
def print_summary(df: pd.DataFrame, report: dict):
    print("\n" + "="*60)
    print("   GHANA MOMO ETL PIPELINE — RUN SUMMARY")
    print("="*60)
    print(f"  Raw Records Extracted    : {report['raw_count']:,}")
    print(f"  Clean Records Loaded     : {report['clean_count']:,}")
    print(f"  Records Dropped          : {report['raw_count'] - report['clean_count']:,}")
    print(f"  Transaction Success Rate : {report['success_rate_pct']}%")
    print(f"  High Value Flagged       : {report['high_value_flagged']:,}")
    print(f"  Rapid Succession Flagged : {report['rapid_succession_flagged']:,}")
    print("-"*60)
    print("  TRANSACTIONS BY TYPE:")
    summary = df.groupby("transaction_type_label")["amount"].agg(
        Count="count", Total_GHS="sum"
    ).sort_values("Total_GHS", ascending=False)
    print(summary.to_string())
    print("-"*60)
    print("  TOP REGIONS BY VOLUME (GHS):")
    print(df.groupby("region")["amount"].sum()
            .sort_values(ascending=False).head(5).to_string())
    print("="*60 + "\n")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def run_pipeline():
    logger.info("=" * 55)
    logger.info("  GHANA MOMO ETL PIPELINE — STARTED")
    logger.info("=" * 55)
    start = datetime.now()

    raw_df          = extract(RAW_DATA_PATH)
    clean_df, rpt   = transform(raw_df)
    load(clean_df, rpt)
    print_summary(clean_df, rpt)

    duration = (datetime.now() - start).total_seconds()
    logger.info(f"PIPELINE COMPLETED in {duration:.2f} seconds")


if __name__ == "__main__":
    run_pipeline()