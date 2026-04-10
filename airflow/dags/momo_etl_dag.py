"""
Apache Airflow DAG — Ghana MoMo ETL Pipeline
=============================================
Schedules the MoMo ETL pipeline to run automatically
every day at 6:00 AM Ghana time (UTC+0).

Pipeline Tasks:
    Task 1 → extract_data      : Generate/read raw MoMo transactions
    Task 2 → transform_data    : Clean, validate, enrich, flag anomalies
    Task 3 → load_to_postgres  : Load clean data into PostgreSQL warehouse
    Task 4 → generate_report   : Print business summary and log metrics
    Task 5 → notify_completion : Log pipeline completion status

Schedule: Daily at 06:00 AM (UTC)
Owner: Lawrence Koomson
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  DAG DEFAULT ARGUMENTS
# ─────────────────────────────────────────────
default_args = {
    "owner":            "lawrence_koomson",
    "depends_on_past":  False,
    "email":            ["koomsonlawrence64@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ─────────────────────────────────────────────
#  DAG DEFINITION
# ─────────────────────────────────────────────
dag = DAG(
    dag_id="ghana_momo_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for Ghana Mobile Money transaction data",
    schedule_interval="0 6 * * *",   # Every day at 06:00 AM UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["momo", "etl", "ghana", "fintech", "data-engineering"],
)

# ─────────────────────────────────────────────
#  TASK FUNCTIONS
# ─────────────────────────────────────────────
def task_extract(**context):
    """
    Extract raw MoMo transaction data.
    Pushes extracted DataFrame to XCom for next task.
    """
    from etl_pipeline import extract, RAW_DATA_PATH
    logger.info("AIRFLOW TASK: extract_data — Starting extraction...")
    df = extract(RAW_DATA_PATH)
    logger.info(f"AIRFLOW TASK: extract_data — Extracted {len(df):,} records")

    # Push record count to XCom for monitoring
    context["ti"].xcom_push(key="raw_count", value=len(df))

    # Save extracted data to temp CSV for next task
    temp_path = "/tmp/momo_raw_extract.csv"
    df.to_csv(temp_path, index=False)
    context["ti"].xcom_push(key="temp_raw_path", value=temp_path)

    logger.info(f"AIRFLOW TASK: extract_data — Saved to {temp_path}")
    return len(df)


def task_transform(**context):
    """
    Transform extracted data.
    Reads from XCom temp path, applies all transformations.
    """
    import pandas as pd
    from etl_pipeline import transform

    logger.info("AIRFLOW TASK: transform_data — Starting transformation...")

    # Pull temp file path from previous task
    temp_path = context["ti"].xcom_pull(
        task_ids="extract_data", key="temp_raw_path"
    )
    df = pd.read_csv(temp_path, dtype=str)

    clean_df, report = transform(df)

    logger.info(f"AIRFLOW TASK: transform_data — Clean records: {len(clean_df):,}")
    logger.info(f"AIRFLOW TASK: transform_data — Success rate: {report['success_rate_pct']}%")

    # Save clean data for load task
    clean_path = "/tmp/momo_clean_transform.csv"
    clean_df.to_csv(clean_path, index=False)

    # Push metrics to XCom
    context["ti"].xcom_push(key="clean_count",        value=int(len(clean_df)))
    context["ti"].xcom_push(key="clean_path",         value=clean_path)
    context["ti"].xcom_push(key="success_rate",       value=report["success_rate_pct"])
    context["ti"].xcom_push(key="high_value_flagged", value=report["high_value_flagged"])

    return int(len(clean_df))


def task_load(**context):
    """
    Load transformed data into PostgreSQL warehouse.
    """
    import pandas as pd
    from etl_pipeline import load, transform

    logger.info("AIRFLOW TASK: load_to_postgres — Starting load...")

    clean_path = context["ti"].xcom_pull(
        task_ids="transform_data", key="clean_path"
    )
    df = pd.read_csv(clean_path)

    # Re-run transform to get report (since we can't pass full df via XCom easily)
    raw_df = pd.read_csv(
        context["ti"].xcom_pull(task_ids="extract_data", key="temp_raw_path"),
        dtype=str
    )
    clean_df, report = transform(raw_df)
    load(clean_df, report)

    logger.info(f"AIRFLOW TASK: load_to_postgres — Load complete.")
    return "success"


def task_report(**context):
    """
    Generate business summary report and log key metrics.
    """
    import pandas as pd
    from etl_pipeline import print_summary, transform

    logger.info("AIRFLOW TASK: generate_report — Generating summary...")

    raw_df = pd.read_csv(
        context["ti"].xcom_pull(task_ids="extract_data", key="temp_raw_path"),
        dtype=str
    )
    clean_df, report = transform(raw_df)
    print_summary(clean_df, report)

    # Log key metrics
    raw_count    = context["ti"].xcom_pull(task_ids="extract_data",  key="raw_count")
    clean_count  = context["ti"].xcom_pull(task_ids="transform_data", key="clean_count")
    success_rate = context["ti"].xcom_pull(task_ids="transform_data", key="success_rate")
    hv_flagged   = context["ti"].xcom_pull(task_ids="transform_data", key="high_value_flagged")

    logger.info("=" * 55)
    logger.info("  AIRFLOW PIPELINE RUN — METRICS")
    logger.info("=" * 55)
    logger.info(f"  Raw Records       : {raw_count:,}")
    logger.info(f"  Clean Records     : {clean_count:,}")
    logger.info(f"  Dropped Records   : {raw_count - clean_count:,}")
    logger.info(f"  Success Rate      : {success_rate}%")
    logger.info(f"  High Value Flagged: {hv_flagged:,}")
    logger.info("=" * 55)

    return "report_complete"


def task_notify(**context):
    """
    Log pipeline completion. In production this would
    send a Slack message or email notification.
    """
    run_date  = context["ds"]
    clean_cnt = context["ti"].xcom_pull(task_ids="transform_data", key="clean_count")

    logger.info("=" * 55)
    logger.info("  AIRFLOW PIPELINE — COMPLETION NOTIFICATION")
    logger.info("=" * 55)
    logger.info(f"  Pipeline       : ghana_momo_etl_pipeline")
    logger.info(f"  Run Date       : {run_date}")
    logger.info(f"  Records Loaded : {clean_cnt:,}")
    logger.info(f"  Status         : SUCCESS")
    logger.info(f"  Warehouse      : momo_dw.fact_transactions")
    logger.info("=" * 55)

    # In production — send Slack/email:
    # slack_client.send(f"MoMo ETL complete: {clean_cnt:,} records loaded for {run_date}")
    return "notified"


# ─────────────────────────────────────────────
#  TASK DEFINITIONS
# ─────────────────────────────────────────────
start = EmptyOperator(
    task_id="pipeline_start",
    dag=dag
)

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=task_extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=task_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=task_load,
    dag=dag,
)

report_task = PythonOperator(
    task_id="generate_report",
    python_callable=task_report,
    dag=dag,
)

notify_task = PythonOperator(
    task_id="notify_completion",
    python_callable=task_notify,
    dag=dag,
)

end = EmptyOperator(
    task_id="pipeline_end",
    dag=dag
)

# ─────────────────────────────────────────────
#  TASK DEPENDENCIES (Pipeline Flow)
# ─────────────────────────────────────────────
start >> extract_task >> transform_task >> load_task >> report_task >> notify_task >> end