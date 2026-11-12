"""Daily orchestration. Task IDs are stable across application refactors."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PROJECT_DIR = "/opt/airflow"
DBT_DIR = f"{PROJECT_DIR}/analytics_dbt"


def pipeline_task(task_id: str, command: str) -> BashOperator:
    """Run a pipeline CLI command from the project root."""
    return BashOperator(
        task_id=task_id,
        cwd=PROJECT_DIR,
        bash_command=f"python -u -m job_market {command}",
    )


def dbt_task(task_id: str, command: str) -> BashOperator:
    """Run a dbt command from the warehouse project directory."""
    return BashOperator(task_id=task_id, cwd=DBT_DIR, bash_command=f"dbt {command}")


with DAG(
    "it_job_market_etl_pipeline",
    default_args={
        "owner": "DataEngineer",
        "depends_on_past": False,
        "start_date": pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Collect, transform and publish IT job listings",
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    itviec_listings = pipeline_task("crawl_itviec", "crawl itviec")
    itviec_details = pipeline_task("enrich_itviec", "enrich itviec")
    topcv_listings = pipeline_task("crawl_topcv", "crawl topcv")
    topcv_details = pipeline_task("enrich_topcv", "enrich topcv")
    sources_ready = EmptyOperator(task_id="wait_for_raw_data")
    ingest = pipeline_task("ingest_landing_to_bronze", "ingest")
    extract_itviec = pipeline_task("ai_extract_itviec", "extract itviec")
    extract_topcv = pipeline_task("ai_extract_topcv", "extract topcv")
    seed = dbt_task("dbt_seed_taxonomy", "seed")
    silver = dbt_task("dbt_build_silver", "run --select silver")
    gold = dbt_task("dbt_build_gold", "run --select gold")
    test_data = dbt_task("dbt_test_data_quality", "test")
    publish_search = pipeline_task("sync_qdrant_vector_db", "sync-search")

    itviec_listings >> itviec_details
    topcv_listings >> topcv_details
    [itviec_details, topcv_details] >> sources_ready >> ingest
    # DuckDB writes must not overlap across task processes.
    ingest >> extract_itviec >> extract_topcv >> seed >> silver >> gold >> test_data >> publish_search
