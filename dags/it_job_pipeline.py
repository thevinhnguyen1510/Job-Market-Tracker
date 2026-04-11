from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# --- 1. SETUP directory & environment ---
PROJECT_DIR = r"D:\de-job-market-tracker" 
DBT_DIR = f"{PROJECT_DIR}\\analytics_dbt"
PYTHON_CMD = f"{PROJECT_DIR}\\venv\\Scripts\\python.exe"

# --- 2. CONFIG AIRFLOW ---
default_args = {
    'owner': 'DataEngineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'it_job_market_etl_pipeline',
    default_args=default_args,
    description='End-to-end Job Market Pipeline',
    schedule_interval='0 2 * * *', # Chạy lúc 2h sáng mỗi ngày
    catchup=False
) as dag:

    # --- Phase 1 & 2: Crawl & Enrich (Parallel) ---
    crawl_itviec = BashOperator(
        task_id='crawl_itviec', 
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_CMD} crawl_data_from_ITVIEC.py'
    )
    enrich_itviec = BashOperator(
        task_id='enrich_itviec', 
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_CMD} enrich_data_from_ITVIEC.py'
    )
    
    crawl_topcv = BashOperator(
        task_id='crawl_topcv', 
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_CMD} crawl_data_from_TOPCV.py'
    )
    enrich_topcv = BashOperator(
        task_id='enrich_topcv', 
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_CMD} enrich_data_from_TOPCV.py'
    )

    wait_for_raw_data = EmptyOperator(task_id='wait_for_raw_data')

    # --- Phase 3: AI Extractor (Parallel) & Cleanup ---
    ai_extract_itviec = BashOperator(
        task_id='ai_extract_itviec', 
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_CMD} ai_extractor.py itviec'
    )
    ai_extract_topcv = BashOperator(
        task_id='ai_extract_topcv', 
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_CMD} ai_extractor.py topcv'
    )
    
    cleanup_expired = BashOperator(
        task_id='cleanup_expired_jobs', 
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_CMD} cleanup_jobs.py'
    )

    # --- Phase 4: Analytics (DBT) & Vector Sync ---
    update_metrics = BashOperator(
        task_id='dbt_run_models', 
        bash_command=f"cd {DBT_DIR} && {PROJECT_DIR}\\venv\\Scripts\\activate && dbt run"
    )
    
    sync_qdrant = BashOperator(
        task_id='sync_qdrant_vector_db', 
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_CMD} sync_qdrant.py'
    )

    crawl_itviec >> enrich_itviec >> wait_for_raw_data
    crawl_topcv >> enrich_topcv >> wait_for_raw_data

    wait_for_raw_data >> ai_extract_itviec >> cleanup_expired
    wait_for_raw_data >> ai_extract_topcv >> cleanup_expired

    cleanup_expired >> update_metrics
    cleanup_expired >> sync_qdrant