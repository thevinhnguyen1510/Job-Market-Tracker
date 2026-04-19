from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# ==========================================
# 1. SETUP DIRECTORY & ENVIRONMENT
# ==========================================
SCRIPTS_DIR = "/opt/airflow/scripts"
DBT_DIR = "/opt/airflow/analytics_dbt"
PYTHON_CMD = "python" 

# ==========================================
# 2. CONFIG AIRFLOW
# ==========================================
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
    schedule=None,
    catchup=False
) as dag:

    # ==========================================
    # PHASE 1 & 2: CRAWL & ENRICH (RAW LAYER)
    # ==========================================
    crawl_itviec = BashOperator(
        task_id='crawl_itviec', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} crawl_data_from_ITVIEC.py'
    )
    enrich_itviec = BashOperator(
        task_id='enrich_itviec', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} enrich_job_details_ITVIEC.py'
    )
    
    crawl_topcv = BashOperator(
        task_id='crawl_topcv', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} crawl_data_from_TOPCV.py'
    )
    enrich_topcv = BashOperator(
        task_id='enrich_topcv', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} enrich_job_details_TOPCV.py'
    )

    # Dummy operator acting as a checkpoint for the Raw Layer
    wait_for_raw_data = EmptyOperator(task_id='wait_for_raw_data')

    # ==========================================
    # PHASE 2.5: DBT STAGING & INTERMEDIATE 
    # ==========================================
    dbt_build_int = BashOperator(
        task_id='dbt_build_int',
        bash_command=f'cd {DBT_DIR} && dbt run --select staging intermediate'
    )

    # ==========================================
    # PHASE 3: AI EXTRACTOR (SILVER LAYER) & CLEANUP
    # ==========================================
    ai_extract_itviec = BashOperator(
        task_id='ai_extract_itviec', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} ai_extractor.py itviec'
    )
    ai_extract_topcv = BashOperator(
        task_id='ai_extract_topcv', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} ai_extractor.py topcv'
    )
    
    cleanup_expired = BashOperator(
        task_id='cleanup_expired_jobs', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} cleanup_jobs.py'
    )

    # ==========================================
    # PHASE 4: ANALYTICS (GOLD LAYER) & VECTOR SYNC
    # ==========================================
    # Only run the 'gold' models here to save execution time
    update_metrics = BashOperator(
        task_id='dbt_run_models_gold', 
        bash_command=f"cd {DBT_DIR} && dbt run --select gold"
    )
    
    sync_qdrant = BashOperator(
        task_id='sync_qdrant_vector_db', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} sync_qdrant.py'
    )

    # ==========================================
    # WORKFLOW / DEPENDENCIES DEFINITION
    # ==========================================
    
    # 1. RAW LAYER: Strictly sequential to prevent DuckDB concurrency write locks
    crawl_itviec >> enrich_itviec >> crawl_topcv >> enrich_topcv >> wait_for_raw_data

    # 2. DBT INTEGRATION: Runs only after all raw data is safely landed
    wait_for_raw_data >> dbt_build_int

    # 3. SILVER LAYER: Sequential extraction to avoid database locking
    dbt_build_int >> ai_extract_itviec >> ai_extract_topcv >> cleanup_expired

    # 4. DOWNSTREAM LAYER: Finalize metrics and update vector search engine
    cleanup_expired >> update_metrics >> sync_qdrant