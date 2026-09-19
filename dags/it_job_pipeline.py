import pendulum
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.empty import EmptyOperator # type: ignore
from datetime import timedelta

# ==========================================
# 1. SETUP TIMEZONE & DIRECTORY
# ==========================================
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

SCRIPTS_DIR = "/opt/airflow/scripts"
DBT_DIR = "/opt/airflow/analytics_dbt"
PYTHON_CMD = "python -u" 

# ==========================================
# 2. CONFIG AIRFLOW
# ==========================================
default_args = {
    'owner': 'DataEngineer',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'it_job_market_etl_pipeline',
    default_args=default_args,
    description='End-to-end Job Market Pipeline',
    schedule='0 7 * * *', 
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


    ingest_landing = BashOperator(
        task_id='ingest_landing_to_bronze',
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} ingest_landing_to_bronze.py'
    )


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

    dbt_test = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command=f"cd {DBT_DIR} && dbt test"
    )
    
    sync_qdrant = BashOperator(
        task_id='sync_qdrant_vector_db', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} sync_qdrant.py'
    )

    # ==========================================
    # WORKFLOW / DEPENDENCIES DEFINITION
    # ==========================================
    
    # 1. RAW CRAWL & ENRICH BRANCHES
    crawl_itviec >> enrich_itviec
    crawl_topcv >> enrich_topcv

    # 2. BRONZE INGESTION & STAGING
    [enrich_itviec, enrich_topcv] >> wait_for_raw_data >> ingest_landing >> dbt_build_int

    # 3. SILVER LAYER: Sequential extraction to prevent DuckDB write contention
    dbt_build_int >> ai_extract_itviec >> ai_extract_topcv >> cleanup_expired

    # 4. GOLD LAYER & VECTOR SYNC: Finalize market marts and update vector embeddings
    cleanup_expired >> update_metrics >> dbt_test >> sync_qdrant