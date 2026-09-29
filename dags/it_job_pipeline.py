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
    description='End-to-end 3-Tier Medallion Job Market Pipeline',
    schedule='0 7 * * *', 
    catchup=False,
    max_active_runs=1
) as dag:

    # ==========================================
    # PHASE 1: BRONZE LAYER (CRAWL & INGEST)
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

    # Checkpoint after scraping is completed
    wait_for_raw_data = EmptyOperator(task_id='wait_for_raw_data')

    ingest_landing = BashOperator(
        task_id='ingest_landing_to_bronze',
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} ingest_landing_to_bronze.py'
    )

    # ==========================================
    # PHASE 2: AI EXTRACTION (BRONZE AI TABLE)
    # ==========================================
    ai_extract_itviec = BashOperator(
        task_id='ai_extract_itviec', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} ai_extractor.py itviec'
    )
    ai_extract_topcv = BashOperator(
        task_id='ai_extract_topcv', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} ai_extractor.py topcv'
    )

    # ==========================================
    # PHASE 3: SILVER LAYER (DBT TRANSFORMATION)
    # ==========================================
    dbt_seed_data = BashOperator(
        task_id='dbt_seed_taxonomy',
        bash_command=f'cd {DBT_DIR} && dbt seed'
    )

    dbt_build_silver = BashOperator(
        task_id='dbt_build_silver',
        bash_command=f'cd {DBT_DIR} && dbt run --select silver'
    )

    # ==========================================
    # PHASE 4: GOLD LAYER (ANALYTICS & MARTS)
    # ==========================================
    dbt_build_gold = BashOperator(
        task_id='dbt_build_gold', 
        bash_command=f"cd {DBT_DIR} && dbt run --select gold"
    )

    dbt_test = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command=f"cd {DBT_DIR} && dbt test"
    )

    # ==========================================
    # PHASE 5: VECTOR SYNC (SEMANTIC SEARCH)
    # ==========================================
    sync_qdrant = BashOperator(
        task_id='sync_qdrant_vector_db', 
        bash_command=f'cd {SCRIPTS_DIR} && {PYTHON_CMD} sync_qdrant.py'
    )

    # ==========================================
    # WORKFLOW / DEPENDENCIES DEFINITION
    # ==========================================
    
    # 1. Scraping & Ingestion to Bronze
    crawl_itviec >> enrich_itviec
    crawl_topcv >> enrich_topcv
    [enrich_itviec, enrich_topcv] >> wait_for_raw_data >> ingest_landing

    # 2. AI Extraction sequentially to prevent DuckDB file lock
    ingest_landing >> ai_extract_itviec >> ai_extract_topcv

    # 3. dbt Silver Transformation
    ai_extract_topcv >> dbt_seed_data >> dbt_build_silver

    # 4. dbt Gold Marts & Data Quality Tests
    dbt_build_silver >> dbt_build_gold >> dbt_test

    # 5. Vector DB Sync
    dbt_test >> sync_qdrant