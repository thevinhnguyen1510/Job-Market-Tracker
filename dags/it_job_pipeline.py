from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Configure default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1), # Airflow needs a past timestamp to start
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # If error, try to run again 1 time
    'retry_delay': timedelta(minutes=5), # Wait 5 minutes before trying again
}

# 2. Initialize DAG (Pipeline script)
with DAG(
    'vietnam_it_job_market_etl',
    default_args=default_args,
    description='Automated Pipeline: Crawl -> AI Enrich -> Analyze',
    #schedule_interval='@daily', # Schedule: Run 1 time per day at midnight
    schedule_interval=None,
    catchup=False,
    tags=['job_market', 'duckdb', 'openai'],
) as dag:

    # TASK 1: Crawl data (Internet -> Bronze)
    # Note: Replace 'crawl_itviec.py' with the actual name of your crawl script
    run_crawler = BashOperator(
        task_id='crawl_itviec_to_bronze',
        bash_command='cd /opt/airflow/scripts && python crawl_data.py',
    )

    # TASK 1.5: Crawl job details (Internet -> Bronze)
    run_crawler_details = BashOperator(
        task_id='crawl_job_details_to_bronze',
        bash_command='cd /opt/airflow/scripts && python enrich_job_details.py',
    )

    # TASK 2: Clean / Normalize data using AI (Bronze -> Silver)    
    run_ai_extractor = BashOperator(
        task_id='run_ai_enrichment_to_silver',
        bash_command='cd /opt/airflow/scripts && python ai_extractor.py',
    )

    # TASK 3: Analyze and generate report (Silver -> Gold)
    run_market_analysis = BashOperator(
        task_id='run_market_analysis_to_gold',
        bash_command='cd /opt/airflow/scripts && python market_insight.py',
    )

    # 3. Set execution order (River flow)
    # Crawl done -> Run AI extraction -> Run AI extraction done -> Run analysis
    run_crawler >> run_crawler_details >> run_ai_extractor >> run_market_analysis