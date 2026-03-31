from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# === DEFAULT SETTINGS ===
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# === DAG DEFINITION ===
with DAG(
    dag_id='movie_data_pipeline',
    default_args=default_args,
    description='Movie Data Cleaning and Enrichment Pipeline',
    schedule_interval='@daily',  # manual trigger
    catchup=False,
) as dag:

    # === TASK 1: RUN DATA CLEANING SCRIPT ===
    clean_and_enrich = BashOperator(
        task_id='clean_and_enrich',
        bash_command='python /app/scripts/data-cleaning.py'
    )

    pipeline_success = BashOperator(
        task_id='pipeline_success',
        bash_command='echo "Pipeline executed successfully!"'
    )

    # === TASK FLOW ===
    clean_and_enrich >> pipeline_success