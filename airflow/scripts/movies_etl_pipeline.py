from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def extract_data():
    # Runs your Python ETL script to clean and extract raw CSVs
    subprocess.run([
        "python", 
        r"C:\Users\lorai_3r1nn9u\movie-analytics_capstone-proj\scripts\data-cleaning.py"
    ], check=True)

def load_to_postgres():
    # Runs your load script to push data into Postgres
    subprocess.run([
        "python", 
        r"C:\Users\lorai_3r1nn9u\movie-analytics_capstone-proj\airflow\scripts\load_to_postgres.py"
    ], check=True)

def run_dbt():
    # Runs dbt transformations
    subprocess.run([
        "dbt", 
        "run", 
        "--project-dir", 
        r"C:\Users\lorai_3r1nn9u\movie-analytics_capstone-proj\dbt"
    ], check=True)

def test_dbt():
    # Runs dbt tests
    subprocess.run([
        "dbt", 
        "test", 
        "--project-dir", 
        r"C:\Users\lorai_3r1nn9u\movie-analytics_capstone-proj\dbt"
    ], check=True)

with DAG(
    "movies_analytics_etl",
    default_args=default_args,
    description="ETL pipeline for movie analytics",
    start_date=datetime(2026, 3, 30),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # Extraction
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    # Load to Postgres
    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    # Transformations
    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt
    )

    # Tests
    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=test_dbt
    )

    # DAG flow
    start >> extract >> load >> dbt_run >> dbt_test >> end