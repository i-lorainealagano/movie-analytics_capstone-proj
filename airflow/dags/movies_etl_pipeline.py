from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta
import subprocess

PROJECT_ROOT = "/opt/airflow/dags"
DBT_PROJECT_DIR = "/usr/app/"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_data():
    subprocess.run([
        "python",
        "/app/scripts/data-cleaning.py"
    ], check=True)


def load_to_postgres():
    subprocess.run([
        "python",
        "/app/scripts/load_to_postgres.py"
    ], check=True)


def run_dbt():
    subprocess.run([
        "docker",
        "exec",
        "dbt-movie",
        "dbt",
        "run",
        "--project-dir",
        "/usr/app"
    ], check=True)


def test_dbt():
    subprocess.run([
        "docker",
        "exec",
        "dbt-movie",
        "dbt",
        "test",
        "--project-dir",
        "/usr/app"
    ], check=True)

with DAG(
    dag_id="movies_analytics_etl",
    default_args=default_args,
    description="ETL pipeline for movie analytics",
    start_date=datetime(2026, 3, 30),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=test_dbt
    )

    start >> extract >> load >> dbt_run >> dbt_test >> end