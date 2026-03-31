FROM apache/airflow:2.10.2

# Switch to root to install system dependencies if needed
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# Switch back to airflow user to install python packages
USER airflow

# Install dbt and the postgres adapter
RUN pip install --no-cache-dir \
    dbt-postgres \
    pandas \
    sqlalchemy \
    psycopg2-binary