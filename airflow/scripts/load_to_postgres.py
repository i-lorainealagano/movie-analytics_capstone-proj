import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection
DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/movie_db"

engine = create_engine(DB_URI)

def load_csv_to_table(csv_path, table_name):
    print(f"Loading {csv_path} → {table_name}")

    df = pd.read_csv(csv_path)

    df.to_sql(
        table_name,
        engine,
        if_exists="replace",   
        index=False
    )

    print(f"Finished loading {table_name}")


def main():
    load_csv_to_table("/opt/airflow/data/movies_main.csv", "clean_movies")
    load_csv_to_table("/opt/airflow/data/movie_extended.csv", "clean_extended")
    load_csv_to_table("/opt/airflow/data/movie_ratings.csv", "clean_ratings")


if __name__ == "__main__":
    main()