import os
from pathlib import Path
import pandas as pd
import json
from pandas import json_normalize
from sqlalchemy import create_engine


DB_URI = "postgresql+psycopg2://airflow:airflow@localhost:5433/movie_db"
engine = create_engine(DB_URI)


def get_base_path():
    """
    Returns correct data directory depending on environment.
    """
    if os.path.exists("/app/data"):
        return Path("/app/data")

    return Path(r"C:\Users\lorai_3r1nn9u\movie-analytics_capstone-proj\data")


def load_file_to_table(file_path, table_name):
    print(f"Loading {file_path} → {table_name}")

    file_path = Path(file_path)

    if not file_path.exists():
        print(f"⚠️ SKIPPING missing file: {file_path}")
        return

    try:
        # CSV handling
        if file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path)

        # JSON handling
        elif file_path.suffix.lower() == ".json":
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            df = json_normalize(data) if isinstance(data, (list, dict)) else None

        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

        # Load to PostgreSQL
        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False
        )

        print(f"✔ Finished loading {table_name}")

    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")


def main():
    base_path = get_base_path()

    load_file_to_table(base_path / "movies_main.csv", "clean_movies")
    load_file_to_table(base_path / "movie_extended.csv", "clean_extended")
    load_file_to_table(base_path / "ratings.json", "clean_ratings")


if __name__ == "__main__":
    main()