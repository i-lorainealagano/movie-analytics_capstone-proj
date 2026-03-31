#imports
import os
import pandas as pd
import numpy as np
import json
import ast
from dateutil import parser

class MovieDataCleaner:
    def __init__(self, data_folder="/app/data"):
        self.data_folder = data_folder

        self.movies_file = os.path.join(data_folder, "movies_main.csv")
        self.extended_file = os.path.join(data_folder, "movie_extended.csv")
        self.ratings_file = os.path.join(data_folder, "ratings.json")

        self.movies_df = None
        self.extended_df = None
        self.ratings_df = None

    # === HELPER FUNCTION ===
    @staticmethod
    def print_data_info(df, name):
        print(f"\n=== {name} DATASET ===")
        print("Shape:", df.shape)
        print("Columns:", df.columns.tolist())
        print("Data types:\n", df.dtypes)
        print("Missing values:\n", df.isnull().sum())
        print("Duplicate rows:", df.duplicated().sum())
        print("-" * 50)

    # === LOAD DATASETS ===
    def load_datasets(self):
        print("Loading datasets...\n")

        self.movies_df = pd.read_csv(self.movies_file)
        self.extended_df = pd.read_csv(self.extended_file)

        with open(self.ratings_file, "r") as f:
            ratings_data = json.load(f)

        self.ratings_df = pd.json_normalize(ratings_data)

        self.movies_df.columns = self.movies_df.columns.str.lower().str.strip()
        self.extended_df.columns = self.extended_df.columns.str.lower().str.strip()
        self.ratings_df.columns = self.ratings_df.columns.str.lower().str.strip()

        self.print_data_info(self.movies_df, "MOVIES")
        self.print_data_info(self.extended_df, "EXTENDED")
        self.print_data_info(self.ratings_df, "RATINGS")

    # === CLEAN MOVIES ===
    def clean_movies(self):
        print("\nCleaning Movies dataset...")

        # Convert ID
        self.movies_df["id"] = pd.to_numeric(self.movies_df["id"], errors="coerce").astype("Int64")
        self.movies_df = self.movies_df.dropna(subset=["id"])

        # Convert numeric columns properly
        self.movies_df["budget_clean"] = pd.to_numeric(self.movies_df["budget"], errors="coerce").replace(0, np.nan)
        self.movies_df["revenue_clean"] = pd.to_numeric(self.movies_df["revenue"], errors="coerce").replace(0, np.nan)

        # Fill title
        self.movies_df["title"] = self.movies_df["title"].fillna("Unknown")

        # Remove duplicates
        self.movies_df = self.movies_df.drop_duplicates(subset=["id"])

        # Date Cleaning
        self.movies_df["release_date"] = self.movies_df["release_date"].astype(str).str.strip().replace(["", "nan", "None", "null", "0"], np.nan)
        self.movies_df["release_date_clean"] = pd.to_datetime(self.movies_df["release_date"], errors="coerce")
        self.movies_df["release_year"] = self.movies_df["release_date_clean"].dt.year
        self.movies_df["release_month"] = self.movies_df["release_date_clean"].dt.month

        # Save missing dates
        missing_dates = self.movies_df[self.movies_df["release_date_clean"].isna()]
        missing_file = os.path.join(self.data_folder, "missing_release_dates.csv")
        missing_dates.to_csv(missing_file, index=False)
        print(f"Missing release_date rows saved: {missing_file}, Total: {missing_dates.shape[0]}")

        # === FLAGS ===
        self.movies_df["budget_missing_flag"] = self.movies_df["budget_clean"].isna()
        self.movies_df["revenue_missing_flag"] = self.movies_df["revenue_clean"].isna()
        self.movies_df["suspicious_flag"] = self.movies_df["budget_missing_flag"] & (~self.movies_df["revenue_missing_flag"])

        # Financial classification
        self.movies_df["financial_status"] = self.movies_df.apply(self.classify_financial, axis=1)

        # Save suspicious
        suspicious_movies = self.movies_df[self.movies_df["suspicious_flag"]]
        suspicious_file = os.path.join(self.data_folder, "suspicious_movies.csv")
        suspicious_movies.to_csv(suspicious_file, index=False)
        print(f"Suspicious movies saved: {suspicious_file}, Total: {suspicious_movies.shape[0]}")

    # === CLEAN EXTENDED ===
    def clean_extended(self):
        print("\nCleaning Extended dataset...")
        self.extended_df["id"] = pd.to_numeric(self.extended_df["id"], errors="coerce").astype("Int64")
        self.extended_df = self.extended_df.dropna(subset=["id"])

        self.extended_df["genres"] = self.extended_df["genres"].fillna("Unknown")
        self.extended_df["production_companies"] = self.extended_df["production_companies"].fillna("Unknown")

        def safe_parse(x):
            if pd.isna(x) or x == "[]":
                return []
            try:
                return ast.literal_eval(x)
            except:
                return []

        self.extended_df["production_countries"] = self.extended_df["production_countries"].apply(safe_parse)
        self.extended_df["spoken_languages"] = self.extended_df["spoken_languages"].apply(safe_parse)

        self.extended_df = self.extended_df.drop_duplicates(subset=["id"])

    # === CLEAN RATINGS ===
    def clean_ratings(self):
        print("\nCleaning Ratings dataset...")
        self.ratings_df["movie_id"] = pd.to_numeric(self.ratings_df["movie_id"], errors="coerce").astype("Int64")
        self.ratings_df = self.ratings_df.dropna(subset=["movie_id"])
        self.ratings_df["ratings_summary.std_dev"] = self.ratings_df["ratings_summary.std_dev"].fillna(0)
        self.ratings_df["last_rated"] = pd.to_datetime(self.ratings_df["last_rated"], unit="s", errors="coerce")

    # === ENRICH MOVIES ===
    def enrich_movies(self):
        print("\nEnriching Movies dataset...")

        # Merge extended info first
        self.movies_df = self.movies_df.merge(self.extended_df, on="id", how="left")

        # Revenue category
        revenue_temp = self.movies_df["revenue_clean"].fillna(0)
        try:
            self.movies_df['revenue_category'] = pd.qcut(revenue_temp, q=3, labels=['Low', 'Medium', 'High'], duplicates='drop')
        except ValueError:
            self.movies_df['revenue_category'] = 'Unknown'

        # Merge ratings
        self.movies_df = self.movies_df.merge(
            self.ratings_df[['movie_id', 'ratings_summary.avg_rating']],
            left_on='id',
            right_on='movie_id',
            how='left'
        )
        self.movies_df["has_ratings"] = self.movies_df["ratings_summary.avg_rating"].notna()
        self.movies_df["ratings_summary.avg_rating"] = self.movies_df["ratings_summary.avg_rating"].fillna(0)

        # Data completeness
        self.movies_df["data_completeness"] = np.where(
            self.movies_df["budget_clean"].notna() &
            self.movies_df["revenue_clean"].notna() &
            self.movies_df["release_date_clean"].notna(),
            "Complete",
            "Incomplete"
        )

    # === PREPARE FOR POSTGRESQL ===
    def prepare_for_pg(self, df):
        df_copy = df.copy()

        # Ensure movie_id exists (for NOT NULL PK)
        df_copy["movie_id"] = df_copy["id"]

        # Convert list/dict columns to JSON string
        for col in ["genres", "production_companies", "production_countries", "spoken_languages"]:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(lambda x: json.dumps(x) if not isinstance(x, float) else '[]')

        # Replace NaN with None for numeric columns
        for col in ["budget_clean", "revenue_clean", "ratings_summary.avg_rating"]:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].replace({np.nan: None})

        # Format dates as ISO
        if "release_date_clean" in df_copy.columns:
            df_copy["release_date_clean"] = df_copy["release_date_clean"].dt.strftime("%Y-%m-%d")

        return df_copy

    # === SAVE ===
    def save_cleaned(self):
        print("\nSaving cleaned datasets...")

        # Prepare movies dataframe for PostgreSQL
        movies_clean = self.prepare_for_pg(self.movies_df)
        movies_clean.to_csv(os.path.join(self.data_folder, "clean_movies.csv"), index=False, quoting=1)  # QUOTE_ALL

        # Save other CSVs normally
        self.extended_df.to_csv(os.path.join(self.data_folder, "clean_extended.csv"), index=False)
        self.ratings_df.to_csv(os.path.join(self.data_folder, "clean_ratings.csv"), index=False)
        self.movies_df.to_csv(os.path.join(self.data_folder, "movies_with_flags.csv"), index=False)

        print("\n✅ Cleaned and enriched datasets saved successfully!")
        print("✅ PostgreSQL-ready CSV: clean_movies.csv")

    # === UTILITIES ===
    @staticmethod
    def classify_financial(row):
        if row["budget_missing_flag"] and row["revenue_missing_flag"]:
            return "Both Missing"
        elif row["budget_missing_flag"]:
            return "Revenue Only"
        elif row["revenue_missing_flag"]:
            return "Budget Only"
        else:
            return "Complete"

    # === FINAL CHECK ===
    def final_checks(self):
        print("\n=== FINAL DATA CHECK ===")
        print("\nMovies missing values:\n", self.movies_df.isnull().sum())
        print("Movies duplicates:", self.movies_df.duplicated(subset=["id"]).sum())
        print("\nExtended missing values:\n", self.extended_df.isnull().sum())
        print("Extended duplicates:", self.extended_df.duplicated(subset=["id"]).sum())
        print("\nRatings missing values:\n", self.ratings_df.isnull().sum())
        print("Ratings duplicates:", self.ratings_df.duplicated().sum())

    # === RUN ALL ===
    def run_all(self):
        self.load_datasets()
        self.clean_movies()
        self.clean_extended()
        self.clean_ratings()
        self.enrich_movies()
        self.final_checks()
        self.save_cleaned()


# === RUN SCRIPT ===
if __name__ == "__main__":
    cleaner = MovieDataCleaner(data_folder="/app/data")
    cleaner.run_all()