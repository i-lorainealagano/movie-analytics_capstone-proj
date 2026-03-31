import os
import pandas as pd
import json

# === PROJECT PATHS ===
project_root = os.path.dirname(os.path.abspath(__file__))
data_folder = os.path.join(project_root, "data")

movies_main_file = os.path.join(data_folder, "movies_main.csv")
movies_extended_file = os.path.join(data_folder, "movie_extended.csv")
ratings_file = os.path.join(data_folder, "ratings.json")

# === LOAD DATASETS ===
print("Loading Movies_main.csv...")
movies_df = pd.read_csv(movies_main_file)
print(f"Total rows: {movies_df.shape[0]}\n")

# === PARSE release_date ===
# Try multiple date formats
date_formats = ["%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y"]

def parse_date(x):
    for fmt in date_formats:
        try:
            return pd.to_datetime(x, format=fmt)
        except (ValueError, TypeError):
            continue
    return pd.NaT

movies_df['release_date_clean'] = movies_df['release_date'].apply(parse_date)

# === FIND MISSING OR INVALID DATES ===
problematic_dates = movies_df[movies_df['release_date_clean'].isnull()]
print("=== Movies with MISSING or INVALID release_date ===")
print(problematic_dates[['id', 'title', 'release_date']])

print(f"\nTotal problematic release_date rows: {problematic_dates.shape[0]}")

# Optional: save problematic rows to a CSV for manual inspection
problematic_dates.to_csv(os.path.join(data_folder, "problematic_release_dates.csv"), index=False)
print("\n✅ Problematic release dates saved to 'problematic_release_dates.csv'")