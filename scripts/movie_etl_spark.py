import os
import sys
from datetime import datetime

# Ensure PySpark uses correct Python environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# OPTIONAL FIX (needed on Windows Spark)
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce,
    to_timestamp, year, month, expr
)
from pyspark.sql.types import DoubleType


class MovieETL:
    def __init__(self, data_folder="./data"):
        self.data_folder = data_folder

        self.spark = (
            SparkSession.builder
            .appName("MoviePipeline")
            .getOrCreate()
        )

        self.movies_df = None
        self.extended_df = None
        self.ratings_df = None

    # =========================
    # LOAD DATASETS
    # =========================
    def load_datasets(self):
        print("📥 Loading datasets...")

        self.movies_df = self.spark.read.csv(
            os.path.join(self.data_folder, "movies_main.csv"),
            header=True,
            inferSchema=True
        )

        self.extended_df = self.spark.read.csv(
            os.path.join(self.data_folder, "movie_extended.csv"),
            header=True,
            inferSchema=True
        )

        self.ratings_df = self.spark.read.option("multiline", "true").json(
            os.path.join(self.data_folder, "ratings.json")
        )

        print("\nMovies schema:")
        self.movies_df.printSchema()

        print("\nRatings schema:")
        self.ratings_df.printSchema()

        self.ratings_df.show(5, truncate=False)

    # =========================
    # LOWERCASE COLUMNS
    # =========================
    def _lower_columns(self, df):
        for c in df.columns:
            df = df.withColumnRenamed(c, c.lower().strip())
        return df

    # =========================
    # STANDARDIZE IDS
    # =========================
    def standardize_ids(self):
        print("🔧 Standardizing ID columns...")

        self.movies_df = self.movies_df.withColumn("id", expr("try_cast(id as int)"))
        self.extended_df = self.extended_df.withColumn("id", expr("try_cast(id as int)"))
        self.ratings_df = self.ratings_df.withColumn("movie_id", expr("try_cast(movie_id as int)"))

        self.movies_df = self.movies_df.filter(col("id").isNotNull())
        self.extended_df = self.extended_df.filter(col("id").isNotNull())
        self.ratings_df = self.ratings_df.filter(col("movie_id").isNotNull())

    # =========================
    # CLEAN MOVIES
    # =========================
    def clean_movies(self):
        print("🧼 Cleaning Movies dataset...")

        df = self._lower_columns(self.movies_df)

        df = df.withColumn("title", coalesce(col("title"), lit("Unknown")))

        df = df.withColumn("budget_num", col("budget").cast(DoubleType()))
        df = df.withColumn("revenue_num", col("revenue").cast(DoubleType()))

        df = df.withColumn(
            "budget_clean",
            when(col("budget_num") == 0, None).otherwise(col("budget_num"))
        )

        df = df.withColumn(
            "revenue_clean",
            when(col("revenue_num") == 0, None).otherwise(col("revenue_num"))
        )

        df = df.withColumn(
            "release_date_clean",
            to_timestamp(col("release_date"))
        )

        df = df.withColumn("release_year", year(col("release_date_clean")))
        df = df.withColumn("release_month", month(col("release_date_clean")))

        df = df.withColumn("budget_missing_flag", col("budget_clean").isNull())
        df = df.withColumn("revenue_missing_flag", col("revenue_clean").isNull())

        df = df.withColumn(
            "financial_status",
            when(col("budget_missing_flag") & col("revenue_missing_flag"), "Both Missing")
            .when(col("budget_missing_flag"), "Revenue Only")
            .when(col("revenue_missing_flag"), "Budget Only")
            .otherwise("Complete")
        )

        self.movies_df = df

    # =========================
    # CLEAN EXTENDED
    # =========================
    def clean_extended(self):
        print("🧼 Cleaning Extended dataset...")

        df = self._lower_columns(self.extended_df)
        df = df.dropDuplicates(["id"])

        self.extended_df = df

    # =========================
    # CLEAN RATINGS
    # =========================
    def clean_ratings(self):
        print("🧼 Cleaning Ratings dataset...")

        df = self._lower_columns(self.ratings_df)

        df = df.withColumn("avg_rating", col("ratings_summary.avg_rating"))

        df = df.withColumn(
            "last_rated",
            to_timestamp(col("last_rated"))
        )

        self.ratings_df = df

    # =========================
    # ENRICH DATA
    # =========================
    def enrich_movies(self):
        print("🔗 Enriching dataset...")

        self.extended_df = self.extended_df.dropDuplicates(["id"])
        self.ratings_df = self.ratings_df.dropDuplicates(["movie_id"])

        ratings_df = self.ratings_df.select(
            col("movie_id").alias("id"),
            col("avg_rating")
        )

        df = self.movies_df.join(self.extended_df, on="id", how="left")
        df = df.join(ratings_df, on="id", how="left")

        df = df.withColumn("avg_rating", coalesce(col("avg_rating"), lit(0)))
        df = df.withColumn("has_ratings", col("avg_rating") > 0)

        df = df.withColumn(
            "data_completeness",
            when(
                col("budget_clean").isNotNull() &
                col("revenue_clean").isNotNull() &
                col("release_date_clean").isNotNull(),
                "Complete"
            ).otherwise("Incomplete")
        )

        self.movies_df = df

    # =========================
    # FINAL CHECKS
    # =========================
    def final_checks(self):
        print("\n📊 Running final checks...")

        print("Movies rows:", self.movies_df.count())
        print("Extended rows:", self.extended_df.count())
        print("Ratings rows:", self.ratings_df.count())

        print("\nSample output:")
        self.movies_df.select(
            "id", "title", "avg_rating", "financial_status"
        ).show(5)

        print("\n========================")
        print("✅ ETL PIPELINE SUCCESS")
        print("========================\n")

    # =========================
    # SAVE OUTPUTS (FIXED)
    # =========================
    def save_cleaned(self):
        print("💾 Saving outputs...")

        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        movies_output = f"output/movies_clean_{run_id}"
        ratings_output = f"output/ratings_clean_{run_id}"
        extended_output = f"output/extended_clean_{run_id}"

        self.movies_df.write.mode("errorifexists").parquet(movies_output)
        self.ratings_df.write.mode("errorifexists").parquet(ratings_output)
        self.extended_df.write.mode("errorifexists").parquet(extended_output)

    # =========================
    # RUN PIPELINE
    # =========================
    def run_all(self):
        self.load_datasets()
        self.standardize_ids()
        self.clean_movies()
        self.clean_extended()
        self.clean_ratings()
        self.enrich_movies()
        self.final_checks()
        self.save_cleaned()


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    etl = MovieETL("./data")
    etl.run_all()