from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MoviePipeline") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

print("Spark Started Successfully")
print("Version:", spark.version)

spark.stop()