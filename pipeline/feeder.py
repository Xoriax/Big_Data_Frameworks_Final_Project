from datetime import date
from pyspark.sql import SparkSession, functions as F
import time

spark = (
    SparkSession.builder
    .appName("feeder")
    .getOrCreate()
)

input_path = "file:///source/war.csv" 
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

today = date.today()
df2 = (
    df.withColumn("year", F.lit(today.year))
        .withColumn("month", F.lit(today.month))
        .withColumn("day", F.lit(today.day))
)

df2.cache()

df2.show(5)

r =  df2.count()
print("Nombre de lignes : {}".format(r))

output_base = "hdfs://namenode:9000/data/raw/war_partitioned"

time.sleep(120)

(
    df2.repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_base)
)

# ajouter container pour le sql car besoin de partitionner countries.sql
