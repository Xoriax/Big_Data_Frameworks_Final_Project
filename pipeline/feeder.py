from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
import re
import sys
import time
import os

class TeeWriter:
    def __init__(self, *streams):
        self.streams = streams
    def write(self, data):
        for s in self.streams:
            try:
                s.write(data)
                s.flush()
            except Exception:
                pass
    def flush(self):
        for s in self.streams:
            try:
                s.flush()
            except Exception:
                pass

if not os.path.exists("/opt/logs"):
    os.makedirs("/opt/logs")
_log_file = open("/opt/logs/logs.feeder.txt", "w")
sys.stdout = TeeWriter(sys.__stdout__, _log_file)
sys.stderr = TeeWriter(sys.__stderr__, _log_file)

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
    .option("sep", ";")
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

sql_path = "file:///source/economics.sql"

sql_lines = spark.sparkContext.textFile(sql_path)
insert_lines = sql_lines.filter(lambda line: line.strip().upper().startswith("INSERT INTO"))

def parse_economics(line):
    match = re.search(r"VALUES\s*\((.+)\)\s*;", line, re.IGNORECASE)
    if match:
        parts = re.findall(r"'([^']*)'|(-?[\d.]+(?:[Ee][+\-]?\d+)?)", match.group(1))
        values = [p[0] if p[0] != "" else p[1] for p in parts]
        if len(values) == 18:
            try:
                return (
                    int(values[0]),
                    float(values[1]),
                    float(values[2]),
                    float(values[3]),
                    values[4],
                    float(values[5]),
                    float(values[6]),
                    float(values[7]),
                    float(values[8]),
                    float(values[9]),
                    int(values[10]),
                    float(values[11]),
                    float(values[12]),
                    float(values[13]),
                    int(float(values[14])),
                    float(values[15]),
                    float(values[16]),
                    values[17],
                )
            except (ValueError, IndexError):
                return None
    return None

schema_economics = StructType([
    StructField("Conflict_Id", IntegerType(), True),
    StructField("Pre_War_Unemployment_pct", FloatType(), True),
    StructField("During_War_Unemployment_pct", FloatType(), True),
    StructField("Unemployment_Spike_pp", FloatType(), True),
    StructField("Status", StringType(), True),
    StructField("Youth_Unemployment_Change_pct", FloatType(), True),
    StructField("Pre_War_Poverty_Rate_pct", FloatType(), True),
    StructField("During_War_Poverty_Rate_pct", FloatType(), True),
    StructField("Extreme_Poverty_Rate_pct", FloatType(), True),
    StructField("Food_Insecurity_Rate_pct", FloatType(), True),
    StructField("Households_Fallen_Into_Poverty", IntegerType(), True),
    StructField("GDP_Change_pct", FloatType(), True),
    StructField("Inflation_Rate_pct", FloatType(), True),
    StructField("Currency_Devaluation_pct", FloatType(), True),
    StructField("Estimated_Reconstruction_Cost_USD", LongType(), True),
    StructField("Informal_Economy_Pre_War_pct", FloatType(), True),
    StructField("Informal_Economy_During_War_pct", FloatType(), True),
    StructField("War_Profiteering_Documented", StringType(), True),
])

rdd_economics = insert_lines.map(parse_economics).filter(lambda x: x is not None)
df_economics = spark.createDataFrame(rdd_economics, schema_economics)

df_economics2 = (
    df_economics
    .withColumn("year", F.lit(today.year))
    .withColumn("month", F.lit(today.month))
    .withColumn("day", F.lit(today.day))
)

df_economics2.cache()

df_economics2.show(5)

r_economics = df_economics2.count()
print("Nombre de lignes (economics) : {}".format(r_economics))

output_economics = "hdfs://namenode:9000/data/raw/economics_partitioned"

(
    df_economics2.repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_economics)
)