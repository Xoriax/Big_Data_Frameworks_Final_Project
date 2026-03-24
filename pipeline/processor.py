from datetime import date
from pyspark.sql import SparkSession, functions as F
import sys
import os
import time

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
_log_file = open("/opt/logs/logs.processor.txt", "w")
sys.stdout = TeeWriter(sys.__stdout__, _log_file)
sys.stderr = TeeWriter(sys.__stderr__, _log_file)

spark = (
    SparkSession.builder
    .appName("processor")
    .enableHiveSupport()
    .getOrCreate()
)

input_path = "hdfs://namenode:9000/data/raw/war_partitioned" 
print("[processor] Lecture de war_partitioned...")
df = (
    spark.read.parquet(input_path)
)

df2 = (df
    .withColumn("id", F.col("Conflict_Id").cast("int"))
    .withColumn("name", F.col("Conflict_Name")) 
    .withColumn("type", F.col("Conflict_Type"))
    .withColumn("region", F.col("Region"))
    .withColumn("start_year", F.col("Start_Year").cast("int"))
    .withColumn("end_year", F.col("End_Year").cast("int"))
    .withColumn("primary_country", F.col("Primary_Country"))
    .withColumn("most_affected_sector", F.col("Most_Affected_Sector"))
    .withColumn("cost_of_war_usd", F.col("Cost_of_War_USD").cast("double"))
    .withColumn("black_market_activity_level", F.col("Black_Market_Activity_Level").cast("double"))
    .withColumn("primary_black_market_goods", F.col("Primary_Black_Market_Goods"))
    .withColumn("currency_black_market_rate_gap_pct", F.col("Currency_Black_Market_Rate_Gap_%").cast("double"))
    .select("name", "type", "region", "start_year", "end_year", "primary_country", "most_affected_sector", "cost_of_war_usd","black_market_activity_level", "currency_black_market_rate_gap_pct", "primary_black_market_goods" , "id"))

today=date.today()
df3= (
    df2.withColumn("year", F.lit(today.year))
        .withColumn("month", F.lit(today.month))
        .withColumn("day", F.lit(today.day))
)
print("[processor] war : {} lignes transformees".format(df3.count()))

time.sleep(120)

spark.sql("DROP TABLE IF EXISTS default.war_curated")

(df3.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .saveAsTable("default.war_curated"))
print("[processor] Table default.war_curated ecrite avec succes")

input_path = "hdfs://namenode:9000/data/raw/economics_partitioned" 
print("[processor] Lecture de economics_partitioned...")
df = (
    spark.read.parquet(input_path)
)

df2_economics = (df
    .withColumn("id", F.col("Conflict_Id").cast("int"))
    .withColumn("prewar_unemployment_pct", F.col("Pre_War_Unemployment_pct").cast("double"))
    .withColumn("during_war_unemployment_pct", F.col("During_War_Unemployment_pct").cast("double"))
    .withColumn("status", F.col("Status"))
    .withColumn("youth_unemployment_change_pct", F.col("Youth_Unemployment_Change_pct").cast("double"))
    .withColumn("prewar_poverty_rate_pct", F.col("Pre_War_Poverty_Rate_pct").cast("double"))
    .withColumn("during_war_poverty_rate_pct", F.col("During_War_Poverty_Rate_pct").cast("double"))
    .withColumn("extreme_poverty_rate_pct", F.col("Extreme_Poverty_Rate_pct").cast("double"))
    .withColumn("food_insecurity_rate_pct", F.col("Food_Insecurity_Rate_pct").cast("double"))
    .withColumn("households_fallen_into_poverty_estimate", F.col("Households_Fallen_Into_Poverty").cast("int"))
    .withColumn("gdp_change_pct", F.col("GDP_Change_pct").cast("double"))
    .withColumn("inflation_rate_change_pct", F.col("Inflation_Rate_pct").cast("double"))
    .withColumn("currency_devaluation_pct", F.col("Currency_Devaluation_pct").cast("double"))
    .withColumn("informal_economy_size_pre_war_pct", F.col("Informal_Economy_Pre_War_pct").cast("double"))
    .withColumn("informal_economy_size_during_war_pct", F.col("Informal_Economy_During_War_pct").cast("double"))
    .withColumn("war_profiteering_documented", F.col("War_Profiteering_Documented"))
    .select("prewar_unemployment_pct", "during_war_unemployment_pct", "status", "youth_unemployment_change_pct", "prewar_poverty_rate_pct", "during_war_poverty_rate_pct", "extreme_poverty_rate_pct", "food_insecurity_rate_pct", "households_fallen_into_poverty_estimate", "gdp_change_pct", "inflation_rate_change_pct", "currency_devaluation_pct", "informal_economy_size_pre_war_pct", "informal_economy_size_during_war_pct","war_profiteering_documented", "id"))

today=date.today()
df3_economics= (
    df2_economics.withColumn("year", F.lit(today.year))
        .withColumn("month", F.lit(today.month))
        .withColumn("day", F.lit(today.day))
)
print("[processor] economics : {} lignes transformees".format(df3_economics.count()))

spark.sql("DROP TABLE IF EXISTS default.economics_curated")

(df3_economics.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .saveAsTable("default.economics_curated"))
print("[processor] Table default.economics_curated ecrite avec succes")