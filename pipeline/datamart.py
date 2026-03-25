from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys
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
_log_file = open("/opt/logs/logs.datamart.txt", "w")
sys.stdout = TeeWriter(sys.__stdout__, _log_file)
sys.stderr = TeeWriter(sys.__stderr__, _log_file)

spark = (
    SparkSession.builder
    .appName("datamart")
    .enableHiveSupport()
    .getOrCreate()
)

war_curated = spark.table("default.war_curated")
economics_curated = spark.table("default.economics_curated")
print("[datamart] war_curated : {} lignes".format(war_curated.count()))
print("[datamart] economics_curated : {} lignes".format(economics_curated.count()))

economics_humanitarian = spark.sql("SELECT id, status, (during_war_poverty_rate_pct-prewar_poverty_rate_pct) AS poverty_change_pct, extreme_poverty_rate_pct, food_insecurity_rate_pct, households_fallen_into_poverty_estimate  FROM economics_curated where status = 'Ongoing'")
war_humanitarian = spark.sql("SELECT id, name AS name_of_war, type AS type_of_war, start_year, primary_country FROM war_curated")

humanitarian_data = war_humanitarian.join(economics_humanitarian,on="id",how="inner")

window_humanitarian = Window.orderBy(
    F.col("extreme_poverty_rate_pct").desc(),
    F.col("food_insecurity_rate_pct").desc()
)
humanitarian_data = humanitarian_data.withColumn("rank",F.rank().over(window_humanitarian))
print("[datamart] humanitarian_data : {} lignes".format(humanitarian_data.count()))

(
    humanitarian_data.write
                    .mode("overwrite")
                    .format("parquet")
                    .option("path", "hdfs://namenode:9000/data/gold/gold_output_humanitarian")
                    .saveAsTable("default.gold_output_humanitarian")
)
print("[datamart] gold_output_humanitarian ecrite")

economics_government = spark.sql("SELECT id, status, (during_war_unemployment_pct-prewar_unemployment_pct) AS unemployment_change_pct, youth_unemployment_change_pct, gdp_change_pct, inflation_rate_change_pct, currency_devaluation_pct FROM economics_curated where status = 'Ongoing'")
war_government = spark.sql("SELECT id, name AS name_of_war, type AS type_of_war, start_year, primary_country, most_affected_sector, currency_black_market_rate_gap_pct FROM war_curated")

government_data = war_government.join(economics_government,on="id",how="inner")

government_data = war_government.join(economics_government, on="id", how="inner")
government_data = government_data.withColumn(
    "crisis_score",
    -F.col("gdp_change_pct") * 0.3 +
    F.col("unemployment_change_pct") * 0.2 +
    F.col("inflation_rate_change_pct") * 0.2 +
    F.col("currency_devaluation_pct") * 0.2 +
    F.col("currency_black_market_rate_gap_pct") * 0.1
)
window_government = Window.orderBy(F.col("crisis_score").desc())
government_data = government_data.withColumn("rank", F.rank().over(window_government))
print("[datamart] government_data : {} lignes".format(government_data.count()))

(
    government_data.write
                    .mode("overwrite")
                    .format("parquet")
                    .option("path", "hdfs://namenode:9000/data/gold/gold_output_government")
                    .saveAsTable("default.gold_output_government")
)
print("[datamart] gold_output_government ecrite")

economics_finance = spark.sql("SELECT id, status, (during_war_unemployment_pct-prewar_unemployment_pct) AS unemployment_change_pct, youth_unemployment_change_pct, gdp_change_pct, inflation_rate_change_pct, currency_devaluation_pct FROM economics_curated where status = 'Ongoing'")
war_finance = spark.sql("SELECT id, name AS name_of_war, type AS type_of_war, start_year, primary_country, most_affected_sector, black_market_activity_level, currency_black_market_rate_gap_pct , primary_black_market_goods FROM war_curated")

finance_data = war_finance.join(economics_finance,on="id",how="inner")

finance_data = finance_data.withColumn(
    "investment_score",
    (
        F.col("black_market_activity_level") * 0.3 +
        F.col("currency_black_market_rate_gap_pct") * 0.3+
        F.col("currency_devaluation_pct") * 0.2+ 
        -F.col("gdp_change_pct") * 0.1+
        F.col("inflation_rate_change_pct") * 0.1  
    )
)

window_finance = Window.orderBy(F.col("investment_score").desc())
finance_data=finance_data.withColumn("rank",F.rank().over(window_finance))
print("[datamart] finance_data : {} lignes".format(finance_data.count()))

(
    finance_data.write
                    .mode("overwrite")
                    .format("parquet")
                    .option("path", "hdfs://namenode:9000/data/gold/gold_output_finance") 
                    .saveAsTable("default.gold_output_finance")
)
print("[datamart] gold_output_finance ecrite")
print("[datamart] pipeline termine")