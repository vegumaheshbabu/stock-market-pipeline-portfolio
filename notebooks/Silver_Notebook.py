# Databricks notebook source
from pyspark.sql.functions import (
    col, to_timestamp, round, when, 
    current_timestamp, current_date,
    lag, current_timestamp, current_date
)
from pyspark.sql.window import Window

# Read from Bronze
df_bronze = spark.read.table("workspace.bronze.stock_market_raw")

# Only process today's records (incremental)
df_new = df_bronze.filter(col("ingestion_date") == current_date())

print(f"📦 Records to process: {df_new.count()}")

# Transformations
window = Window.partitionBy("symbol").orderBy("timestamp")

df_silver = df_new \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("price", round(col("price"), 2)) \
    .withColumn("high", round(col("high"), 2)) \
    .withColumn("low", round(col("low"), 2)) \
    .withColumn("open", round(col("open"), 2)) \
    .withColumn("prev_close", round(col("prev_close"), 2)) \
    .withColumn("price_change", round(col("price") - col("prev_close"), 2)) \
    .withColumn("price_change_pct", round((col("price") - col("prev_close")) / col("prev_close") * 100, 2)) \
    .withColumn("is_positive", when(col("price") >= col("prev_close"), True).otherwise(False)) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .dropDuplicates(["symbol", "timestamp"]) \
    .filter(col("price") > 0) \
    .filter(col("price").isNotNull())

# MERGE into Silver — existing records never disturbed
from delta.tables import DeltaTable

# Check if silver table exists
if spark.catalog.tableExists("workspace.silver.stock_market_clean"):
    silver_table = DeltaTable.forName(spark, "workspace.silver.stock_market_clean")
    
    silver_table.alias("silver").merge(
        df_silver.alias("new"),
        "silver.symbol = new.symbol AND silver.timestamp = new.timestamp"
    ).whenNotMatchedInsertAll() \
     .execute()
    
    print("✅ Silver layer updated using MERGE — existing records untouched!")
else:
    df_silver.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("workspace.silver.stock_market_clean")
    
    print("✅ Silver layer created for first time!")

display(df_silver)

# COMMAND ----------

from pyspark.sql.functions import (
    avg, max, min, first, last,
    count, round, current_timestamp,
    current_date, sum
)

# Read from Silver
df_silver = spark.read.table("workspace.silver.stock_market_clean")

# Gold Layer — Daily aggregations per stock
df_gold = df_silver \
    .groupBy("symbol", "ingestion_date") \
    .agg(
        first("open").alias("open_price"),
        max("high").alias("daily_high"),
        min("low").alias("daily_low"),
        last("price").alias("close_price"),
        avg("price").alias("avg_price"),
        avg("price_change_pct").alias("avg_price_change_pct"),
        count("*").alias("total_records")
    ) \
    .withColumn("open_price", round(col("open_price"), 2)) \
    .withColumn("daily_high", round(col("daily_high"), 2)) \
    .withColumn("daily_low", round(col("daily_low"), 2)) \
    .withColumn("close_price", round(col("close_price"), 2)) \
    .withColumn("avg_price", round(col("avg_price"), 2)) \
    .withColumn("avg_price_change_pct", round(col("avg_price_change_pct"), 2)) \
    .withColumn("gold_updated_at", current_timestamp())

# MERGE into Gold — incremental load
from delta.tables import DeltaTable

if spark.catalog.tableExists("workspace.gold.stock_market_daily"):
    gold_table = DeltaTable.forName(spark, "workspace.gold.stock_market_daily")
    
    gold_table.alias("gold").merge(
        df_gold.alias("new"),
        "gold.symbol = new.symbol AND gold.ingestion_date = new.ingestion_date"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print("✅ Gold layer updated using MERGE!")
else:
    df_gold.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("workspace.gold.stock_market_daily")
    
    print("✅ Gold layer created for first time!")

display(df_gold)
print("\n🎉 Full pipeline complete! Bronze → Silver → Gold ✅")