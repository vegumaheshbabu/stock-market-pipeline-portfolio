# Databricks notebook source
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