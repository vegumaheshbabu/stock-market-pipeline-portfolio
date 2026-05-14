from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when

def get_spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def test_silver_price_rounding():
    spark = get_spark()
    data = [("AAPL", 273.054, 274.275, 270.29, 270.33, 270.23, "2026-04-21T00:15:40")]
    columns = ["symbol", "price", "high", "low", "open", "prev_close", "timestamp"]
    df = spark.createDataFrame(data, columns)
    df_silver = df.withColumn("price", round(col("price"), 2))
    result = df_silver.collect()[0]
    assert result["price"] == 273.05

def test_no_null_prices():
    spark = get_spark()
    data = [("AAPL", 273.05, 274.0, 270.0, 270.0, 270.0, "2026-04-21"),
            ("GOOGL", None, 341.0, 336.0, 340.0, 341.0, "2026-04-21")]
    columns = ["symbol", "price", "high", "low", "open", "prev_close", "timestamp"]
    df = spark.createDataFrame(data, columns)
    df_clean = df.filter(col("price").isNotNull())
    assert df_clean.count() == 1

def test_price_change_calculation():
    spark = get_spark()
    data = [("AAPL", 273.05, 270.23)]
    columns = ["symbol", "price", "prev_close"]
    df = spark.createDataFrame(data, columns)
    df = df.withColumn("price_change", round(col("price") - col("prev_close"), 2))
    result = df.collect()[0]
    assert result["price_change"] == 2.82
