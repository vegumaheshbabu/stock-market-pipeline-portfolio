# Databricks notebook source
# MAGIC %pip install confluent-kafka

# COMMAND ----------

from confluent_kafka import Consumer
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date, lit

# Confluent credentials
BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
API_KEY = "J6QAE6QW673ZKOEI"
API_SECRET = "cflt8RTenN5IuMenC5s83rR45U61qpupKzcsifpBG+/5wGYoTnbAMeIakJkf6pYQ"

# Kafka Consumer config
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
    'group.id': 'databricks-bronze-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['Stock-market-data'])

# Collect messages
messages = []
print("📡 Reading from Kafka topic...")

print("test")

start = time.time()
while time.time() - start < 30:  # read for 30 seconds
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue
    data = json.loads(msg.value().decode('utf-8'))
    # Add metadata columns
    data['ingestion_timestamp'] = datetime.utcnow().isoformat()
    data['kafka_topic'] = msg.topic()
    data['kafka_partition'] = msg.partition()
    data['kafka_offset'] = msg.offset()
    messages.append(data)
    print(f"✅ Received: {data['symbol']} | Price: {data['price']}")

consumer.close()
print(f"\n📦 Total messages received: {len(messages)}")

# Save to Bronze layer
if messages:
    df_bronze = spark.createDataFrame(messages)
    df_bronze = df_bronze.withColumn("ingestion_date", current_date())
    
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("workspace.bronze.stock_market_raw")
    
    print(f"✅ {len(messages)} records written to Bronze layer!")
    display(df_bronze)
else:
    print("⚠️ No messages received — check producer is running on EC2!")
