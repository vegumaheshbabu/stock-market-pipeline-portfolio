from confluent_kafka import Producer
import requests
import json
import time
from datetime import datetime

# Confluent credentials — replace with your NEW cluster details
BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
API_KEY = "J6QAE6QW673ZKOEI"
API_SECRET = "cflt8RTenN5IuMenC5s83rR45U61qpupKzcsifpBG+/5wGYoTnbAMeIakJkf6pYQ"

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET
}

producer = Producer(conf)

# Finnhub API key
FINNHUB_KEY = "d7h39h1r01qhiu0a3qp0d7h39h1r01qhiu0a3qpg"

# Stock symbols — we can add more later
SYMBOLS = ["AAPL", "GOOGL", "MSFT"]

def fetch_stock(symbol):
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_KEY}"
    response = requests.get(url).json()
    try:
        return {
            "symbol": symbol,
            "price": float(response["c"]),
            "high": float(response["h"]),
            "low": float(response["l"]),
            "open": float(response["o"]),
            "prev_close": float(response["pc"]),
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Error fetching {symbol}:", response)
        return None

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} partition {msg.partition()}")

while True:
    for symbol in SYMBOLS:
        stock = fetch_stock(symbol)
        if stock:
            producer.produce(
                "Stock-market-data",  # your new topic name
                json.dumps(stock),
                callback=delivery_report
            )
            producer.flush()
            print(f"Sent: {stock}")
        time.sleep(2)  # small gap between each symbol

    print("Sleeping 24 hours...")
    time.sleep(86400) 
