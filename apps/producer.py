from kafka import KafkaProducer
import json
import time
import requests
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
SYMBOL = os.getenv("SYMBOL", "MSFT")

# Kafka producer
producer = KafkaProducer(
    # --- THIS LINE WAS CORRECTED ---
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_stock_data():
    """Fetch 5-minute intraday stock data via RapidAPI Alpha Vantage."""
    url = "https://alpha-vantage.p.rapidapi.com/query"
    headers = {
        "x-rapidapi-host": "alpha-vantage.p.rapidapi.com",
        "x-rapidapi-key": RAPIDAPI_KEY
    }
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": SYMBOL,
        "interval": "5min",       # 5-minute interval
        "datatype": "json",
        "output_size": "compact"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json().get("Time Series (5min)", {})
    return data

# Continuous loop to fetch and send data every 60 seconds
while True:
    try:
        stock_data = get_stock_data()
        record_count = len(stock_data)

        if record_count == 0:
            print(f"[{datetime.now()}] No data received for {SYMBOL}.")
        else:
            print(f"[{datetime.now()}] Fetched {record_count} intraday records for {SYMBOL}")
            for timestamp, values in stock_data.items():
                values["timestamp"] = timestamp
                producer.send("stock_data", value=values)
                print(f"  Sent: {values}")

        print("Waiting 60 seconds before next fetch...\n")
        time.sleep(60)

    except Exception as e:
        print(f"[{datetime.now()}] Error occurred: {e}")
        print("Retrying in 60 seconds...\n")
        time.sleep(60)
