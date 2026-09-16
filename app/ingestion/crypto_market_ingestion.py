import json
import time
import requests
from kafka import KafkaProducer
# from configuration import config
from app.configuration.config import config
import logging

from app.configuration.logging_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)



KAKFA_BROKER = config["kafka"]["bootstrap_servers"]
KAFKA_TOPIC = config["kafka"]["topic"]

COINGEKCO_URL = config["coingecko"]["url"] 

PARAMS = {
    'vs_currency': config["coingecko"]["vs_currency"],
    'ids': config["coingecko"]["ids"],
    'order': config["coingecko"]["order"],
    'per_page': config["coingecko"]["per_page"],
    'page': config["coingecko"]["page"],
    'sparkline': config["coingecko"]["sparkline"],
    'price_change_percentage': config["coingecko"]["price_change_percentage"],
}

desired_keys = [
    "id", "symbol", "current_price", "market_cap",
    "total_volume", "high_24h", "low_24h", "last_updated"
]

producer = KafkaProducer(
    bootstrap_servers=[KAKFA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Streaming filtered crypto data from CoinGecko to Kafka...")

try:
    logger.info("Starting crypto market ingestion")
    while True:
        response = requests.get(COINGEKCO_URL, params=PARAMS)
        if response.status_code == 200:
            data = response.json()

            # Filter only desired keys from each coin
            filtered_data = [
                {key: coin.get(key) for key in desired_keys}
                for coin in data
            ]

            payload = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'data': filtered_data
            }

            producer.send(KAFKA_TOPIC, value=payload)
            print(f"Pushed {len(filtered_data)} records at {payload['timestamp']}")
            logger.info(f"Pushed {len(filtered_data)} records at {payload['timestamp']}")


        else:
            print(f"API Error: {response.status_code} - {response.text}")
            logger.info(f"API Error: {response.status_code} - {response.text}")
        time.sleep(30)
        
except KeyboardInterrupt:
    print("\nStopped manually.")

finally:
    producer.close()        
