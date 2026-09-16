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


try:
    logger.info("Starting crypto market ingestion")
    while True:
        #response = requests.get(COINGEKCO_URL, params=PARAMS)
        try:
            response = requests.get(
                COINGEKCO_URL,
                params=PARAMS,
                timeout=30
            )
            response.raise_for_status()

        except requests.RequestException:
            logger.exception("Failed to fetch data from CoinGecko")
            time.sleep(30)
            continue
        # response.raise_for_status()
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
        logger.info(
                "Pushed %d records at %s",
                len(filtered_data),
                payload["timestamp"]
            )

        
except KeyboardInterrupt:
    logger.info("\nStopped manually.")

finally:
    producer.close()        
