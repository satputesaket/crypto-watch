import json
import time
import requests

from kafka import KafkaProducer
from app.configuration.config import config

import logging
from app.configuration.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAKFA_BROKER = config["kafka"]["bootstrap_servers"]
KAFKA_TOPIC = config["kafka"]["topic"]

COINGEKCO_URL = config["coingecko"]["url"] 

REQUEST_INTERVAL = 30
REQUEST_TIMEOUT = 30


PARAMS = {
    'vs_currency': config["coingecko"]["vs_currency"],
    'ids': config["coingecko"]["ids"],
    'order': config["coingecko"]["order"],
    'per_page': config["coingecko"]["per_page"],
    'page': config["coingecko"]["page"],
    'sparkline': config["coingecko"]["sparkline"],
    'price_change_percentage': config["coingecko"]["price_change_percentage"],
}

DESIRED_KEYS = [
    "id", "symbol", "current_price", "market_cap",
    "total_volume", "high_24h", "low_24h", "last_updated"
]

# ---------------------------------------------------------
# Kafka
# ---------------------------------------------------------

def create_kafka_producer() -> KafkaProducer:
    """Create and configure Kafka producer."""

    return KafkaProducer(
        bootstrap_servers=[KAKFA_BROKER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

# ---------------------------------------------------------
# CoinGecko
# ---------------------------------------------------------

def fetch_market_data() -> list[dict]:
    """Fetch cryptocurrency market data from CoinGecko."""

    try:
        response = requests.get(
            COINGEKCO_URL,
            params=PARAMS,
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise ValueError("Unexpected response format from CoinGecko")

        logger.info(
            "Successfully fetched %d records from CoinGecko",
            len(data),
        )

        return data

    except requests.RequestException:
        logger.exception("Failed to fetch data from CoinGecko")
        raise

    except ValueError:
        logger.exception("Invalid response received from CoinGecko")
        raise


# ---------------------------------------------------------
# Transformation
# ---------------------------------------------------------

def transform_market_data(data: list[dict]) -> list[dict]:
    """Keep only the fields required by the pipeline."""

    return [
        {
            key: coin.get(key)
            for key in DESIRED_KEYS
        }
        for coin in data
    ]


def build_payload(data: list[dict]) -> dict:
    """Build Kafka message payload."""

    return {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "data": data,
    }

# ---------------------------------------------------------
# Kafka publishing
# ---------------------------------------------------------

def publish_to_kafka(
    producer: KafkaProducer,
    payload: dict,
) -> None:
    """Publish payload to Kafka."""

    future = producer.send(
        KAFKA_TOPIC,
        value=payload,
    )

    # Wait for Kafka acknowledgement.
    future.get(timeout=10)

    logger.info(
        "Published %d records to Kafka topic '%s'",
        len(payload["data"]),
        KAFKA_TOPIC,
    )


# ---------------------------------------------------------
# Main ingestion loop
# ---------------------------------------------------------

def run_ingestion() -> None:
    """Continuously fetch and publish cryptocurrency market data."""

    producer = create_kafka_producer()

    logger.info("Starting crypto market ingestion")

    try:
        while True:

            try:
                raw_data = fetch_market_data()

                filtered_data = transform_market_data(
                    raw_data
                )

                payload = build_payload(
                    filtered_data
                )

                publish_to_kafka(
                    producer,
                    payload,
                )

            except Exception:
                logger.exception(
                    "Error during ingestion cycle"
                )

            time.sleep(REQUEST_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Ingestion stopped manually")

    finally:
        logger.info("Closing Kafka producer")
        producer.flush()
        producer.close()


# ---------------------------------------------------------
# Application entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    print("Started")
    run_ingestion()

  
