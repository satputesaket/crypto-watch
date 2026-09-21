from unittest.mock import Mock, patch

import pytest

from app.ingestion.crypto_market_ingestion import (
    transform_market_data,
    build_payload,
    fetch_market_data,
    publish_to_kafka,
)


# ---------------------------------------------------------
# Test data
# ---------------------------------------------------------

SAMPLE_COIN_DATA = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "current_price": 100000,
        "market_cap": 2000000000,
        "total_volume": 50000000,
        "high_24h": 102000,
        "low_24h": 98000,
        "last_updated": "2026-09-21T10:00:00Z",

        # This should NOT appear in transformed data
        "name": "Bitcoin",
        "image": "https://example.com/bitcoin.png",
    }
]


# ---------------------------------------------------------
# transform_market_data()
# ---------------------------------------------------------

def test_transform_market_data():

    result = transform_market_data(SAMPLE_COIN_DATA)

    assert len(result) == 1

    assert result[0]["id"] == "bitcoin"
    assert result[0]["symbol"] == "btc"
    assert result[0]["current_price"] == 100000

    # Make sure unwanted fields are removed
    assert "name" not in result[0]
    assert "image" not in result[0]


# ---------------------------------------------------------
# build_payload()
# ---------------------------------------------------------

def test_build_payload():

    transformed_data = [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "current_price": 100000,
        }
    ]

    payload = build_payload(transformed_data)

    assert "timestamp" in payload
    assert "data" in payload

    assert payload["data"] == transformed_data


# ---------------------------------------------------------
# fetch_market_data()
# ---------------------------------------------------------

@patch("app.ingestion.crypto_market_ingestion.requests.get")
def test_fetch_market_data(mock_get):

    mock_response = Mock()

    mock_response.json.return_value = SAMPLE_COIN_DATA

    mock_get.return_value = mock_response

    result = fetch_market_data()

    assert result == SAMPLE_COIN_DATA

    mock_get.assert_called_once()


# ---------------------------------------------------------
# fetch_market_data() - HTTP failure
# ---------------------------------------------------------

@patch("app.ingestion.crypto_market_ingestion.requests.get")
def test_fetch_market_data_http_failure(mock_get):

    mock_get.side_effect = Exception("Connection failed")

    with pytest.raises(Exception):
        fetch_market_data()


# ---------------------------------------------------------
# publish_to_kafka()
# ---------------------------------------------------------

def test_publish_to_kafka():

    producer = Mock()

    future = Mock()

    producer.send.return_value = future

    payload = {
        "timestamp": "2026-09-21 10:00:00",
        "data": SAMPLE_COIN_DATA,
    }

    publish_to_kafka(producer, payload)

    producer.send.assert_called_once()

    future.get.assert_called_once()