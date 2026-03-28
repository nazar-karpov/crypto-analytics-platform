"""
Kafka producer: polls CoinGecko /coins/markets every 30s,
sends each coin as a JSON message to topic raw_crypto_prices.
"""
import json
import os
import time
import logging
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_crypto_prices")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,  # reduced for faster testing (was 50)
    "page": 1,
    "sparkline": False,
    "price_change_percentage": "24h",
}


def delivery_callback(err, msg):
    if err:
        log.error("Message delivery failed: %s", err)
    else:
        log.debug("Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())


def fetch_coins() -> list[dict]:
    """Fetch top 50 coins from CoinGecko. Returns [] on transient errors."""
    try:
        response = requests.get(COINGECKO_URL, params=COINGECKO_PARAMS, timeout=15)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            log.warning("Rate limited by CoinGecko. Sleeping %ds...", retry_after)
            time.sleep(retry_after)
            return []
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        log.error("CoinGecko request failed: %s", exc)
        return []


def coin_to_message(coin: dict) -> dict:
    """Normalize a CoinGecko market coin entry to our schema."""
    return {
        "coin_id": coin.get("id", ""),
        "symbol": (coin.get("symbol") or "").upper(),
        "name": coin.get("name", ""),
        "price_usd": float(coin.get("current_price") or 0),
        "market_cap": float(coin.get("market_cap") or 0),
        "volume_24h": float(coin.get("total_volume") or 0),
        "price_change_24h_pct": float(coin.get("price_change_percentage_24h") or 0),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    log.info("Producer started. Kafka=%s  Topic=%s  Interval=%ds",
             KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, POLL_INTERVAL)

    while True:
        coins = fetch_coins()
        if coins:
            for coin in coins:
                msg = coin_to_message(coin)
                producer.produce(
                    KAFKA_TOPIC,
                    key=msg["coin_id"].encode(),
                    value=json.dumps(msg).encode(),
                    callback=delivery_callback,
                )
            producer.flush()
            log.info("Published %d messages to %s", len(coins), KAFKA_TOPIC)
        else:
            log.warning("No data fetched; skipping this cycle")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
