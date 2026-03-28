"""
DAG: initial_load  (schedule='@once', trigger manually or on first deploy)

Backfills 7 days of historical OHLC data from CoinGecko's
/coins/{id}/market_chart endpoint for the top 50 coins.

Writes data directly to STG layer (s3://stg/prices/) in the same Parquet
schema as the streaming job, so the regular stg_to_dds DAG can process it.

CoinGecko free API rate limit: ~1 req/sec.  We sleep 1.2s between calls.
"""
from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
STG_BUCKET = "stg"
BACKFILL_DAYS = 7

# Top 20 coins for the initial load (to stay within rate limits)
TOP_COINS = [
    "bitcoin", "ethereum", "tether", "binancecoin", "solana",
    "ripple", "usd-coin", "staked-ether", "avalanche-2", "dogecoin",
    "cardano", "tron", "chainlink", "polkadot", "polygon",
    "wrapped-bitcoin", "dai", "shiba-inu", "litecoin", "bitcoin-cash",
]


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _fetch_market_chart(coin_id: str, days: int = 7) -> list[dict]:
    """Fetch OHLC + volume data from CoinGecko market_chart endpoint."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days, "interval": "hourly"}
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                log.warning("Rate limited. Sleeping %ds…", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data
        except requests.RequestException as exc:
            log.error("Attempt %d failed for %s: %s", attempt + 1, coin_id, exc)
            time.sleep(5)
    return {}


def _fetch_coin_meta(coin_id: str) -> dict:
    """Fetch basic metadata (symbol, name) for a coin."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    params = {
        "localization": False,
        "tickers": False,
        "market_data": False,
        "community_data": False,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        d = resp.json()
        return {"symbol": d.get("symbol", "").upper(), "name": d.get("name", coin_id)}
    except Exception as exc:
        log.warning("Could not fetch meta for %s: %s", coin_id, exc)
        return {"symbol": coin_id.upper(), "name": coin_id}


def _write_partition(s3, rows: list[dict], year: int, month: int, day: int, hour: int):
    if not rows:
        return
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df, preserve_index=False)
    key = f"prices/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/data.parquet"
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    s3.put_object(Bucket=STG_BUCKET, Key=key, Body=buf.getvalue().to_pybytes())


def backfill_historical(**context):
    s3 = _s3_client()

    # Group by (year, month, day, hour) to write partitioned Parquet
    from collections import defaultdict
    partitions: dict[tuple, list] = defaultdict(list)

    for coin_id in TOP_COINS:
        log.info("Backfilling %s…", coin_id)
        meta = _fetch_coin_meta(coin_id)
        time.sleep(1.2)

        chart = _fetch_market_chart(coin_id, days=BACKFILL_DAYS)
        if not chart:
            log.warning("No data for %s", coin_id)
            continue

        prices = chart.get("prices", [])          # [[ms, price], ...]
        volumes = chart.get("total_volumes", [])  # [[ms, vol], ...]
        market_caps = chart.get("market_caps", [])

        vol_map = {int(t): v for t, v in volumes}
        mc_map = {int(t): v for t, v in market_caps}

        for ts_ms, price in prices:
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            row = {
                "coin_id": coin_id,
                "symbol": meta["symbol"],
                "name": meta["name"],
                "price_usd": float(price),
                "market_cap": float(mc_map.get(ts_ms, 0)),
                "volume_24h": float(vol_map.get(ts_ms, 0)),
                "price_change_24h_pct": 0.0,  # not available in market_chart
                "timestamp": ts.isoformat(),
                "event_ts": ts,
                "load_ts": datetime.now(timezone.utc),
            }
            key = (ts.year, ts.month, ts.day, ts.hour)
            partitions[key].append(row)

        time.sleep(1.2)  # respect rate limit between coins

    # Write partitions to S3
    total = 0
    for (y, m, d, h), rows in partitions.items():
        _write_partition(s3, rows, y, m, d, h)
        total += len(rows)

    log.info("Initial load complete: %d records across %d partitions", total, len(partitions))


with DAG(
    dag_id="initial_load",
    description="One-time 7-day historical backfill from CoinGecko → STG",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["backfill", "stg"],
) as dag:
    PythonOperator(
        task_id="backfill_historical",
        python_callable=backfill_historical,
        execution_timeout=timedelta(hours=2),
    )
