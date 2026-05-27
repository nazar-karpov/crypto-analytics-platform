"""
DAG: dds_to_ddm  (runs every hour, depends on stg_to_dds)

Pure pandas version — no PySpark, no JVM.
Reads DDS Parquet from MinIO, computes 4 marts, writes to ClickHouse.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
CLICKHOUSE_HOST  = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT  = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB    = os.environ.get("CLICKHOUSE_DB", "crypto")

default_args = {"owner": "airflow", "retries": 1}


def _s3fs():
    import s3fs
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )


def _ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        username="default",
        password="",
    )


def _insert(ch, table: str, df):
    if df is None or df.empty:
        print(f"  [skip] {table}: empty")
        return
    ch.insert_df(f"{CLICKHOUSE_DB}.{table}", df)
    print(f"  [ok] {table}: {len(df)} rows")


def compute_marts(**context):
    import pandas as pd
    import numpy as np
    import io

    fs = _s3fs()
    ch = _ch()

    # ── Load DDS ──────────────────────────────────────────────────────────────
    try:
        with fs.open("dds/prices_fact/prices_fact.parquet", "rb") as fh:
            fact = pd.read_parquet(fh)
        with fs.open("dds/coins_dim/coins_dim.parquet", "rb") as fh:
            dim = pd.read_parquet(fh)
    except Exception as exc:
        print(f"DDS not ready yet: {exc}")
        return

    if fact.empty:
        print("Fact table is empty — skipping.")
        return

    fact["event_ts"] = pd.to_datetime(fact["event_ts"], utc=True)
    dim = dim[dim["is_current"] == True][["coin_id","symbol","name"]]
    enriched = fact.merge(dim, on="coin_id", how="left")
    print(f"Enriched rows: {len(enriched)}")

    # ── Mart 1: mart_hourly_ohlcv ─────────────────────────────────────────────
    enriched["hour"] = enriched["event_ts"].dt.floor("h")
    ohlcv = (
        enriched.sort_values("event_ts")
        .groupby(["coin_id","hour"])
        .agg(
            open=("price_usd",  "first"),
            high=("price_usd",  "max"),
            low=("price_usd",   "min"),
            close=("price_usd", "last"),
            volume=("volume_24h","mean"),
        )
        .reset_index()
    )
    ohlcv["updated_at"] = pd.Timestamp.now()
    _insert(ch, "mart_hourly_ohlcv", ohlcv)

    # ── Mart 2: mart_top_movers ───────────────────────────────────────────────
    today = pd.Timestamp.now(tz="UTC").normalize()
    today_df = enriched[enriched["event_ts"] >= today]
    if not today_df.empty:
        latest = (
            today_df.sort_values("event_ts", ascending=False)
            .drop_duplicates(subset=["coin_id"])
        )
        gainers = latest.nlargest(10, "price_change_24h_pct").copy()
        gainers["direction"] = "gainer"
        gainers["rank"] = range(1, len(gainers)+1)
        losers = latest.nsmallest(10, "price_change_24h_pct").copy()
        losers["direction"] = "loser"
        losers["rank"] = range(1, len(losers)+1)
        movers = pd.concat([gainers, losers], ignore_index=True)
        movers["date"] = today.date()
        movers["updated_at"] = pd.Timestamp.now()
        movers = movers[["date","coin_id","symbol","name",
                          "price_change_24h_pct","direction","rank","updated_at"]]
        movers["rank"] = movers["rank"].astype("int16")
        _insert(ch, "mart_top_movers", movers)

    # ── Mart 3: mart_market_overview ──────────────────────────────────────────
    # First: get ONE value per coin per hour (mean), then sum across coins
    per_coin_hour = (
        enriched.groupby(["hour", "coin_id"])
        .agg(market_cap=("market_cap", "mean"),
             volume_24h=("volume_24h", "mean"))
        .reset_index()
    )
    total = (
        per_coin_hour.groupby("hour")
        .agg(total_market_cap=("market_cap", "sum"),
             total_volume=("volume_24h", "sum"))
        .reset_index()
    )
    btc = (per_coin_hour[per_coin_hour["coin_id"] == "bitcoin"]
           .rename(columns={"market_cap": "btc_mc"})[["hour", "btc_mc"]])
    eth = (per_coin_hour[per_coin_hour["coin_id"] == "ethereum"]
           .rename(columns={"market_cap": "eth_mc"})[["hour", "eth_mc"]])

    overview = total.merge(btc, on="hour", how="left").merge(eth, on="hour", how="left")
    overview["btc_dominance"] = (overview["btc_mc"].fillna(0) / overview["total_market_cap"].replace(0, float("nan")) * 100).fillna(0)
    overview["eth_dominance"] = (overview["eth_mc"].fillna(0) / overview["total_market_cap"].replace(0, float("nan")) * 100).fillna(0)
    overview["updated_at"] = pd.Timestamp.now()
    overview = overview[["hour","total_market_cap","btc_dominance","eth_dominance","total_volume","updated_at"]]
    _insert(ch, "mart_market_overview", overview)

    # ── Mart 4: mart_coin_stats ───────────────────────────────────────────────
    now = pd.Timestamp.now(tz="UTC")
    d7  = now - pd.Timedelta(days=7)
    d30 = now - pd.Timedelta(days=30)

    def coin_stats(g):
        g7  = g[g["event_ts"] >= d7]["price_usd"]
        g30 = g[g["event_ts"] >= d30]["price_usd"]
        avg7  = float(g7.mean())  if len(g7)  > 0 else 0.0
        avg30 = float(g30.mean()) if len(g30) > 0 else 0.0
        vol30 = float(g30.std())  if len(g30) > 1 else 0.0
        if len(g30) > 1:
            roll_max = g30.cummax()
            dd = (g30 - roll_max) / roll_max * 100
            max_dd = float(dd.min())
        else:
            max_dd = 0.0
        return pd.Series({
            "avg_price_7d":    avg7,
            "avg_price_30d":   avg30,
            "volatility_30d":  0.0 if np.isnan(vol30)  else vol30,
            "max_drawdown_30d":0.0 if np.isnan(max_dd) else max_dd,
            "last_price":      float(g.sort_values("event_ts")["price_usd"].iloc[-1]),
            "symbol":          g["symbol"].iloc[-1] if "symbol" in g.columns else "",
            "name":            g["name"].iloc[-1]   if "name"   in g.columns else "",
        })

    stats = (
        enriched.groupby("coin_id", group_keys=False)
        .apply(coin_stats)
        .reset_index()
    )
    stats["updated_at"] = pd.Timestamp.now()
    _insert(ch, "mart_coin_stats", stats)

    print("All marts done.")


with DAG(
    dag_id="dds_to_ddm",
    description="DDS → DDM: compute marts (pandas, no Spark)",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ddm","clickhouse"],
) as dag:

    wait_for_dds = ExternalTaskSensor(
        task_id="wait_for_stg_to_dds",
        external_dag_id="stg_to_dds",
        external_task_id="stg_to_dds_spark",
        timeout=3600,
        mode="reschedule",
        poke_interval=30,
    )

    compute = PythonOperator(
        task_id="compute_marts",
        python_callable=compute_marts,
    )

    wait_for_dds >> compute
