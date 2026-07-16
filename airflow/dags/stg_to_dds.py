"""
DAG: stg_to_dds  (runs every 5 minutes)

Pure pandas + s3fs for STG reads — no PySpark, no JVM startup.
Reads STG Parquet from MinIO, deduplicates, applies SCD2, and writes
the DDS (silver) layer as Iceberg tables:
  - dds.coins_dim    (SCD2 dimension, atomic snapshot overwrite)
  - dds.prices_fact  (append-only fact, no full-table rewrite)

Fast enough for <10k rows/hour.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")

default_args = {"owner": "airflow", "retries": 1}


def _s3fs():
    import s3fs
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )


def process_stg_to_dds(**context):
    import pandas as pd
    import pyarrow as pa
    from iceberg_utils import COINS_DIM_TABLE, PRICES_FACT_TABLE, ensure_tables, get_catalog

    fs = _s3fs()

    # ── 1. Read all STG parquet files ────────────────────────────────────────
    try:
        stg_files = fs.glob("stg/prices/**/*.parquet")
    except Exception as exc:
        print(f"Cannot list STG files: {exc}")
        return

    if not stg_files:
        print("No STG parquet files yet — nothing to process.")
        return

    frames = []
    for f in stg_files:
        try:
            with fs.open(f, "rb") as fh:
                frames.append(pd.read_parquet(fh))
        except Exception as exc:
            print(f"Skipping {f}: {exc}")

    if not frames:
        print("All STG files unreadable.")
        return

    df = pd.concat(frames, ignore_index=True)
    print(f"STG rows loaded: {len(df)}")

    # ── 2. Deduplicate ───────────────────────────────────────────────────────
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    df["load_ts"]  = pd.to_datetime(df["load_ts"],  utc=True)
    df = (
        df.sort_values("load_ts", ascending=False)
          .drop_duplicates(subset=["coin_id", "event_ts"])
          .drop(columns=[c for c in ["year","month","day","hour"] if c in df.columns])
    )
    print(f"After dedup: {len(df)} rows")

    catalog = get_catalog()
    ensure_tables(catalog)
    dim_table = catalog.load_table(COINS_DIM_TABLE)
    fact_table = catalog.load_table(PRICES_FACT_TABLE)

    # ── 3. SCD2 for coins_dim → Iceberg atomic overwrite ─────────────────────
    now_ts  = pd.Timestamp.now(tz="UTC")
    far_future = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")

    dim = dim_table.scan().to_pandas()
    if len(dim) > 0:
        dim["valid_from"] = pd.to_datetime(dim["valid_from"], utc=True)
        dim["valid_to"]   = pd.to_datetime(dim["valid_to"],   utc=True)

    # Latest attrs per coin from incoming batch
    latest_attrs = (
        df.sort_values("event_ts", ascending=False)
          .drop_duplicates(subset=["coin_id"])[["coin_id","symbol","name"]]
    )

    current = dim[dim["is_current"] == True] if len(dim) > 0 else dim.copy()
    historical = dim[dim["is_current"] == False] if len(dim) > 0 else dim.copy()

    # New coins not in dim
    existing_ids = set(current["coin_id"]) if len(current) > 0 else set()
    new_coins = latest_attrs[~latest_attrs["coin_id"].isin(existing_ids)].copy()
    new_coins["valid_from"] = now_ts
    new_coins["valid_to"]   = far_future
    new_coins["is_current"] = True

    # Changed coins
    if len(current) > 0:
        merged = latest_attrs.merge(current[["coin_id","symbol","name"]], on="coin_id", suffixes=("_new","_old"))
        changed_ids = merged[
            (merged["symbol_new"] != merged["symbol_old"]) |
            (merged["name_new"]   != merged["name_old"])
        ]["coin_id"].tolist()
    else:
        changed_ids = []

    if changed_ids:
        current.loc[current["coin_id"].isin(changed_ids), "valid_to"]   = now_ts
        current.loc[current["coin_id"].isin(changed_ids), "is_current"] = False
        new_versions = latest_attrs[latest_attrs["coin_id"].isin(changed_ids)].copy()
        new_versions["valid_from"] = now_ts
        new_versions["valid_to"]   = far_future
        new_versions["is_current"] = True
    else:
        new_versions = pd.DataFrame(columns=["coin_id","symbol","name","valid_from","valid_to","is_current"])

    full_dim = pd.concat([historical, current, new_coins, new_versions], ignore_index=True)
    full_dim = full_dim[["coin_id","symbol","name","valid_from","valid_to","is_current"]]
    full_dim["is_current"] = full_dim["is_current"].astype(bool)

    dim_arrow = pa.Table.from_pandas(full_dim, schema=dim_table.schema().as_arrow(), preserve_index=False)
    dim_table.overwrite(dim_arrow)
    print(f"SCD2 dim written (Iceberg snapshot overwrite): {len(full_dim)} records")

    # ── 4. Append new rows to prices_fact → Iceberg append (no full rewrite) ─
    fact = df[["coin_id","price_usd","market_cap","volume_24h",
               "price_change_24h_pct","event_ts","load_ts"]].copy()
    fact["fact_id"] = [str(uuid.uuid4()) for _ in range(len(fact))]

    # Dedup against already-committed rows on (coin_id, event_ts). At this data
    # scale a full scan-for-read is cheap; the important win vs. the old design
    # is that we only append new rows — no full-table rewrite on every run.
    existing = fact_table.scan().to_pandas()
    if len(existing) > 0:
        existing_keys = set(zip(existing["coin_id"], pd.to_datetime(existing["event_ts"], utc=True)))
        fact = fact[~fact.apply(lambda r: (r["coin_id"], r["event_ts"]) in existing_keys, axis=1)]

    if fact.empty:
        print("No new fact rows to append.")
        return

    fact = fact[["fact_id","coin_id","price_usd","market_cap","volume_24h",
                 "price_change_24h_pct","event_ts","load_ts"]]
    fact_arrow = pa.Table.from_pandas(fact, schema=fact_table.schema().as_arrow(), preserve_index=False)
    fact_table.append(fact_arrow)
    print(f"Fact appended (Iceberg): {len(fact)} new rows")


with DAG(
    dag_id="stg_to_dds",
    description="STG → DDS: SCD2 dim + fact, written as Iceberg tables (pandas, no Spark)",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dds", "iceberg"],
) as dag:
    PythonOperator(
        task_id="stg_to_dds_spark",
        python_callable=process_stg_to_dds,
    )
