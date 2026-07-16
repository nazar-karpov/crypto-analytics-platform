"""
Shared Iceberg catalog/schema helpers for the DDS (silver) layer.

DDS lives as two Iceberg tables in the `dds` namespace, backed by:
  - catalog metadata: Iceberg REST catalog (iceberg-rest container)
  - data + table metadata files: MinIO bucket `lakehouse`

A REST catalog is used (rather than PyIceberg's SQLAlchemy-based SQL
catalog) so the Airflow image never needs SQLAlchemy 2.x — PyIceberg's
sql-postgres/sql-sqlite extras pull in SQLAlchemy 2.x, which conflicts
with Airflow 2.8 / Flask-AppBuilder's hard requirement of SQLAlchemy <2.0.
The REST catalog client only talks HTTP.

All functions here are meant to be called lazily from inside Airflow
task callables (not at DAG-parse time), so a not-yet-ready catalog
container can't crash the scheduler's DAG parsing.
"""
from __future__ import annotations

import os

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    NestedField,
    StringType,
    TimestamptzType,
)

ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://lakehouse/")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")

DDS_NAMESPACE = "dds"
COINS_DIM_TABLE = f"{DDS_NAMESPACE}.coins_dim"
PRICES_FACT_TABLE = f"{DDS_NAMESPACE}.prices_fact"

COINS_DIM_SCHEMA = Schema(
    NestedField(1, "coin_id", StringType(), required=True),
    NestedField(2, "symbol", StringType(), required=False),
    NestedField(3, "name", StringType(), required=False),
    NestedField(4, "valid_from", TimestamptzType(), required=False),
    NestedField(5, "valid_to", TimestamptzType(), required=False),
    NestedField(6, "is_current", BooleanType(), required=False),
)

PRICES_FACT_SCHEMA = Schema(
    NestedField(1, "fact_id", StringType(), required=True),
    NestedField(2, "coin_id", StringType(), required=True),
    NestedField(3, "price_usd", DoubleType(), required=False),
    NestedField(4, "market_cap", DoubleType(), required=False),
    NestedField(5, "volume_24h", DoubleType(), required=False),
    NestedField(6, "price_change_24h_pct", DoubleType(), required=False),
    NestedField(7, "event_ts", TimestamptzType(), required=True),
    NestedField(8, "load_ts", TimestamptzType(), required=False),
)

# Hidden partitioning by month(event_ts) — Iceberg tracks this itself,
# no manual year/month columns needed in the data like the old raw-Parquet layout.
PRICES_FACT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=7, field_id=1000, transform=MonthTransform(), name="event_month"
    )
)


def get_catalog() -> Catalog:
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "dds_catalog",
        uri=ICEBERG_REST_URI,
        warehouse=ICEBERG_WAREHOUSE,
        **{
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": "us-east-1",
        },
    )


def ensure_tables(catalog: Catalog) -> None:
    # create_table_if_not_exists is idempotent and avoids the REST catalog's
    # table_exists (HEAD) probe, which tabulario/iceberg-rest 1.6.0 answers
    # with a 400 instead of a clean 404.
    catalog.create_namespace_if_not_exists(DDS_NAMESPACE)
    catalog.create_table_if_not_exists(COINS_DIM_TABLE, schema=COINS_DIM_SCHEMA)
    catalog.create_table_if_not_exists(
        PRICES_FACT_TABLE,
        schema=PRICES_FACT_SCHEMA,
        partition_spec=PRICES_FACT_PARTITION_SPEC,
    )
