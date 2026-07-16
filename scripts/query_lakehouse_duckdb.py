"""
Demo: a second query engine (DuckDB) reading the same Iceberg tables
that Airflow/pandas and ClickHouse's marts are built from — the core
lakehouse promise of "open storage, many engines" instead of data
locked inside one database.

Resolves the table's current metadata file via the PyIceberg catalog,
then hands that path straight to DuckDB's iceberg_scan(), bypassing
pandas entirely.

Run inside the Airflow container:

    docker compose exec airflow-scheduler python /opt/airflow/scripts/query_lakehouse_duckdb.py
"""
import sys

sys.path.insert(0, "/opt/airflow/dags")

import duckdb  # noqa: E402
from iceberg_utils import (  # noqa: E402
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    PRICES_FACT_TABLE,
    get_catalog,
)


def main():
    catalog = get_catalog()
    table = catalog.load_table(PRICES_FACT_TABLE)
    metadata_location = table.metadata_location
    print(f"Resolved current Iceberg metadata file: {metadata_location}")

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    endpoint_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    con.execute(f"SET s3_endpoint='{endpoint_host}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")

    print("\n=== DuckDB reading dds.prices_fact directly via iceberg_scan() ===")
    summary = con.execute(f"""
        SELECT
            count(*)               AS total_rows,
            count(DISTINCT coin_id) AS distinct_coins,
            min(event_ts)           AS earliest_event,
            max(event_ts)           AS latest_event
        FROM iceberg_scan('{metadata_location}')
    """).fetchdf()
    print(summary.to_string(index=False))

    print("\n=== Top 5 rows by price_usd (via DuckDB, not pandas/ClickHouse) ===")
    top5 = con.execute(f"""
        SELECT coin_id, price_usd, event_ts
        FROM iceberg_scan('{metadata_location}')
        ORDER BY price_usd DESC
        LIMIT 5
    """).fetchdf()
    print(top5.to_string(index=False))


if __name__ == "__main__":
    main()
