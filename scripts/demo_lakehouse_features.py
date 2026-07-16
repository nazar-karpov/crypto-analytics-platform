"""
Demo: ACID snapshots, time travel, and schema evolution on the DDS
Iceberg tables — the properties that make this a lakehouse rather
than "just Parquet files in a bucket".

Run inside the Airflow container, where iceberg_utils.py and the
pyiceberg/duckdb deps are installed:

    docker compose exec airflow-scheduler python /opt/airflow/scripts/demo_lakehouse_features.py

Trigger the `stg_to_dds` DAG at least twice (5 min apart) beforehand
so `dds.prices_fact` has more than one snapshot to time-travel between.
"""
import sys

sys.path.insert(0, "/opt/airflow/dags")

from iceberg_utils import PRICES_FACT_TABLE, get_catalog  # noqa: E402
from pyiceberg.types import StringType  # noqa: E402


def show_snapshot_history(table):
    print("\n=== Snapshot history (dds.prices_fact) ===")
    snapshots = list(table.history())
    if not snapshots:
        print("No snapshots yet — run the stg_to_dds DAG first.")
        return snapshots
    for i, entry in enumerate(snapshots):
        print(f"  [{i}] snapshot_id={entry.snapshot_id}  timestamp_ms={entry.timestamp_ms}")
    return snapshots


def show_time_travel(table, snapshots):
    print("\n=== Time travel ===")
    if len(snapshots) < 2:
        print("Only one snapshot so far — trigger stg_to_dds again, then re-run this "
              "script to compare row counts between two points in time.")
        return

    oldest_id = snapshots[0].snapshot_id
    newest_id = snapshots[-1].snapshot_id

    old_rows = len(table.scan(snapshot_id=oldest_id).to_pandas())
    new_rows = len(table.scan(snapshot_id=newest_id).to_pandas())

    print(f"  rows at oldest snapshot ({oldest_id}): {old_rows}")
    print(f"  rows at newest snapshot ({newest_id}): {new_rows}")
    print("  -> same table, two points in time, no manual backups required.")


def show_schema_evolution(table):
    print("\n=== Schema evolution ===")
    print("  schema before:")
    print(f"  {table.schema()}")

    if "ingestion_note" not in table.schema().column_names:
        with table.update_schema() as update:
            update.add_column("ingestion_note", field_type=StringType(), required=False)

    print("  schema after adding 'ingestion_note' (nullable, no rewrite of existing data files):")
    print(f"  {table.schema()}")


def main():
    catalog = get_catalog()
    table = catalog.load_table(PRICES_FACT_TABLE)

    snapshots = show_snapshot_history(table)
    show_time_travel(table, snapshots)
    show_schema_evolution(table)


if __name__ == "__main__":
    main()
