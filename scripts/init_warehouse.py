"""Initialize Silver schema in the working DuckDB warehouse.

Usage (inside the airflow image):
    python scripts/init_warehouse.py [--db /opt/airflow/data/warehouse/telegram.duckdb]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import duckdb

from warehouse.schema import SCHEMA_VERSION, init_schema


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db", default=os.environ.get("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    )
    args = parser.parse_args()

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.db)
    try:
        init_schema(conn)
        version = conn.execute(
            "SELECT value FROM warehouse_meta WHERE key = 'schema_version'"
        ).fetchall()
        print(f"warehouse initialized: {args.db}, schema_version={version[0][0]} (expected {SCHEMA_VERSION})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
