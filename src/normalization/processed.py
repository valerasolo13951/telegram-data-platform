"""Учёт обработанных файлов Bronze (безопасные повторные запуски)."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from duckdb import DuckDBPyConnection
import polars as pl

DDL = """
CREATE TABLE IF NOT EXISTS bronze_processed_files (
    file_path    VARCHAR PRIMARY KEY,
    run_id       VARCHAR NOT NULL,
    messages     BIGINT,
    processed_at TIMESTAMP NOT NULL
)
"""


def ensure_table(conn: DuckDBPyConnection) -> None:
    conn.execute(DDL).fetchall()


def processed_paths(conn: DuckDBPyConnection) -> set[str]:
    ensure_table(conn)
    rows = conn.execute("SELECT file_path FROM bronze_processed_files").fetchall()
    return {r[0] for r in rows}


def record_processed(conn: DuckDBPyConnection, entries: list[dict]) -> None:
    if not entries:
        return
    df = pl.DataFrame(
        entries,
        schema={
            "file_path": pl.String,
            "run_id": pl.String,
            "messages": pl.Int64,
            "processed_at": pl.Datetime("us"),
        },
    )
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    staged = df.select("file_path", "run_id", "messages").with_columns(
        pl.lit(now, dtype=pl.Datetime("us")).alias("processed_at")
    )
    conn.register("_stg_processed", staged)
    conn.execute("BEGIN")
    try:
        conn.execute(
            "INSERT OR IGNORE INTO bronze_processed_files SELECT file_path, run_id, messages, processed_at FROM _stg_processed"
        ).fetchall()
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("_stg_processed")
