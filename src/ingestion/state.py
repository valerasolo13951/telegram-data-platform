"""Per-channel collection state stored in DuckDB (spec §3.3).

State is only advanced after the channel's data has been published to Bronze.
"""
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
from duckdb import DuckDBPyConnection

STATE_DDL = """
CREATE TABLE IF NOT EXISTS collection_state (
    channel_id                VARCHAR PRIMARY KEY,
    last_successful_message_id BIGINT,
    last_successful_run_at    TIMESTAMP,
    last_seen_message_id      BIGINT,
    last_reconciliation_at    TIMESTAMP,
    collection_status         VARCHAR NOT NULL DEFAULT 'pending',
    last_error                VARCHAR,
    updated_at                TIMESTAMP NOT NULL
)
"""

VALID_STATUSES = {"pending", "ok", "temporary_error", "channel_unavailable", "failed"}


def ensure_state_table(conn: DuckDBPyConnection) -> None:
    conn.execute(STATE_DDL).fetchall()


def get_state(conn: DuckDBPyConnection, channel_id: str) -> dict | None:
    rows = conn.execute(
        "SELECT channel_id, last_successful_message_id, last_successful_run_at,"
        " last_seen_message_id, last_reconciliation_at, collection_status,"
        " last_error, updated_at FROM collection_state WHERE channel_id = ?",
        [channel_id],
    ).fetchall()
    if not rows:
        return None
    r = rows[0]
    return {
        "channel_id": r[0],
        "last_successful_message_id": r[1],
        "last_successful_run_at": r[2],
        "last_seen_message_id": r[3],
        "last_reconciliation_at": r[4],
        "collection_status": r[5],
        "last_error": r[6],
        "updated_at": r[7],
    }


def upsert_state(conn: DuckDBPyConnection, rows: list[dict]) -> None:
    """Idempotent state update from a list of state dicts (batch, one txn)."""
    if not rows:
        return
    df = pl.DataFrame(
        rows,
        schema={
            "channel_id": pl.String,
            "last_successful_message_id": pl.Int64,
            "last_successful_run_at": pl.Datetime("us"),
            "last_seen_message_id": pl.Int64,
            "last_reconciliation_at": pl.Datetime("us"),
            "collection_status": pl.String,
            "last_error": pl.String,
            "updated_at": pl.Datetime("us"),
        },
        orient="row" if isinstance(rows[0], (list, tuple)) else None,
    )
    conn.register("_stg_state", df)
    conn.execute("BEGIN")
    try:
        conn.execute(
            """
            INSERT INTO collection_state
            SELECT * FROM _stg_state
            ON CONFLICT (channel_id) DO UPDATE SET
                last_successful_message_id = COALESCE(excluded.last_successful_message_id, collection_state.last_successful_message_id),
                last_successful_run_at    = COALESCE(excluded.last_successful_run_at, collection_state.last_successful_run_at),
                last_seen_message_id      = COALESCE(excluded.last_seen_message_id, collection_state.last_seen_message_id),
                last_reconciliation_at    = COALESCE(excluded.last_reconciliation_at, collection_state.last_reconciliation_at),
                collection_status         = excluded.collection_status,
                last_error                = excluded.last_error,
                updated_at                = excluded.updated_at
            """
        ).fetchall()
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("_stg_state")


def success_state(channel_id: str, last_seen_message_id: int) -> dict:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return {
        "channel_id": channel_id,
        "last_successful_message_id": last_seen_message_id,
        "last_successful_run_at": now,
        "last_seen_message_id": last_seen_message_id,
        "last_reconciliation_at": None,
        "collection_status": "ok",
        "last_error": None,
        "updated_at": now,
    }


def error_state(channel_id: str, status: str, error: str) -> dict:
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid status {status!r}")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return {
        "channel_id": channel_id,
        "last_successful_message_id": None,
        "last_successful_run_at": None,
        "last_seen_message_id": None,
        "last_reconciliation_at": None,
        "collection_status": status,
        "last_error": error[:500],
        "updated_at": now,
    }
