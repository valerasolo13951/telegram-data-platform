"""Controlled publication of Silver tables.

Each publish loads a polars DataFrame into a staging relation and upserts it
inside one transaction. All functions are idempotent: re-publishing the same
data does not create duplicates.
"""
from __future__ import annotations

import polars as pl
from duckdb import DuckDBPyConnection

MESSAGE_COLUMNS = [
    "message_key", "channel_id", "message_id", "message_date", "edited_at",
    "text", "content_hash", "message_url", "views_count", "forwards_count",
    "replies_count", "has_media", "media_type", "is_deleted",
    "first_collected_at", "last_collected_at", "updated_at",
]
CHANNEL_COLUMNS = [
    "channel_id", "username", "title", "description", "channel_url",
    "is_active", "first_seen_at", "last_seen_at", "last_message_at",
    "created_at", "updated_at",
]
LINK_COLUMNS = [
    "link_id", "message_key", "raw_url", "normalized_url", "domain",
    "link_type", "position_start", "position_end", "extraction_version",
    "extracted_at",
]
META_COLUMNS = [
    "enrichment_id", "message_key", "task_name", "label", "scores_json",
    "confidence", "model_name", "model_version", "inference_version",
    "input_hash", "processed_at", "status", "error_message",
    "threshold_version",
]


def _check_columns(df: pl.DataFrame, expected: list[str], name: str) -> None:
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(f"{name}: missing columns {sorted(missing)}")


def _to_naive_utc(df: pl.DataFrame) -> pl.DataFrame:
    """Store all timestamps as naive UTC regardless of input timezone."""
    for name, dtype in df.schema.items():
        if dtype == pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col(name).dt.replace_time_zone(None))
        elif isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
            df = df.with_columns(
                pl.col(name).dt.convert_time_zone("UTC").dt.replace_time_zone(None)
            )
    return df


def _ordered(df: pl.DataFrame, expected: list[str]) -> pl.DataFrame:
    return df.select(expected)


def publish_channels(conn: DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Upsert channels; first_seen_at/created_at of the existing row win."""
    _check_columns(df, CHANNEL_COLUMNS, "channels")
    df = _to_naive_utc(_ordered(df, CHANNEL_COLUMNS))
    conn.register("_stg_channels", df)
    conn.execute("BEGIN")
    try:
        conn.execute(
            """
            INSERT INTO channels
            SELECT * FROM _stg_channels
            ON CONFLICT (channel_id) DO UPDATE SET
                username        = excluded.username,
                title           = excluded.title,
                description     = excluded.description,
                channel_url     = excluded.channel_url,
                is_active       = excluded.is_active,
                last_seen_at    = excluded.last_seen_at,
                last_message_at = COALESCE(excluded.last_message_at, channels.last_message_at),
                updated_at      = excluded.updated_at
            """
        ).fetchall()
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("_stg_channels")
    return len(df)


def publish_messages(conn: DuckDBPyConnection, df: pl.DataFrame) -> dict:
    """Upsert messages; first_collected_at of the existing row is preserved."""
    _check_columns(df, MESSAGE_COLUMNS, "messages")
    df = _to_naive_utc(_ordered(df, MESSAGE_COLUMNS))
    conn.register("_stg_messages", df)
    conn.execute("BEGIN")
    try:
        conn.execute(
            """
            INSERT INTO messages
            SELECT * FROM _stg_messages
            ON CONFLICT (message_key) DO UPDATE SET
                message_date      = excluded.message_date,
                edited_at         = excluded.edited_at,
                text              = excluded.text,
                content_hash      = excluded.content_hash,
                message_url       = excluded.message_url,
                views_count       = excluded.views_count,
                forwards_count    = excluded.forwards_count,
                replies_count     = excluded.replies_count,
                has_media         = excluded.has_media,
                media_type        = excluded.media_type,
                is_deleted        = excluded.is_deleted,
                last_collected_at = excluded.last_collected_at,
                updated_at        = excluded.updated_at
            """
        ).fetchall()
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("_stg_messages")
    return {"rows_received": len(df)}


def publish_message_links(conn: DuckDBPyConnection, df: pl.DataFrame) -> dict:
    """Replace links of the affected messages (re-extraction is the norm)."""
    _check_columns(df, LINK_COLUMNS, "message_links")
    df = _to_naive_utc(_ordered(df, LINK_COLUMNS))
    conn.register("_stg_links", df)
    conn.execute("BEGIN")
    try:
        conn.execute(
            "DELETE FROM message_links WHERE message_key IN "
            "(SELECT DISTINCT message_key FROM _stg_links)"
        ).fetchall()
        conn.execute("INSERT INTO message_links SELECT * FROM _stg_links").fetchall()
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("_stg_links")
    return {"rows_received": len(df)}


def publish_message_meta(conn: DuckDBPyConnection, df: pl.DataFrame) -> dict:
    """Insert enrichment results; a repeated identical result is a no-op."""
    _check_columns(df, META_COLUMNS, "message_meta")
    df = _to_naive_utc(_ordered(df, META_COLUMNS))
    conn.register("_stg_meta", df)
    conn.execute("BEGIN")
    try:
        conn.execute(
            """
            INSERT INTO message_meta
            SELECT * FROM _stg_meta
            ON CONFLICT (enrichment_id) DO UPDATE SET
                label             = excluded.label,
                scores_json       = excluded.scores_json,
                confidence        = excluded.confidence,
                processed_at      = excluded.processed_at,
                status            = excluded.status,
                error_message     = excluded.error_message,
                threshold_version = excluded.threshold_version
            """
        ).fetchall()
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("_stg_meta")
    return {"rows_received": len(df)}
