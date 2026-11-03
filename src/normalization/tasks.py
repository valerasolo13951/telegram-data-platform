"""Airflow-facing normalization tasks.

DataFrames never travel through XCom: each mapped task normalizes one bronze
file into staging parquet under STAGING_PATH and returns a small dict with the
staging path; a single publisher task loads all staging files, dedups and
publishes to Silver in one DuckDB connection (single-writer rule).
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path

from airflow.operators.python import get_current_context

from warehouse import publisher
from warehouse.schema import init_schema

from . import processed, transform


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def discover_unprocessed_partitions() -> list[str]:
    """Bronze message files not yet in bronze_processed_files."""
    import duckdb

    from ingestion.bronze import discover_bronze_files

    bronze_root = _env("BRONZE_PATH", "data/bronze/telegram_messages")
    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    conn = duckdb.connect(duckdb_file)
    try:
        done = processed.processed_paths(conn)
    finally:
        conn.close()
    return [str(p) for p in discover_bronze_files(bronze_root) if str(p) not in done]


def normalize_partition(file_path: str) -> dict:
    """Normalize one bronze file into staging parquet; return small descriptor."""
    import polars as pl

    from ingestion.bronze import read_bronze_partition

    context = get_current_context()
    run_id = context["run_id"]
    staging_root = Path(_env("STAGING_PATH", "data/staging")) / "normalization" / run_id.replace(":", "_").replace("+", "_")
    staging_root.mkdir(parents=True, exist_ok=True)

    bronze_df = read_bronze_partition(file_path)
    if bronze_df.is_empty():
        return {"file_path": file_path, "messages": 0, "staging": None}

    messages = transform.build_messages(bronze_df)
    links = transform.build_links(bronze_df)
    token = uuid.uuid4().hex[:8]
    messages_path = staging_root / f"messages-{token}.parquet"
    links_path = staging_root / f"links-{token}.parquet"
    messages.write_parquet(messages_path)
    links.write_parquet(links_path)
    return {
        "file_path": file_path,
        "messages": len(messages),
        "links": len(links),
        "staging_messages": str(messages_path),
        "staging_links": str(links_path),
    }


def publish_silver(descriptors: list[dict]) -> dict:
    """Single-writer publication of normalized data + processed-file records."""
    import duckdb
    import polars as pl

    from ingestion.bronze import discover_channel_info_files

    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    bronze_root = _env("BRONZE_PATH", "data/bronze/telegram_messages")
    context = get_current_context()
    run_id = context["run_id"]

    message_frames = [pl.read_parquet(d["staging_messages"]) for d in descriptors if d.get("staging_messages")]
    link_frames = [pl.read_parquet(d["staging_links"]) for d in descriptors if d.get("staging_links")]
    message_frames = [f for f in message_frames if not f.is_empty()]
    link_frames = [f for f in link_frames if not f.is_empty()]

    conn = duckdb.connect(duckdb_file)
    try:
        init_schema(conn)

        channels_df = transform.build_channels(discover_channel_info_files(bronze_root))
        if not channels_df.is_empty():
            publisher.publish_channels(conn, channels_df)

        total_messages = total_links = 0
        if message_frames:
            messages = transform.dedup_messages(pl.concat(message_frames))
            publisher.publish_messages(conn, messages)
            total_messages = len(messages)
        if link_frames:
            links = pl.concat(link_frames).unique(subset=["link_id"], keep="first")
            publisher.publish_message_links(conn, links)
            total_links = len(links)

        processed.record_processed(
            conn,
            [{"file_path": d["file_path"], "run_id": run_id, "messages": d.get("messages", 0)}
             for d in descriptors],
        )
    finally:
        conn.close()

    # cleanup staging
    for d in descriptors:
        for key in ("staging_messages", "staging_links"):
            if d.get(key):
                Path(d[key]).unlink(missing_ok=True)
    return {"files": len(descriptors), "messages": total_messages, "links": total_links}
