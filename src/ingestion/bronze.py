"""Bronze layer: append-only Parquet storage of raw collection results.

Layout: <root>/channel_id=<username>/collection_date=YYYY-MM-DD/<name>.parquet
Files are written to a temp name and atomically renamed; published files are
never modified. Provenance columns (collected_at, run_id, schema_version,
collector_version) are stamped here.
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path

import polars as pl

from .collector import BRONZE_POST_SCHEMA, COLLECTOR_VERSION, now_utc

BRONZE_SCHEMA_VERSION = 1


def bronze_partition_path(root: str | Path, username: str, collection_date: str) -> Path:
    return Path(root) / f"channel_id={username}" / f"collection_date={collection_date}"


def write_bronze_partition(
    posts: pl.DataFrame,
    root: str | Path,
    run_id: str,
    collected_at=None,
) -> Path:
    """Write one Bronze partition atomically; returns the published file path."""
    if posts.is_empty():
        raise ValueError("refusing to write empty bronze partition")
    ts = collected_at or now_utc()
    df = posts.with_columns(
        pl.lit(ts).alias("collected_at"),
        pl.lit(run_id).alias("run_id"),
        pl.lit(BRONZE_SCHEMA_VERSION).alias("bronze_schema_version"),
        pl.lit(COLLECTOR_VERSION).alias("collector_version"),
        pl.lit("telegram").alias("source"),
    )
    partition = bronze_partition_path(root, df["username"][0], ts.date().isoformat())
    partition.mkdir(parents=True, exist_ok=True)
    tmp = partition / f".tmp-{uuid.uuid4().hex}.parquet"
    final = partition / f"part-{run_id}-{uuid.uuid4().hex[:8]}.parquet"
    df.write_parquet(tmp)
    os.replace(tmp, final)
    return final


def read_bronze_partition(path: str | Path) -> pl.DataFrame:
    df = pl.read_parquet(path)
    missing = set(BRONZE_POST_SCHEMA) - set(df.columns)
    if missing:
        raise ValueError(f"bronze file {path}: missing columns {sorted(missing)}")
    return df


def write_channel_info_partition(
    info: dict,
    root: str | Path,
    run_id: str,
    collected_at=None,
) -> Path:
    """Persist channel metadata alongside message partitions (append-only)."""
    ts = collected_at or now_utc()
    username = info["username"]
    row = pl.DataFrame(
        [
            {
                "username": username,
                "title": info.get("title"),
                "description": info.get("description"),
                "channel_url": info.get("channel_url"),
                "collected_at": ts,
                "run_id": run_id,
            }
        ]
    )
    partition = Path(root) / f"channel_id={username}" / "channel_info" / f"collection_date={ts.date().isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    tmp = partition / f".tmp-{uuid.uuid4().hex}.parquet"
    final = partition / f"part-{run_id}-{uuid.uuid4().hex[:8]}.parquet"
    row.write_parquet(tmp)
    os.replace(tmp, final)
    return final


def discover_bronze_files(root: str | Path) -> list[Path]:
    """All published bronze parquet files (tmp-files are skipped)."""
    root = Path(root)
    if not root.exists():
        return []
    return sorted(
        p for p in root.rglob("*.parquet")
        if not p.name.startswith(".tmp-") and "channel_info" not in p.parts
    )


def discover_channel_info_files(root: str | Path) -> list[Path]:
    """Latest-first channel metadata files."""
    root = Path(root)
    if not root.exists():
        return []
    return sorted(
        (p for p in root.rglob("*.parquet") if "channel_info" in p.parts),
        reverse=True,
    )
