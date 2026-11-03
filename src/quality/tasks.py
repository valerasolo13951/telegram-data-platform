"""Задачи качества для Airflow: проверки таблиц Silver + свежих файлов Bronze."""
from __future__ import annotations

import os
from pathlib import Path

from airflow.operators.python import get_current_context


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _load_table(conn, table: str):
    import polars as pl

    arrow = conn.sql(f"SELECT * FROM {table}").arrow()
    return pl.from_arrow(arrow).to_pandas()


def run_quality_task() -> dict:
    """Проверить Bronze (свежие файлы) и таблицы Silver; при FAIL — исключение."""
    import duckdb

    from ingestion.bronze import discover_bronze_files, read_bronze_partition
    from . import suites
    from .runner import FAIL, run_suite

    context = get_current_context()
    run_id = context["run_id"]
    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    bronze_root = _env("BRONZE_PATH", "data/bronze/telegram_messages")
    bronze_lookback = int(_env("TG_QUALITY_BRONZE_LOOKBACK", "5"))

    conn = duckdb.connect(duckdb_file)
    summaries = []
    try:
        # --- Bronze: последние файлы ---
        files = discover_bronze_files(bronze_root)[-bronze_lookback:]
        import polars as pl

        bronze_frames = [read_bronze_files_safe(f) for f in files]
        bronze_frames = [f for f in bronze_frames if f is not None and not f.is_empty()]
        if bronze_frames:
            df = pl.concat(bronze_frames).to_pandas()
            summaries.append(run_suite(conn, df, "bronze_recent", "bronze",
                                       "telegram_messages", suites.bronze_checks(), run_id))

        # --- Silver ---
        set_ids = {r[0] for r in conn.execute("SELECT channel_id FROM channels").fetchall()}
        suites.set_channel_ids(set_ids)

        channels = _load_table(conn, "channels")
        if not channels.empty:
            summaries.append(run_suite(conn, channels, "silver_channels", "silver",
                                       "channels", suites.channels_checks(), run_id))

        messages = _load_table(conn, "messages")
        if not messages.empty:
            summaries.append(run_suite(conn, messages, "silver_messages", "silver",
                                       "messages", suites.messages_checks(), run_id))

        links = _load_table(conn, "message_links")
        if not links.empty:
            summaries.append(run_suite(conn, links, "silver_links", "silver",
                                       "message_links", suites.message_links_checks(), run_id))

        meta = _load_table(conn, "message_meta")
        if not meta.empty:
            summaries.append(run_suite(conn, meta, "silver_meta", "silver",
                                       "message_meta", suites.message_meta_checks(), run_id))
    finally:
        conn.close()

    total_fail = sum(s["fail"] for s in summaries)
    print(f"сводка качества: {summaries}")
    if total_fail:
        raise RuntimeError(
            f"качество не пройдено: {total_fail} критических проверок FAIL; "
            "подробности в таблице quality_results"
        )
    return {"suites": summaries, "fail": total_fail}


def read_bronze_files_safe(path):
    try:
        return read_bronze_partition_file(path)
    except Exception:  # noqa: BLE001
        return None


def read_bronze_partition_file(path):
    from ingestion.bronze import read_bronze_partition

    return read_bronze_partition(Path(path))
