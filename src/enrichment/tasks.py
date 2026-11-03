"""Задачи обогащения для Airflow.

Каждая задача inference сама выбирает список работы (свежий запрос), гоняет
локальный батчевый inference вне транзакций БД и пишет результат в staging
parquet; одна задача-издатель загружает staging и публикует message_meta
через одно соединение DuckDB (правило единственного писателя, спецификация §3.5).
"""
from __future__ import annotations

import os
from pathlib import Path

from airflow.operators.python import get_current_context


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def run_inference_task(task_name: str) -> dict:
    """Выбрать сообщения, прогнать одну модель, записать staging-parquet; компактный dict."""
    import duckdb

    from . import adapters, meta
    from .models import get_task_config

    context = get_current_context()
    run_id = context["run_id"].replace(":", "_").replace("+", "_")
    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    staging_root = Path(_env("STAGING_PATH", "data/staging")) / "enrichment" / run_id
    staging_root.mkdir(parents=True, exist_ok=True)
    batch_limit = int(_env("TG_ENRICHMENT_BATCH_LIMIT", "500"))
    batch_size = int(_env("TG_ENRICHMENT_BATCH_SIZE", "16"))

    conn = duckdb.connect(duckdb_file, read_only=True)
    try:
        messages = meta.select_messages_for_enrichment(conn, task_name, limit=batch_limit)
    finally:
        conn.close()
    if messages.is_empty():
        return {"task_name": task_name, "selected": 0, "staging": None}

    cfg = get_task_config(task_name)
    from .meta import build_failed_rows

    try:
        texts = messages["text"].to_list()
        predictions = adapters.predict_batch(task_name, texts, batch_size=batch_size)
        rows = meta.build_meta_rows(task_name, messages, predictions, cfg["revision"])
        status = "ok"
        error = None
    except Exception as exc:  # noqa: BLE001 — ошибка модели не блокирует другие
        error = repr(exc)[:500]
        status = "failed"
        rows = build_failed_rows(task_name, messages, error)
    staging = staging_root / f"meta-{task_name}.parquet"
    rows.write_parquet(staging)
    return {
        "task_name": task_name,
        "selected": len(messages),
        "status": status,
        "error": error,
        "staging": str(staging),
    }


def publish_meta_task(results: list[dict]) -> dict:
    """Публикация message_meta одним писателем из staging-файлов."""
    import duckdb
    import polars as pl

    from warehouse import publisher
    from warehouse.schema import init_schema

    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    conn = duckdb.connect(duckdb_file)
    total = 0
    try:
        init_schema(conn)
        for r in results:
            if not r.get("staging"):
                continue
            rows = pl.read_parquet(r["staging"])
            if not rows.is_empty():
                publisher.publish_message_meta(conn, rows)
                total += len(rows)
    finally:
        conn.close()
    for r in results:
        if r.get("staging"):
            Path(r["staging"]).unlink(missing_ok=True)
    return {"published": total, "tasks": {r["task_name"]: r.get("status") for r in results}}
