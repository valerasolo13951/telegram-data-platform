"""Построение строк message_meta из результатов inference (чистые функции, тестируемые)."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import polars as pl

from warehouse.keys import content_hash, enrichment_id

from .models import INFERENCE_VERSION, get_task_config


def build_meta_rows(
    task_name: str,
    messages: pl.DataFrame,  # columns: message_key, text
    predictions: list[dict],  # aligned with messages rows
    model_version: str,
    cache_dir_tags: str = "",
) -> pl.DataFrame:
    """Одна строка message_meta на сообщение; пустые тексты → status=skipped."""
    cfg = get_task_config(task_name)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[dict] = []
    for msg, pred in zip(messages.iter_rows(named=True), predictions):
        text = msg["text"]
        input_hash = content_hash(text)
        base = {
            "enrichment_id": enrichment_id(
                msg["message_key"], task_name, cfg["model_name"], cfg["revision"],
                INFERENCE_VERSION, input_hash,
            ),
            "message_key": msg["message_key"],
            "task_name": task_name,
            "model_name": cfg["model_name"],
            "model_version": cfg["revision"],
            "inference_version": INFERENCE_VERSION,
            "input_hash": input_hash,
            "processed_at": now,
            "threshold_version": cfg["threshold_version"],
        }
        if not text or not text.strip():
            # явное правило для пустых текстов — не предсказание
            rows.append({**base, "label": None, "scores_json": None,
                         "confidence": None, "status": "skipped",
                         "error_message": "пустой текст"})
            continue
        rows.append({
            **base,
            "label": pred["label"],
            "scores_json": json.dumps(pred["scores"], ensure_ascii=False, sort_keys=True),
            "confidence": max(pred["scores"].values()) if pred["label"] else None,
            "status": "success",
            "error_message": None,
        })
    if not rows:
        return pl.DataFrame(schema=META_RESULT_SCHEMA)
    return pl.DataFrame(rows)


META_RESULT_SCHEMA = {
    "enrichment_id": pl.String,
    "message_key": pl.String,
    "task_name": pl.String,
    "label": pl.String,
    "scores_json": pl.String,
    "confidence": pl.Float64,
    "model_name": pl.String,
    "model_version": pl.String,
    "inference_version": pl.String,
    "input_hash": pl.String,
    "processed_at": pl.Datetime("us"),
    "status": pl.String,
    "error_message": pl.String,
    "threshold_version": pl.String,
}


def build_failed_rows(task_name: str, messages: pl.DataFrame, error: str) -> pl.DataFrame:
    """Строки status=failed, чтобы сбой был наблюдаем по каждому сообщению."""
    cfg = get_task_config(task_name)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for msg in messages.iter_rows(named=True):
        input_hash = content_hash(msg["text"])
        rows.append({
            "enrichment_id": enrichment_id(
                msg["message_key"], task_name, cfg["model_name"], cfg["revision"],
                INFERENCE_VERSION, input_hash,
            ),
            "message_key": msg["message_key"],
            "task_name": task_name,
            "label": None,
            "scores_json": None,
            "confidence": None,
            "model_name": cfg["model_name"],
            "model_version": cfg["revision"],
            "inference_version": INFERENCE_VERSION,
            "input_hash": input_hash,
            "processed_at": now,
            "status": "failed",
            "error_message": error[:500],
            "threshold_version": cfg["threshold_version"],
        })
    if not rows:
        return pl.DataFrame(schema=META_RESULT_SCHEMA)
    return pl.DataFrame(rows)


def select_messages_for_enrichment(conn, task_name: str, limit: int = 500) -> pl.DataFrame:
    """Сообщения, чей текущий текст не имеет результата для этой задачи+версий.

    Успешный результат с теми же (message_key, model_version,
    inference_version, input_hash) закрывает сообщение; failed/skipped
    и устаревшие по тексту пересчитываются.
    """
    cfg = get_task_config(task_name)
    rows = conn.execute(
        """
        SELECT m.message_key, m.text
        FROM messages m
        LEFT JOIN message_meta mm
          ON mm.message_key = m.message_key
         AND mm.task_name = ?
         AND mm.model_version = ?
         AND mm.inference_version = ?
         AND mm.input_hash = m.content_hash
         AND mm.status = 'success'
        WHERE mm.enrichment_id IS NULL
          AND m.is_deleted = FALSE
        LIMIT ?
        """,
        [task_name, cfg["revision"], INFERENCE_VERSION, limit],
    ).fetchall()
    return pl.DataFrame(
        {"message_key": [r[0] for r in rows], "text": [r[1] for r in rows]},
        schema={"message_key": pl.String, "text": pl.String},
    )
