"""Phase 5 tests: meta rows, selection logic, interpretation rules."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from enrichment import meta, models  # noqa: E402
from warehouse import keys, publisher  # noqa: E402
from warehouse.schema import init_schema  # noqa: E402


def _messages(texts):
    return pl.DataFrame(
        {
            "message_key": [keys.message_key("telegram", "c", i) for i in range(len(texts))],
            "text": texts,
        },
        schema={"message_key": pl.String, "text": pl.String},
    )


# ---------- registry ----------

def test_registry_has_three_pinned_models():
    assert set(models.MODEL_REGISTRY) == {"toxicity", "sentiment", "emotion"}
    for cfg in models.MODEL_REGISTRY.values():
        assert cfg["revision"], f"{cfg['task_name']} not pinned"
        assert len(cfg["revision"]) == 40  # commit SHA


# ---------- meta rows ----------

def test_build_meta_rows_success_and_skipped():
    msgs = _messages(["какой хороший день!", "   "])
    preds = [
        {"scores": {"positive": 0.9, "neutral": 0.2, "negative": 0.1}, "label": "positive"},
        {"scores": {}, "label": None},
    ]
    rows = meta.build_meta_rows("sentiment", msgs, preds, models.MODEL_REGISTRY["sentiment"]["revision"])
    assert rows["status"].to_list() == ["success", "skipped"]
    assert rows["label"].to_list() == ["positive", None]
    import json
    scores = json.loads(rows["scores_json"][0])
    assert scores["positive"] == 0.9
    # пустой текст — не предсказание
    assert rows["scores_json"][1] is None
    assert rows["error_message"][1] == "empty text"


def test_build_meta_rows_versions_filled():
    msgs = _messages(["текст"])
    preds = [{"scores": {"x": 0.5}, "label": "x"}]
    rows = meta.build_meta_rows("toxicity", msgs, preds, models.MODEL_REGISTRY["toxicity"]["revision"])
    row = rows.row(0, named=True)
    cfg = models.MODEL_REGISTRY["toxicity"]
    assert row["model_version"] == cfg["revision"]
    assert row["inference_version"] == models.INFERENCE_VERSION
    assert row["input_hash"] == keys.content_hash("текст")


def test_build_failed_rows():
    msgs = _messages(["a", "b"])
    rows = meta.build_failed_rows("emotion", msgs, "cuda oom")
    assert rows["status"].to_list() == ["failed", "failed"]
    assert all("oom" in e for e in rows["error_message"])


# ---------- selection ----------

def test_select_messages_skips_done_and_reprocesses_stale(conn):
    import datetime as dtm

    init_schema(conn)
    pub_ts = dtm.datetime(2026, 1, 1)
    msgs = pl.DataFrame(
        {
            "message_key": [keys.message_key("telegram", "c", 1), keys.message_key("telegram", "c", 2)],
            "channel_id": ["c", "c"],
            "message_id": [1, 2],
            "message_date": [None, None],
            "edited_at": [None, None],
            "text": ["текст один", "текст два"],
            "content_hash": [keys.content_hash("текст один"), keys.content_hash("текст два")],
            "message_url": ["u1", "u2"],
            "views_count": [None, None],
            "forwards_count": [None, None],
            "replies_count": [None, None],
            "has_media": [None, None],
            "media_type": [None, None],
            "is_deleted": [False, False],
            "first_collected_at": [pub_ts, pub_ts],
            "last_collected_at": [pub_ts, pub_ts],
            "updated_at": [pub_ts, pub_ts],
        }
    )
    publisher.publish_messages(conn, msgs)
    cfg = models.MODEL_REGISTRY["sentiment"]

    selected = meta.select_messages_for_enrichment(conn, "sentiment")
    assert set(selected["message_key"]) == set(msgs["message_key"])

    # успешный результат для сообщения 1 → больше не выбирается
    preds = [{"scores": {"neutral": 0.9}, "label": "neutral"}]
    done = meta.build_meta_rows("sentiment", msgs[:1], preds, cfg["revision"])
    publisher.publish_message_meta(conn, done)
    selected = meta.select_messages_for_enrichment(conn, "sentiment")
    assert selected["message_key"].to_list() == [msgs["message_key"][1]]

    # изменение текста (content_hash) делает результат устаревшим
    edited = msgs.with_columns(
        pl.when(pl.col("message_id") == 1)
        .then(pl.lit("новый текст"))
        .otherwise(pl.col("text"))
        .alias("text")
    ).with_columns(
        pl.col("text").map_elements(keys.content_hash, return_dtype=pl.String).alias("content_hash")
    )
    publisher.publish_messages(conn, edited)
    selected = meta.select_messages_for_enrichment(conn, "sentiment")
    assert set(selected["message_key"]) == set(msgs["message_key"])
