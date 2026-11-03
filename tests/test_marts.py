"""Тесты Phase 7: гранулярность и агрегаты витрин на синтетических данных Silver."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from marts import builder  # noqa: E402
from warehouse import keys, publisher  # noqa: E402
from warehouse.schema import init_schema  # noqa: E402

TS = datetime(2026, 1, 2, 10, 0)


@pytest.fixture()
def silver(conn):
    init_schema(conn)
    conn.execute("DELETE FROM channels").fetchall()
    conn.execute("DELETE FROM messages").fetchall()
    conn.execute("DELETE FROM message_links").fetchall()
    conn.execute("DELETE FROM message_meta").fetchall()

    publisher.publish_channels(conn, pl.DataFrame([dict(
        channel_id="ch1", username="ch1", title="C1", description=None,
        channel_url="https://t.me/ch1", is_active=True,
        first_seen_at=TS, last_seen_at=TS, last_message_at=TS,
        created_at=TS, updated_at=TS,
    )]))

    rows = []
    for i, (mid, text, day) in enumerate([
        (1, "Отличная новость https://t.me/other/5", 2),
        (2, "Какой ужас, идиоты!", 2),
        (3, None, 3),  # медиапост без текста
    ]):
        rows.append(dict(
            message_key=keys.message_key("telegram", "ch1", mid),
            channel_id="ch1", message_id=mid,
            message_date=datetime(2026, 1, day, 12, 0),
            edited_at=None, text=text, content_hash=keys.content_hash(text),
            message_url=f"https://t.me/s/ch1/{mid}",
            views_count=None, forwards_count=None, replies_count=None,
            has_media=(mid == 3), media_type=None, is_deleted=False,
            first_collected_at=TS, last_collected_at=TS, updated_at=TS,
        ))
    publisher.publish_messages(conn, pl.DataFrame(rows))

    mk1 = rows[0]["message_key"]
    publisher.publish_message_links(conn, pl.DataFrame([dict(
        link_id=keys.link_id(mk1, "https://t.me/other/5", 16),
        message_key=mk1, raw_url="https://t.me/other/5",
        normalized_url="https://t.me/other/5", domain="t.me",
        link_type="telegram", position_start=16, position_end=35,
        extraction_version="links-v1", extracted_at=TS,
    )]))

    meta_rows = []
    for task, label, scores, mk in [
        ("sentiment", "positive", {"positive": 0.9, "neutral": 0.1, "negative": 0.0}, mk1),
        ("sentiment", "negative", {"positive": 0.0, "neutral": 0.2, "negative": 0.8}, rows[1]["message_key"]),
        ("toxicity", "non-toxic", {"non-toxic": 0.9, "insult": 0.05, "obscenity": 0.0, "threat": 0.0, "dangerous": 0.0}, mk1),
        ("toxicity", "insult", {"non-toxic": 0.1, "insult": 0.9, "obscenity": 0.1, "threat": 0.0, "dangerous": 0.1}, rows[1]["message_key"]),
        ("emotion", "joy", {"no_emotion": 0.2, "joy": 0.8, "sadness": 0.1, "surprise": 0.1, "fear": 0.0, "anger": 0.0}, mk1),
        ("emotion", None, {"no_emotion": 0.9, "joy": 0.05, "sadness": 0.1, "surprise": 0.05, "fear": 0.0, "anger": 0.0}, rows[1]["message_key"]),
    ]:
        input_hash = keys.content_hash("x")
        meta_rows.append(dict(
            enrichment_id=keys.enrichment_id(mk, task, "m", "rev1", "inf-v1", input_hash),
            message_key=mk, task_name=task, label=label,
            scores_json=json.dumps(scores), confidence=max(scores.values()),
            model_name="m", model_version="rev1", inference_version="inf-v1",
            input_hash=input_hash, processed_at=TS, status="success",
            error_message=None, threshold_version="th-v1",
        ))
    publisher.publish_message_meta(conn, pl.DataFrame(meta_rows))
    return conn


def _build(silver):
    builder.build_marts(silver)
    return silver


def test_activity_daily_grain_and_counts(silver):
    conn = _build(silver)
    rows = conn.sql("SELECT * FROM mart_channel_activity_daily ORDER BY activity_date").arrow()
    df = pl.from_arrow(rows)
    assert df.height == 2  # два дня
    assert df["messages_count"].to_list() == [2, 1]
    assert df["messages_with_links_count"].to_list() == [1, 0]
    assert df["avg_text_length"].to_list()[1] == 0  # пустой текст медиапоста
    # гранулярность канал+день уникальна
    assert df.group_by(["activity_date", "channel_id"]).len().height == 2


def test_sentiment_daily(silver):
    conn = _build(silver)
    df = pl.from_arrow(conn.sql("SELECT * FROM mart_message_sentiment_daily").arrow())
    assert df.height == 1  # обе sentiment-записи в один день
    row = df.row(0, named=True)
    assert row["messages_processed"] == 2
    assert row["positive_count"] == 1 and row["negative_count"] == 1
    assert row["positive_share"] == 0.5
    assert row["avg_sentiment_score"] == pytest.approx(0.45)


def test_toxicity_daily_multilabel(silver):
    conn = _build(silver)
    df = pl.from_arrow(conn.sql("SELECT * FROM mart_message_toxicity_daily").arrow())
    row = df.row(0, named=True)
    assert row["messages_processed"] == 2
    # сумма категорий != числу сообщений (multilabel semantics)
    assert row["insult_count"] == 1
    assert row["non_toxic_count"] == 1
    assert row["avg_insult_score"] == pytest.approx(0.475)


def test_emotions_daily(silver):
    conn = _build(silver)
    df = pl.from_arrow(conn.sql("SELECT * FROM mart_message_emotions_daily").arrow())
    row = df.row(0, named=True)
    assert row["joy_count"] == 1
    assert row["no_emotion_count"] == 1
    assert row["messages_processed"] == 2


def test_domain_mentions(silver):
    conn = _build(silver)
    df = pl.from_arrow(conn.sql("SELECT * FROM mart_domain_mentions_daily").arrow())
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["domain"] == "t.me"
    assert row["telegram_links_count"] == 1
    assert row["unique_messages_count"] == 1


def test_channel_link_edges(silver):
    conn = _build(silver)
    df = pl.from_arrow(conn.sql("SELECT * FROM mart_channel_link_edges").arrow())
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["source_channel_id"] == "ch1"
    assert row["target_channel_id"] == "other"
    assert row["link_count"] == 1


def test_content_daily_coverage(silver):
    conn = _build(silver)
    df = pl.from_arrow(
        conn.sql("SELECT * FROM mart_channel_content_daily ORDER BY activity_date").arrow()
    )
    cov = df["enrichment_coverage"].to_list()
    assert cov == [1.0, 0.0]  # день 2: оба обогащены; день 3: медиапост без текста skipped


def test_all_marts_have_expected_columns(silver):
    conn = _build(silver)
    for mart, cols in builder.MART_COLUMNS.items():
        if mart == "mart_pipeline_health_daily":
            continue  # строится из метаданных Airflow, проверяется живым прогоном
        got = [r[0] for r in conn.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name='{mart}' ORDER BY column_name"
        ).fetchall()]
        missing = set(cols) - set(got)
        assert not missing, f"{mart}: missing {missing}"
