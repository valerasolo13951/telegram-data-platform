"""Phase 2 tests: schema, keys, idempotent publication, message updates."""
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from warehouse import keys, publisher

TS1 = datetime(2026, 1, 3, 0, 1, tzinfo=timezone.utc)
TS2 = datetime(2026, 1, 4, 0, 0, tzinfo=timezone.utc)


def _channel_row(**over):
    base = dict(
        channel_id="123",
        username="example",
        title="Example",
        description=None,
        channel_url="https://t.me/example",
        is_active=True,
        first_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        last_message_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    base.update(over)
    return base


def _message_row(channel_id="123", message_id=100, text="привет мир", **over):
    mk = keys.message_key("telegram", channel_id, message_id)
    base = dict(
        message_key=mk,
        channel_id=channel_id,
        message_id=message_id,
        message_date=datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc),
        edited_at=None,
        text=text,
        content_hash=keys.content_hash(text),
        message_url=f"https://t.me/example/{message_id}",
        views_count=10,
        forwards_count=None,
        replies_count=None,
        has_media=False,
        media_type=None,
        is_deleted=False,
        first_collected_at=TS1,
        last_collected_at=TS1,
        updated_at=TS1,
    )
    base.update(over)
    return base


# ---------- keys ----------

def test_message_key_is_deterministic_and_channel_scoped():
    a = keys.message_key("telegram", "1", 5)
    assert a == keys.message_key("telegram", "1", 5)
    assert a != keys.message_key("telegram", "2", 5)
    assert a != keys.message_key("telegram", "1", 6)


def test_content_hash_normalizes_whitespace():
    assert keys.content_hash("текст") == keys.content_hash("  текст  \n")
    assert keys.content_hash(None) == keys.content_hash("")


# ---------- channels ----------

def test_channels_upsert_preserves_first_seen(conn):
    publisher.publish_channels(conn, pl.DataFrame([_channel_row()]))
    publisher.publish_channels(
        conn,
        pl.DataFrame(
            [_channel_row(title="New title", updated_at=datetime(2026, 2, 1, tzinfo=timezone.utc))]
        ),
    )
    row = conn.execute("SELECT title, first_seen_at, updated_at FROM channels").fetchall()
    assert row[0][0] == "New title"
    assert row[0][1] == datetime(2026, 1, 1)  # preserved
    assert row[0][2] == datetime(2026, 2, 1)
    assert conn.execute("SELECT count(*) FROM channels").fetchall()[0][0] == 1


# ---------- messages: idempotent reload ----------

def test_message_reload_does_not_duplicate(conn):
    df = pl.DataFrame([_message_row()])
    publisher.publish_messages(conn, df)
    publisher.publish_messages(conn, df)  # повторная загрузка того же сообщения
    n = conn.execute(
        "SELECT count(*) FROM messages WHERE message_key = ?", [df["message_key"][0]]
    ).fetchall()[0][0]
    assert n == 1


def test_first_collected_at_preserved_on_reload(conn):
    df = pl.DataFrame([_message_row()])
    publisher.publish_messages(conn, df)
    later = _message_row(last_collected_at=TS2, updated_at=TS2)
    publisher.publish_messages(conn, pl.DataFrame([later]))
    row = conn.execute("SELECT first_collected_at, last_collected_at FROM messages").fetchall()
    assert row[0][0] == TS1.replace(tzinfo=None)
    assert row[0][1] == TS2.replace(tzinfo=None)


def test_edited_message_updates_text_and_hash(conn):
    publisher.publish_messages(conn, pl.DataFrame([_message_row(text="старый текст")]))
    new_text = "новый отредактированный текст"
    publisher.publish_messages(
        conn,
        pl.DataFrame(
            [_message_row(text=new_text, edited_at=datetime(2026, 1, 5, tzinfo=timezone.utc))]
        ),
    )
    row = conn.execute("SELECT text, content_hash, edited_at FROM messages").fetchall()
    assert row[0][0] == new_text
    assert row[0][1] == keys.content_hash(new_text)
    assert conn.execute("SELECT count(*) FROM messages").fetchall()[0][0] == 1


def test_same_text_in_two_messages_not_merged(conn):
    """Повторная публикация одинакового текста — не дубль."""
    publisher.publish_messages(
        conn,
        pl.DataFrame([_message_row(message_id=1), _message_row(message_id=2)]),
    )
    assert conn.execute("SELECT count(*) FROM messages").fetchall()[0][0] == 2


def test_unknown_channel_reference_detected(conn):
    publisher.publish_messages(conn, pl.DataFrame([_message_row(channel_id="999")]))
    orphans = conn.execute(
        "SELECT count(*) FROM messages m "
        "LEFT JOIN channels c USING (channel_id) WHERE c.channel_id IS NULL"
    ).fetchall()[0][0]
    assert orphans == 1


# ---------- message_links ----------

def _link_rows(mk, urls):
    return pl.DataFrame(
        [
            dict(
                link_id=keys.link_id(mk, u, i * 10),
                message_key=mk,
                raw_url=u,
                normalized_url=u.lower(),
                domain=u.split("/")[2],
                link_type="external",
                position_start=i * 10,
                position_end=i * 10 + len(u),
                extraction_version="v1",
                extracted_at=TS1,
            )
            for i, u in enumerate(urls)
        ]
    )


def test_links_replaced_on_reextraction(conn):
    df = pl.DataFrame([_message_row()])
    publisher.publish_messages(conn, df)
    mk = df["message_key"][0]
    publisher.publish_message_links(conn, _link_rows(mk, ["https://a.ru/x", "https://a.ru/x"]))
    # то же вхождение дважды сохраняется, как и повторные ссылки
    assert conn.execute("SELECT count(*) FROM message_links").fetchall()[0][0] == 2
    # повторное извлечение: одна ссылка изменилась
    publisher.publish_message_links(conn, _link_rows(mk, ["https://b.ru/y"]))
    assert conn.execute("SELECT count(*) FROM message_links").fetchall()[0][0] == 1
    assert conn.execute("SELECT raw_url FROM message_links").fetchall()[0][0] == "https://b.ru/y"


# ---------- message_meta ----------

def _meta_row(mk, text="текст", status="success", **over):
    base = dict(
        enrichment_id=keys.enrichment_id(mk, "sentiment", "m", "rev1", "inf1", keys.content_hash(text)),
        message_key=mk,
        task_name="sentiment",
        label="neutral",
        scores_json='{"negative": 0.1, "neutral": 0.8, "positive": 0.1}',
        confidence=0.8,
        model_name="m",
        model_version="rev1",
        inference_version="inf1",
        input_hash=keys.content_hash(text),
        processed_at=datetime(2026, 1, 3, 0, 5, tzinfo=timezone.utc),
        status=status,
        error_message=None,
        threshold_version="t1",
    )
    base.update(over)
    return base


def test_meta_repeated_publish_no_duplicate(conn):
    df = pl.DataFrame([_message_row()])
    publisher.publish_messages(conn, df)
    row = _meta_row(df["message_key"][0])
    publisher.publish_message_meta(conn, pl.DataFrame([row]))
    publisher.publish_message_meta(conn, pl.DataFrame([row]))
    assert conn.execute("SELECT count(*) FROM message_meta").fetchall()[0][0] == 1


def test_meta_failed_status_kept(conn):
    df = pl.DataFrame([_message_row()])
    publisher.publish_messages(conn, df)
    mk = df["message_key"][0]
    failed = _meta_row(
        mk,
        status="failed",
        label=None,
        scores_json=None,
        confidence=None,
        error_message="model error",
    )
    publisher.publish_message_meta(conn, pl.DataFrame([failed]))
    row = conn.execute("SELECT status, error_message FROM message_meta").fetchall()
    assert row[0] == ("failed", "model error")


def test_meta_new_input_hash_creates_new_result(conn):
    """Изменение текста (input_hash) не перезаписывает старый результат."""
    df = pl.DataFrame([_message_row(text="версия 1")])
    publisher.publish_messages(conn, df)
    mk = df["message_key"][0]
    publisher.publish_message_meta(conn, pl.DataFrame([_meta_row(mk, text="версия 1")]))
    publisher.publish_message_meta(conn, pl.DataFrame([_meta_row(mk, text="версия 2")]))
    assert conn.execute("SELECT count(*) FROM message_meta").fetchall()[0][0] == 2
