"""Phase 4 tests: link extraction/classification, bronze→silver transform."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from normalization import transform  # noqa: E402
from normalization.links import classify_url, extract_links, normalize_url  # noqa: E402
from warehouse import keys  # noqa: E402


# ---------- links ----------

def test_extract_positions_are_code_points():
    text = "Смотри https://example.com/a?x=1 и ещё раз https://example.com/a?x=1"
    links = extract_links(text)
    assert len(links) == 2  # повторные вхождения сохраняются
    for link in links:
        assert text[link.position_start:link.position_end] == link.raw_url


def test_extract_handles_cyrillic_and_trailing_punctuation():
    text = "Новость: https://Example.COM/Path (подробности)."
    links = extract_links(text)
    assert links[0].raw_url == "https://Example.COM/Path"
    assert links[0].normalized_url == "https://example.com/Path"
    assert links[0].domain == "example.com"
    assert links[0].link_type == "external"


def test_classify_url_types():
    assert classify_url("https://t.me/durov")[1] == "telegram"
    assert classify_url("https://t.me/durov/123")[1] == "telegram"
    assert classify_url("https://t.me/+AbCdEf")[1] == "invite"
    assert classify_url("https://t.me/joinchat/xyz")[1] == "invite"
    assert classify_url("https://example.com/x")[1] == "external"


def test_normalize_keeps_query_params():
    assert normalize_url("HTTPS://EXAMPLE.com/p?utm=1&b=2") == "https://example.com/p?utm=1&b=2"


def test_extract_empty_text():
    assert extract_links(None) == []
    assert extract_links("") == []


# ---------- transform ----------

def _bronze_df(rows):
    base_schema = {
        "username": pl.String,
        "message_id": pl.Int64,
        "message_url": pl.String,
        "message_date": pl.Datetime("us"),
        "text": pl.String,
        "outlinks": pl.List(pl.String),
        "preview_url": pl.String,
        "preview_title": pl.String,
        "preview_site_name": pl.String,
        "collected_at": pl.Datetime("us"),
        "run_id": pl.String,
        "bronze_schema_version": pl.Int64,
        "collector_version": pl.String,
        "source": pl.String,
    }
    return pl.DataFrame(rows, schema=base_schema)


def _row(message_id, text, collected_at, username="ch1"):
    return {
        "username": username,
        "message_id": message_id,
        "message_url": f"https://t.me/s/{username}/{message_id}",
        "message_date": datetime(2026, 1, 2, 10, 0),
        "text": text,
        "outlinks": [],
        "preview_url": None,
        "preview_title": None,
        "preview_site_name": None,
        "collected_at": collected_at,
        "run_id": "r1",
        "bronze_schema_version": 1,
        "collector_version": "v1",
        "source": "telegram",
    }


def test_dedup_latest_collected_wins():
    df = _bronze_df(
        [
            _row(1, "старая версия", datetime(2026, 1, 1)),
            _row(1, "новая версия", datetime(2026, 1, 2)),
            _row(2, "другое", datetime(2026, 1, 1)),
        ]
    )
    from ingestion.collector import BRONZE_POST_SCHEMA
    # dedup работает по message_key, который строится в build_messages
    messages = transform.build_messages(df)
    deduped = transform.dedup_messages(messages)
    assert len(deduped) == 2
    edited = deduped.filter(pl.col("message_id") == 1)
    assert edited["text"][0] == "новая версия"
    assert edited["content_hash"][0] == keys.content_hash("новая версия")


def test_build_messages_schema():
    df = _bronze_df([_row(5, "текст", datetime(2026, 1, 1))])
    messages = transform.build_messages(df)
    from warehouse.publisher import MESSAGE_COLUMNS
    assert set(messages.columns) == set(MESSAGE_COLUMNS)
    row = messages.row(0, named=True)
    assert row["message_key"] == keys.message_key("telegram", "ch1", 5)
    assert row["channel_id"] == "ch1"
    assert row["views_count"] is None  # веб-источник не даёт просмотров
    assert row["has_media"] is None


def test_build_links_from_text():
    text = "Читай https://t.me/other/10 и https://example.com/x"
    df = _bronze_df([_row(7, text, datetime(2026, 1, 1))])
    links = transform.build_links(df)
    assert len(links) == 2
    types = dict(zip(links["raw_url"], links["link_type"]))
    assert types["https://t.me/other/10"] == "telegram"
    assert types["https://example.com/x"] == "external"
    mk = keys.message_key("telegram", "ch1", 7)
    assert set(links["message_key"]) == {mk}
    # позиции согласованы с исходным текстом
    for r in links.iter_rows(named=True):
        assert text[r["position_start"]:r["position_end"]] == r["raw_url"]


def test_build_links_empty_text():
    df = _bronze_df([_row(8, None, datetime(2026, 1, 1))])
    assert transform.build_links(df).is_empty()
