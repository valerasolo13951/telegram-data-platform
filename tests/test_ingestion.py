"""Phase 3 tests: collector parsing, bronze writer, collection state."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion import bronze, collector, state  # noqa: E402


# ---------- collector ----------

def test_parse_message_id():
    assert collector.parse_message_id("https://t.me/s/telegram/460") == ("telegram", 460)
    assert collector.parse_message_id("https://t.me/s/telegram/abc") is None
    assert collector.parse_message_id("https://t.me/telegram/460") is None


def _fake_posts_df(n, username="testch"):
    return pl.DataFrame(
        {
            "username": [username] * n,
            "message_id": list(range(100, 100 + n)),
            "message_url": [f"https://t.me/s/{username}/{100 + i}" for i in range(n)],
            "message_date": [datetime(2026, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)] * n,
            "text": ["текст"] * n,
            "outlinks": [["https://a.ru"]] * n,
            "preview_url": [None] * n,
            "preview_title": [None] * n,
            "preview_site_name": [None] * n,
        },
        schema=collector.BRONZE_POST_SCHEMA,
    )


# ---------- bronze ----------

def test_bronze_write_is_atomic_and_stamped(tmp_path):
    df = _fake_posts_df(3)
    path = bronze.write_bronze_partition(df, tmp_path, "run__test-1")
    assert "channel_id=testch" in str(path)
    assert "collection_date=" in str(path)
    back = bronze.read_bronze_partition(path)
    assert len(back) == 3
    for col in ("collected_at", "run_id", "bronze_schema_version", "collector_version", "source"):
        assert col in back.columns
    assert back["run_id"][0] == "run__test-1"
    assert back["source"][0] == "telegram"


def test_bronze_refuses_empty(tmp_path):
    with pytest.raises(ValueError):
        bronze.write_bronze_partition(_fake_posts_df(0), tmp_path, "run__x")


def test_bronze_discover_skips_tmp(tmp_path):
    p = bronze.write_bronze_partition(_fake_posts_df(1), tmp_path, "run__a")
    (p.parent / ".tmp-leftover.parquet").touch()
    files = bronze.discover_bronze_files(tmp_path)
    assert files == [p]


# ---------- state ----------

def test_state_upsert_and_get(conn):
    state.ensure_state_table(conn)
    state.upsert_state(conn, [state.success_state("ch1", 55)])
    st = state.get_state(conn, "ch1")
    assert st["last_successful_message_id"] == 55
    assert st["collection_status"] == "ok"

    # ошибка не затирает успешные курсоры (COALESCE)
    state.upsert_state(conn, [state.error_state("ch1", "temporary_error", "timeout")])
    st = state.get_state(conn, "ch1")
    assert st["last_successful_message_id"] == 55
    assert st["collection_status"] == "temporary_error"
    assert "timeout" in st["last_error"]

    # успех после ошибки продвигает курсор и сбрасывает ошибку
    state.upsert_state(conn, [state.success_state("ch1", 60)])
    st = state.get_state(conn, "ch1")
    assert st["last_successful_message_id"] == 60
    assert st["collection_status"] == "ok"
    assert st["last_error"] is None


def test_error_state_validates_status(conn):
    with pytest.raises(ValueError):
        state.error_state("ch1", "bogus", "x")
