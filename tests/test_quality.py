"""Phase 6 tests: quality runner produces PASS/WARN/FAIL and history."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import great_expectations.expectations as gxe  # noqa: E402

from quality import suites  # noqa: E402
from quality.runner import custom_check, gx_check, run_suite  # noqa: E402


def _channels_df():
    return pd.DataFrame({
        "channel_id": ["a", "b"],
        "username": ["a", "b"],
        "title": ["A", None],
        "description": [None, None],
        "channel_url": ["https://t.me/a", "https://t.me/b"],
        "is_active": [True, True],
    })


def _messages_df():
    return pd.DataFrame({
        "message_key": ["k1", "k2"],
        "channel_id": ["a", "b"],
        "message_id": [1, 2],
        "text": ["x", None],
        "content_hash": ["h1", "h2"],
        "views_count": [5, None],
        "forwards_count": [None, None],
        "replies_count": [0, None],
    })


def test_channels_suite_passes(conn):
    s = run_suite(conn, _channels_df(), "t_channels", "silver",
                  "channels", suites.channels_checks(), "run__test")
    assert s == {"suite": "t_channels", "table": "channels", "pass": 4, "warn": 0, "fail": 0}


def test_messages_suite_detects_duplicates_and_orphans(conn):
    suites.set_channel_ids({"a", "b"})
    df = _messages_df()
    df.loc[1, "message_key"] = "k1"  # дубль ключа
    df.loc[1, "channel_id"] = "zzz"  # сирота
    s = run_suite(conn, df, "t_messages", "silver", "messages",
                  suites.messages_checks(), "run__test")
    assert s["fail"] >= 2  # key_unique + no_orphan_channels
    rows = conn.execute(
        "SELECT check_name, result FROM quality_results WHERE suite_name='t_messages' AND result != 'PASS'"
    ).fetchall()
    names = {r[0] for r in rows}
    assert "messages.key_unique" in names
    assert "messages.no_orphan_channels" in names


def test_warn_does_not_fail(conn):
    # пустых текстов больше 10% — WARN, не FAIL
    suites.set_channel_ids({"a", "b"})
    df = _messages_df()
    df.loc[1, "text"] = None
    s = run_suite(conn, df, "t_warn", "silver", "messages",
                  suites.messages_checks(), "run__test")
    assert s["fail"] == 0
    assert s["warn"] >= 1


def test_meta_scores_range(conn):
    df = pd.DataFrame({
        "enrichment_id": ["e1", "e2"],
        "message_key": ["k1", "k2"],
        "task_name": ["sentiment", "sentiment"],
        "status": ["success", "success"],
        "scores_json": ['{"positive": 0.9}', '{"positive": 1.5}'],  # вне [0,1]
        "model_version": ["r", "r"],
        "inference_version": ["i", "i"],
        "input_hash": ["h", "h"],
    })
    s = run_suite(conn, df, "t_meta", "silver", "message_meta",
                  suites.message_meta_checks(), "run__test")
    assert s["fail"] >= 1
    assert "meta.scores_valid_range" in {
        r[0] for r in conn.execute(
            "SELECT check_name FROM quality_results WHERE suite_name='t_meta' AND result='FAIL'"
        ).fetchall()
    }


def test_history_recorded(conn):
    run_suite(conn, _channels_df(), "t_hist", "silver", "channels",
              suites.channels_checks(), "run__a")
    run_suite(conn, _channels_df(), "t_hist", "silver", "channels",
              suites.channels_checks(), "run__b")
    runs = conn.execute(
        "SELECT run_id, count(*) FROM quality_results WHERE suite_name='t_hist' GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert len(runs) == 2  # история сохраняется между запусками
