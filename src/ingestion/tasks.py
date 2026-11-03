"""Задачи сбора для Airflow (тонкие, XCom-friendly).

collect_channel_task возвращает маленький словарь (статус/счётчики/путь) —
никогда DataFrame. Состояние пишет одна консолидированная задача, чтобы
сохранить правило единственного писателя DuckDB (спецификация, §3.5).
"""
from __future__ import annotations

import os
from pathlib import Path

import polars as pl
from airflow.operators.python import get_current_context

from . import bronze, collector, state


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def collect_channel_task(username: str, max_messages: int | None, overlap_size: int) -> dict:
    """Собрать один канал в Bronze. Ошибки возвращаются, а не бросаются:
    ошибка одного канала не должна останавливать остальные (критерии успеха)."""
    context = get_current_context()
    run_id = context["run_id"].replace(":", "_").replace("+", "_")
    bronze_root = _env("BRONZE_PATH", "data/bronze/telegram_messages")
    timeout = int(_env("TG_REQUEST_TIMEOUT", "30"))

    try:
        info = collector.fetch_channel_info(username, timeout=timeout)
        bronze.write_channel_info_partition(info, bronze_root, run_id)
        posts = collector.collect_channel(
            username,
            max_messages=max_messages,
            timeout=timeout,
        )
        if posts.is_empty():
            return {
                "username": username,
                "status": "ok",
                "title": info.get("title"),
                "collected": 0,
                "last_seen_message_id": None,
                "bronze_file": None,
            }
        path = bronze.write_bronze_partition(posts, bronze_root, run_id)
        last_seen = int(posts["message_id"].max())
        return {
            "username": username,
            "status": "ok",
            "title": info.get("title"),
            "collected": len(posts),
            "last_seen_message_id": last_seen,
            "bronze_file": str(path),
        }
    except collector.ChannelUnavailable as exc:
        return {"username": username, "status": "channel_unavailable", "error": str(exc)}
    except collector.TemporaryCollectError as exc:
        return {"username": username, "status": "temporary_error", "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - isolation per channel
        return {"username": username, "status": "failed", "error": repr(exc)[:500]}


def collect_channel_incremental_task(username: str, overlap_size: int) -> dict:
    """Инкрементальный сбор: до last_successful - overlap (включительно)."""
    context = get_current_context()
    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    import duckdb

    conn = duckdb.connect(duckdb_file, read_only=True)
    try:
        st = state.get_state(conn, username)
    finally:
        conn.close()

    stop_at = None
    if st and st["last_successful_message_id"] is not None:
        stop_at = max(0, st["last_successful_message_id"] - overlap_size)

    run_id = context["run_id"].replace(":", "_").replace("+", "_")
    bronze_root = _env("BRONZE_PATH", "data/bronze/telegram_messages")
    timeout = int(_env("TG_REQUEST_TIMEOUT", "30"))
    try:
        info = collector.fetch_channel_info(username, timeout=timeout)
        bronze.write_channel_info_partition(info, bronze_root, run_id)
        posts = collector.collect_channel(
            username, stop_at_message_id=stop_at, timeout=timeout
        )
        if posts.is_empty():
            return {
                "username": username,
                "status": "ok",
                "collected": 0,
                "last_seen_message_id": st["last_seen_message_id"] if st else None,
                "bronze_file": None,
            }
        path = bronze.write_bronze_partition(posts, bronze_root, run_id)
        return {
            "username": username,
            "status": "ok",
            "collected": len(posts),
            "last_seen_message_id": int(posts["message_id"].max()),
            "bronze_file": str(path),
        }
    except collector.ChannelUnavailable as exc:
        return {"username": username, "status": "channel_unavailable", "error": str(exc)}
    except collector.TemporaryCollectError as exc:
        return {"username": username, "status": "temporary_error", "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        return {"username": username, "status": "failed", "error": repr(exc)[:500]}


def update_collection_state_task(results: list[dict]) -> dict:
    """Задача-писатель: записать состояния всех каналов в одной транзакции DuckDB."""
    import duckdb

    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    conn = duckdb.connect(duckdb_file)
    try:
        state.ensure_state_table(conn)
        rows = []
        ok = failed = 0
        for r in results:
            if r.get("status") == "ok" and r.get("last_seen_message_id") is not None:
                rows.append(state.success_state(r["username"], r["last_seen_message_id"]))
                ok += 1
            elif r.get("status") == "ok":
                rows.append(state.error_state(r["username"], "ok", "no messages"))
                ok += 1
            else:
                rows.append(
                    state.error_state(r["username"], r["status"], r.get("error", "unknown"))
                )
                failed += 1
        state.upsert_state(conn, rows)
    finally:
        conn.close()
    summary = {"channels_ok": ok, "channels_failed": failed}
    if failed:
        # visible in the UI, but does not fail the whole run
        print(f"итог сбора: {summary}")
    return summary


# ---------- reconciliation ----------

def compare_collected_ids(collected_ids: list[int], silver_ids: set[int]) -> list[int]:
    """ID из свежесобранного окна, отсутствующие в Silver."""
    return sorted(set(collected_ids) - silver_ids)


def reconcile_channel_task(username: str, window: int) -> dict:
    """Пересобрать свежее окно канала и сверить с Silver.

    Свежесобранные посты всегда пишутся в Bronze (дедуп — в normalization);
    пробелы в Silver внутри окна фиксируются в отчёте.
    """
    context = get_current_context()
    run_id = context["run_id"].replace(":", "_").replace("+", "_")
    bronze_root = _env("BRONZE_PATH", "data/bronze/telegram_messages")
    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    timeout = int(_env("TG_REQUEST_TIMEOUT", "30"))

    import duckdb

    try:
        posts = collector.collect_channel(username, max_messages=window, timeout=timeout)
        collected_ids = posts["message_id"].to_list() if not posts.is_empty() else []
        silver_ids: set[int] = set()
        if collected_ids:
            conn = duckdb.connect(duckdb_file, read_only=True)
            try:
                rows = conn.execute(
                    "SELECT message_id FROM messages WHERE channel_id = ? "
                    "AND message_id BETWEEN ? AND ?",
                    [username, min(collected_ids), max(collected_ids)],
                ).fetchall()
                silver_ids = {r[0] for r in rows}
            finally:
                conn.close()
        missing = compare_collected_ids(collected_ids, silver_ids)

        bronze_file = None
        if not posts.is_empty():
            bronze_file = str(bronze.write_bronze_partition(posts, bronze_root, run_id))
        return {
            "username": username,
            "status": "ok",
            "collected": len(collected_ids),
            "missing_in_silver": len(missing),
            "missing_ids": missing[:50],
            "bronze_file": bronze_file,
        }
    except collector.ChannelUnavailable as exc:
        return {"username": username, "status": "channel_unavailable", "error": str(exc)}
    except collector.TemporaryCollectError as exc:
        return {"username": username, "status": "temporary_error", "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        return {"username": username, "status": "failed", "error": repr(exc)[:500]}


RECONCILIATION_DDL = """
CREATE TABLE IF NOT EXISTS reconciliation_results (
    run_id          VARCHAR NOT NULL,
    channel_id      VARCHAR NOT NULL,
    collected_count BIGINT,
    missing_count   BIGINT,
    status          VARCHAR,
    detail          VARCHAR,
    checked_at      TIMESTAMP NOT NULL
)
"""


def record_reconciliation_result(results: list[dict]) -> dict:
    """Сохранить историю сверки и продвинуть last_reconciliation_at."""
    import duckdb
    from datetime import datetime, timezone

    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")
    context = get_current_context()
    run_id = context["run_id"]
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = duckdb.connect(duckdb_file)
    try:
        conn.execute(RECONCILIATION_DDL).fetchall()
        rows = []
        for r in results:
            missing = r.get("missing_in_silver", 0)
            rows.append(
                {
                    "run_id": run_id,
                    "channel_id": r["username"],
                    "collected_count": r.get("collected", 0),
                    "missing_count": missing,
                    "status": r.get("status", "failed"),
                    "detail": None if not missing else f"missing ids: {r.get('missing_ids', [])}",
                    "checked_at": now,
                }
            )
        conn.register("_stg_recon", pl.DataFrame(rows))
        conn.execute("BEGIN")
        try:
            conn.execute("INSERT INTO reconciliation_results SELECT * FROM _stg_recon").fetchall()
            # продвигаем время сверки только для успешно проверенных каналов
            state_rows = [
                {
                    "channel_id": r["username"],
                    "last_successful_message_id": None,
                    "last_successful_run_at": None,
                    "last_seen_message_id": None,
                    "last_reconciliation_at": now,
                    "collection_status": None if r.get("status") == "ok" else r.get("status"),
                    "last_error": r.get("error"),
                    "updated_at": now,
                }
                for r in results
                if r.get("status") == "ok" or r.get("error")
            ]
            if state_rows:
                # collection_status NULL не должен затирать существующий статус
                fixed = []
                for s in state_rows:
                    if s["collection_status"] is None:
                        conn.execute(
                            "UPDATE collection_state SET last_reconciliation_at = ?, updated_at = ? "
                            "WHERE channel_id = ?",
                            [now, now, s["channel_id"]],
                        ).fetchall()
                    else:
                        fixed.append(s)
                if fixed:
                    # для ошибочных каналов фиксируем только ошибку
                    for s in fixed:
                        conn.execute(
                            "UPDATE collection_state SET collection_status = ?, last_error = ?, "
                            "updated_at = ? WHERE channel_id = ?",
                            [s["collection_status"], s["last_error"], now, s["channel_id"]],
                        ).fetchall()
            conn.execute("COMMIT")
        finally:
            conn.unregister("_stg_recon")
    finally:
        conn.close()
    return {
        "channels": len(results),
        "with_gaps": sum(1 for r in results if r.get("missing_in_silver")),
    }
