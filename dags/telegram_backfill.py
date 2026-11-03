"""DAG telegram_backfill: первичная загрузка истории каналов.

Входные данные: config/channels.csv (реестр каналов).
Выходные данные: Parquet-партиции в Bronze (channel_id=<u>/collection_date=...),
обновлённое состояние сбора в DuckDB (collection_state).
Повторный запуск: безопасен — Bronze только добавляет файлы, состояние
продвигается только после успешной записи; ошибки отдельных каналов
изолированы (mapped tasks) и не останавливают остальные.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion.registry import load_channel_registry  # noqa: E402
from ingestion.tasks import collect_channel_task, update_collection_state_task  # noqa: E402

CHANNELS_FILE = os.environ.get("CHANNELS_FILE", "/opt/airflow/config/channels.csv")
BACKFILL_LIMIT = int(os.environ.get("TG_BACKFILL_MAX_MESSAGES", "1000"))
OVERLAP_SIZE = int(os.environ.get("TG_OVERLAP_SIZE", "0"))


@task
def load_registry() -> list[str]:
    return [c["username"] for c in load_channel_registry(CHANNELS_FILE, active_only=True)]


@task(retries=0, max_active_tis_per_dag=int(os.environ.get("TG_MAX_PARALLEL_CHANNELS", "4")))
def collect(username: str) -> dict:
    # ошибка канала возвращается как статус, а не исключение — изоляция каналов
    return collect_channel_task(username, max_messages=BACKFILL_LIMIT, overlap_size=OVERLAP_SIZE)


@task
def update_state(results: list[dict]) -> dict:
    return update_collection_state_task(results)


with DAG(
    dag_id="telegram_backfill",
    description="Первичная загрузка истории Telegram-каналов (snscrape)",
    start_date=dt.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["tdp", "ingestion"],
    default_args={
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=2),
        "execution_timeout": dt.timedelta(hours=2),
    },
) as dag:
    usernames = load_registry()
    results = collect.expand(username=usernames)
    update_state(results)
