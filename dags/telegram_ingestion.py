"""DAG telegram_ingestion: регулярный инкрементальный сбор новых сообщений.

Входные данные: config/channels.csv + collection_state в DuckDB (курсоры).
Выходные данные: новые Bronze-партиции, продвижение collection_state.
Повторный запуск: безопасен — сбор идёт с перекрытием TG_OVERLAP_SIZE
сообщений ниже последнего успешного, дубликаты в Bronze допустимы
(дедупликация выполняется при загрузке в Silver); состояние канала
продвигается только после успешной публикации Bronze-партиции.
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
from ingestion.tasks import (  # noqa: E402
    collect_channel_incremental_task,
    update_collection_state_task,
)

CHANNELS_FILE = os.environ.get("CHANNELS_FILE", "/opt/airflow/config/channels.csv")
OVERLAP_SIZE = int(os.environ.get("TG_OVERLAP_SIZE", "10"))
SCHEDULE = os.environ.get("TG_INGESTION_SCHEDULE", "0 * * * *")


@task
def load_active_channels() -> list[str]:
    return [c["username"] for c in load_channel_registry(CHANNELS_FILE, active_only=True)]


@task(retries=0, max_active_tis_per_dag=int(os.environ.get("TG_MAX_PARALLEL_CHANNELS", "4")))
def collect(username: str) -> dict:
    return collect_channel_incremental_task(username, overlap_size=OVERLAP_SIZE)


@task
def update_state(results: list[dict]) -> dict:
    return update_collection_state_task(results)


with DAG(
    dag_id="telegram_ingestion",
    description="Инкрементальный сбор Telegram-каналов (snscrape)",
    start_date=dt.datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["tdp", "ingestion"],
    default_args={
        "retries": 2,
        "retry_delay": dt.timedelta(minutes=2),
        "execution_timeout": dt.timedelta(minutes=30),
    },
) as dag:
    usernames = load_active_channels()
    results = collect.expand(username=usernames)
    update_state(results)
