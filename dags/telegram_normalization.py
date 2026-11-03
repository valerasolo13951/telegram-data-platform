"""DAG telegram_normalization: Bronze → Silver.

Входные данные: непроверенные Parquet-файлы Bronze (сообщения + channel_info).
Выходные данные: таблицы channels/messages/message_links в DuckDB,
учёт обработанных файлов (bronze_processed_files).
Повторный запуск: безопасен — уже обработанные файлы пропускаются,
публикация идемпотентна (upsert по ключам), повторная обработка одной
партиции не создаёт дублей.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from normalization.tasks import discover_unprocessed_partitions, normalize_partition, publish_silver  # noqa: E402


@task
def discover() -> list[str]:
    return discover_unprocessed_partitions()


@task(retries=2)
def normalize(file_path: str) -> dict:
    return normalize_partition(file_path)


@task
def publish(descriptors: list[dict]) -> dict:
    return publish_silver(descriptors)


with DAG(
    dag_id="telegram_normalization",
    description="Нормализация Bronze в Silver (DuckDB)",
    start_date=dt.datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["tdp", "normalization"],
    default_args={
        "retries": 2,
        "retry_delay": dt.timedelta(minutes=1),
        "execution_timeout": dt.timedelta(minutes=30),
    },
) as dag:
    publish(normalize.expand(file_path=discover()))
