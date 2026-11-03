"""DAG telegram_analytics_marts: пересборка аналитических витрин Gold.

Входные данные: Silver-таблицы DuckDB, метаданные запусков Airflow.
Выходные данные: 8 витрин mart_* + Gold-проверки в quality_results.
Повторный запуск: безопасен — витрины пересоздаются целиком (CREATE OR
REPLACE) без обращения к Telegram; идемпотентен.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from marts.tasks import refresh_marts_task  # noqa: E402


@task(retries=1)
def refresh_marts() -> dict:
    return refresh_marts_task()


with DAG(
    dag_id="telegram_analytics_marts",
    description="Пересборка аналитических витрин Gold",
    start_date=dt.datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["tdp", "analytics"],
    default_args={
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=5),
        "execution_timeout": dt.timedelta(minutes=30),
    },
) as dag:
    refresh_marts()
