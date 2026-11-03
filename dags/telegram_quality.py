"""DAG telegram_quality: проверки качества Bronze и Silver (Great Expectations).

Входные данные: последние Bronze-файлы, таблицы Silver в DuckDB.
Выходные данные: история проверок в quality_results (PASS/WARN/FAIL).
Повторный запуск: безопасен — добавляет новую запись истории.
FAIL критической проверки роняет задачу (блокирует downstream и виден
в алертах Airflow); WARN только записывается.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from quality.tasks import run_quality_task  # noqa: E402


@task(retries=1)
def validate() -> dict:
    return run_quality_task()


with DAG(
    dag_id="telegram_quality",
    description="Проверки качества данных (Great Expectations)",
    start_date=dt.datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["tdp", "quality"],
    default_args={
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=5),
        "execution_timeout": dt.timedelta(minutes=30),
    },
) as dag:
    validate()
