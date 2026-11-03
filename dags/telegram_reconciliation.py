"""DAG telegram_reconciliation: сверка недавней истории каналов.

Входные данные: config/channels.csv, таблица messages в DuckDB.
Выходные данные: свежие Bronze-партиции (пропуски подхватит normalization),
записи в reconciliation_results, продвижение last_reconciliation_at.
Повторный запуск: безопасен — повторный сбор пишет Bronze (дедуп в Silver),
результаты сверки только добавляются.
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
from ingestion.tasks import reconcile_channel_task, record_reconciliation_result  # noqa: E402

CHANNELS_FILE = os.environ.get("CHANNELS_FILE", "/opt/airflow/config/channels.csv")
RECON_WINDOW = int(os.environ.get("TG_RECONCILIATION_WINDOW", "100"))
SCHEDULE = os.environ.get("TG_RECONCILIATION_SCHEDULE", "0 4 * * *")


@task
def select_channels() -> list[str]:
    return [c["username"] for c in load_channel_registry(CHANNELS_FILE, active_only=True)]


@task(retries=0, max_active_tis_per_dag=int(os.environ.get("TG_MAX_PARALLEL_CHANNELS", "4")))
def reconcile(username: str) -> dict:
    return reconcile_channel_task(username, window=RECON_WINDOW)


@task
def record(results: list[dict]) -> dict:
    return record_reconciliation_result(results)


with DAG(
    dag_id="telegram_reconciliation",
    description="Периодическая сверка истории Telegram-каналов",
    start_date=dt.datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["tdp", "ingestion"],
    default_args={
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=5),
        "execution_timeout": dt.timedelta(minutes=30),
    },
) as dag:
    record(reconcile.expand(username=select_channels()))
