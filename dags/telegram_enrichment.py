"""DAG telegram_enrichment: обогащение сообщений ML-моделями.

Входные данные: таблица messages (тексты + content_hash), реестр моделей
src/enrichment/models.py (закреплённые ревизии Hugging Face).
Выходные данные: message_meta в DuckDB (оценки классов, версии модели
и inference, input_hash, статусы success/failed/skipped).
Повторный запуск: безопасен — сообщения с успешным результатом той же
версии модели/inference и того же input_hash пропускаются; повторная
публикация идентичного результата не создаёт дублей.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from enrichment.models import MODEL_REGISTRY  # noqa: E402
from enrichment.tasks import publish_meta_task, run_inference_task  # noqa: E402


@task
def select_tasks() -> list[str]:
    """Задачи обогащения = модели в реестре (независимые, расширяются)."""
    return list(MODEL_REGISTRY)


@task(retries=1, executor_config={"resources": {"cpu": "2"}})
def run_inference(task_name: str) -> dict:
    return run_inference_task(task_name)


@task
def publish(results: list[dict]) -> dict:
    return publish_meta_task(results)


with DAG(
    dag_id="telegram_enrichment",
    description="ML-обогащение сообщений (toxicity/sentiment/emotion)",
    start_date=dt.datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["tdp", "enrichment"],
    default_args={
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=5),
        "execution_timeout": dt.timedelta(hours=2),
    },
) as dag:
    publish(run_inference.expand(task_name=select_tasks()))
