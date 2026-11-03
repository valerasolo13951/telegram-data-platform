"""DAG tdp_backup: ежедневное резервное копирование warehouse + метаданных Superset.

Входные данные: warehouse DuckDB, метаданные Superset (postgres).
Выходные данные: снапшот DuckDB + pg_dump в data/backups/<ts>/, проверка
восстановления. Повторный запуск: безопасен (новый каталог на каждый ран).
"""
from __future__ import annotations

import datetime as dt

from airflow.operators.bash import BashOperator
from airflow import DAG

with DAG(
    dag_id="tdp_backup",
    description="Резервное копирование warehouse и метаданных Superset",
    start_date=dt.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["tdp", "ops"],
    default_args={
        "retries": 2,
        "retry_delay": dt.timedelta(minutes=10),
        "execution_timeout": dt.timedelta(minutes=15),
    },
) as dag:
    backup = BashOperator(
        task_id="backup_and_verify",
        # строка НЕ должна заканчиваться на ".sh" — иначе BashOperator ищет её
        # как файл шаблона (template_ext) и падает с TemplateNotFound
        bash_command="bash /opt/airflow/scripts/backup.sh ;",
    )
    backup
