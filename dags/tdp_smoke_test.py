"""Smoke-test DAG: проверка готовности инфраструктуры Phase 1.

Входные данные: нет.
Выходные данные: нет (только логи и создание пустого файла-маркера в staging).
Правила повторного запуска: идемпотентен, безопасен для многократного запуска.
"""
from __future__ import annotations

import datetime as dt
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_ROOT = os.environ["DATA_ROOT"]
BRONZE_PATH = os.environ["BRONZE_PATH"]
STAGING_PATH = os.environ["STAGING_PATH"]
WAREHOUSE_PATH = os.environ["WAREHOUSE_PATH"]
MODELS_PATH = os.environ["MODELS_PATH"]


def check_directories():
    for path in (BRONZE_PATH, STAGING_PATH, WAREHOUSE_PATH, MODELS_PATH, f"{MODELS_PATH}/cache"):
        assert os.path.isdir(path), f"missing directory: {path}"
        assert os.access(path, os.W_OK), f"not writable: {path}"
    marker = os.path.join(STAGING_PATH, ".smoke_marker")
    with open(marker, "w", encoding="utf-8") as fh:
        fh.write(dt.datetime.now(dt.timezone.utc).isoformat())
    print(f"directories OK, marker written: {marker}")


def check_dependencies():
    import duckdb  # noqa: F401
    import snscrape.modules.telegram as sntg  # noqa: F401
    conn = duckdb.connect(":memory:")
    conn.execute("SELECT 42")
    print("duckdb OK, snscrape telegram module OK")


def check_duckdb_persistence():
    import duckdb

    db_file = os.environ["DUCKDB_FILE"]
    conn = duckdb.connect(db_file)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS infra_smoke (checked_at TIMESTAMP, ok BOOLEAN)"
    )
    conn.execute(
        "INSERT INTO infra_smoke VALUES (now(), TRUE)"
    )
    n = conn.execute("SELECT count(*) FROM infra_smoke").fetchone()[0]
    conn.close()
    assert n >= 1
    print(f"duckdb persistence OK at {db_file}, rows={n}")


with DAG(
    dag_id="tdp_smoke_test",
    description="Phase 1 infrastructure smoke test",
    start_date=dt.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tdp", "smoke"],
    default_args={"retries": 1, "retry_delay": dt.timedelta(seconds=10)},
) as dag:
    t1 = PythonOperator(task_id="check_directories", python_callable=check_directories)
    t2 = PythonOperator(task_id="check_dependencies", python_callable=check_dependencies)
    t3 = PythonOperator(task_id="check_duckdb_persistence", python_callable=check_duckdb_persistence)

    t1 >> t2 >> t3
