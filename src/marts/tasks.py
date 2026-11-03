"""Задачи витрин для Airflow."""
from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def refresh_marts_task() -> dict:
    """Пересобрать все витрины Gold (один писатель), затем gold-проверки."""
    import duckdb

    from quality.runner import FAIL, run_suite
    from quality.gold import gold_checks
    from .builder import MART_COLUMNS, MART_GRAIN, build_marts
    from .pipeline_health import build_pipeline_health

    from airflow.operators.python import get_current_context

    context = get_current_context()
    run_id = context["run_id"]
    duckdb_file = _env("DUCKDB_FILE", "data/warehouse/telegram.duckdb")

    conn = duckdb.connect(duckdb_file)
    summaries = []
    try:
        built = build_marts(conn)
        build_pipeline_health(conn, lookback_days=7)

        # Gold-проверки: схема, гранулярность, неотрицательность
        for mart in list(MART_COLUMNS):
            df = pl_from_table(conn, mart)
            if df is None or df.empty:
                continue
            import pandas as pd

            summaries.append(run_suite(
                conn, pd.DataFrame(df), f"gold_{mart}", "gold", mart,
                gold_checks(mart, MART_COLUMNS[mart], MART_GRAIN.get(mart, [])), run_id,
            ))
    finally:
        conn.close()

    total_fail = sum(s["fail"] for s in summaries)
    print(f"витрины собраны: {built}; gold-сводка: {summaries}")
    if total_fail:
        raise RuntimeError(f"gold-проверки не пройдены: {total_fail} FAIL")
    return {"marts": built, "suites": summaries}


def pl_from_table(conn, table: str):
    import polars as pl

    exists = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchall()[0][0]
    if not exists:
        return None
    return pl.from_arrow(conn.sql(f"SELECT * FROM {table}").arrow()).to_pandas()
