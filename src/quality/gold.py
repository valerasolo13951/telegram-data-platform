"""Gold-проверки: ожидаемая схема, уникальность гранулярности, неотрицательность."""
from __future__ import annotations

import great_expectations.expectations as gxe

from .runner import Check, custom_check, gx_check


def gold_checks(mart_name: str, expected_columns: list[str], grain: list[str]) -> list[Check]:
    def columns_present(df):
        missing = [c for c in expected_columns if c not in df.columns]
        return max(len(df.columns), 1), len(missing), f"отсутствуют колонки: {missing}"

    def dates_in_range(df):
        import pandas as pd

        col = next((c for c in ("activity_date", "period_start") if c in df.columns), None)
        if col is None:
            return len(df), 0, ""
        lo = pd.Timestamp("2000-01-01")
        hi = pd.Timestamp.now() + pd.Timedelta(days=2)
        bad = int(((df[col] < lo) | (df[col] > hi)).sum())
        return len(df), bad, "даты вне допустимого диапазона"

    checks = [
        custom_check(f"{mart_name}.columns_present", "fail", columns_present),
        custom_check(f"{mart_name}.dates_in_range", "fail", dates_in_range),
    ]
    if grain and all(c in expected_columns for c in grain):
        checks.append(gx_check(
            f"{mart_name}.grain_unique", "fail",
            gxe.ExpectCompoundColumnsToBeUnique(column_list=grain),
        ))
    # неотрицательность всех счётчиков/сумм в витрине
    for col in expected_columns:
        if any(k in col for k in ("count", "sum", "_share", "coverage")):
            checks.append(gx_check(
                f"{mart_name}.{col}_non_negative", "fail",
                gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0),
            ))
    return checks
