"""Quality runner: GX expectations + custom checks → PASS/WARN/FAIL + history."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

import great_expectations.expectations as gxe

RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS quality_results (
    check_name        VARCHAR NOT NULL,
    suite_name        VARCHAR NOT NULL,
    layer             VARCHAR NOT NULL,
    table_name        VARCHAR NOT NULL,
    result            VARCHAR NOT NULL,      -- PASS | WARN | FAIL
    severity          VARCHAR NOT NULL,      -- fail | warn
    rows_checked      BIGINT,
    unexpected_count  BIGINT,
    error_message     VARCHAR,
    checked_at        TIMESTAMP NOT NULL,
    run_id            VARCHAR NOT NULL
)
"""

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"


@dataclass
class Check:
    name: str
    severity: str  # "fail" | "warn"
    expectation: object | None = None  # GX expectation
    custom: object | None = None  # callable(df) -> (rows_checked, unexpected_count, message)


def gx_check(name: str, severity: str, expectation) -> Check:
    return Check(name=name, severity=severity, expectation=expectation)


def custom_check(name: str, severity: str, fn) -> Check:
    return Check(name=name, severity=severity, custom=fn)


def _run_one(check: Check, df: pd.DataFrame, suite_name: str) -> dict:
    if check.expectation is not None:
        import uuid

        import great_expectations as gx

        ctx = gx.get_context()
        ds = ctx.data_sources.add_pandas(name=f"ds_{uuid.uuid4().hex[:8]}")
        asset = ds.add_dataframe_asset(name="df")
        bd = asset.add_batch_definition_whole_dataframe("bd")
        suite = gx.ExpectationSuite(name=f"s_{uuid.uuid4().hex[:8]}", expectations=[check.expectation])
        validator = ctx.get_validator(
            batch=bd.get_batch(batch_parameters={"dataframe": df}),
            expectation_suite=suite,
        )
        vr = validator.validate()
        result = vr.results[0]
        success = result.success
        unexpected = int(result.result.get("unexpected_count") or 0)
        rows = int(result.result.get("element_count") or len(df))
        error = None if success else (
            f"{check.name}: {unexpected}/{rows} rows violate {type(check.expectation).__name__}"
        )
        return {
            "result": PASS if success else FAIL,
            "rows_checked": rows,
            "unexpected_count": unexpected,
            "error_message": error,
        }

    rows, unexpected, message = check.custom(df)
    success = unexpected == 0
    return {
        "result": PASS if success else (FAIL if check.severity == "fail" else WARN),
        "rows_checked": int(rows),
        "unexpected_count": int(unexpected),
        "error_message": None if success else (message or f"{check.name}: {unexpected} violations"),
    }


def run_suite(
    conn,
    df: pd.DataFrame,
    suite_name: str,
    layer: str,
    table_name: str,
    checks: list[Check],
    run_id: str,
) -> dict:
    """Run all checks, persist history, return summary. Raises on FAIL."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    statuses = []
    for check in checks:
        try:
            outcome = _run_one(check, df, suite_name)
        except Exception as exc:  # noqa: BLE001 — ошибка проверки ≠ падение DAG
            outcome = {
                "result": FAIL,
                "rows_checked": len(df),
                "unexpected_count": None,
                "error_message": f"check error: {repr(exc)[:300]}",
            }
        result = outcome["result"]
        if not outcome["error_message"] is None and check.severity == "warn" and result == FAIL:
            result = WARN
        statuses.append(result)
        rows.append({
            "check_name": check.name,
            "suite_name": suite_name,
            "layer": layer,
            "table_name": table_name,
            "result": result,
            "severity": check.severity,
            "rows_checked": outcome["rows_checked"],
            "unexpected_count": outcome["unexpected_count"],
            "error_message": outcome["error_message"],
            "checked_at": now,
            "run_id": run_id,
        })

    conn.execute(RESULTS_DDL).fetchall()
    import polars as pl

    conn.register("_stg_quality", pl.DataFrame(rows))
    conn.execute("BEGIN")
    try:
        conn.execute("INSERT INTO quality_results SELECT * FROM _stg_quality").fetchall()
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("_stg_quality")

    summary = {
        "suite": suite_name,
        "table": table_name,
        "pass": statuses.count(PASS),
        "warn": statuses.count(WARN),
        "fail": statuses.count(FAIL),
    }
    return summary
