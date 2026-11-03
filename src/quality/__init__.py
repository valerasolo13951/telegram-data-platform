"""Data Quality layer on Great Expectations (spec §7).

Severity levels: FAIL blocks downstream (critical), WARN is recorded but
does not fail the run. History is kept in DuckDB table quality_results.
"""
