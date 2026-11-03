"""mart_pipeline_health_daily: здоровье DAG-ов из метаданных Airflow + счётчики склада."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


def build_pipeline_health(conn, lookback_days: int = 7) -> int:
    """Собрать статистику запусков DAG-ов за N дней в таблицу витрины."""
    import polars as pl
    from airflow.models import DagRun
    from airflow.settings import Session

    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=lookback_days)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    rows = []
    with Session() as session:
        for dr in (
            session.query(DagRun)
            .filter(DagRun.start_date.isnot(None), DagRun.start_date >= since)
            .all()
        ):
            rows.append({
                "activity_date": dr.start_date.date(),
                "dag_id": dr.dag_id,
                "state": str(dr.state),
                "duration": (dr.end_date - dr.start_date).total_seconds() if dr.end_date else None,
                "started_at": dr.start_date,
                "ended_success": str(dr.state) == "success",
            })

    empty_schema = {
        "activity_date": pl.Date, "dag_id": pl.String, "runs_count": pl.UInt32,
        "success_count": pl.UInt32, "failed_count": pl.UInt32,
        "avg_duration_seconds": pl.Float64,
        "messages_collected": pl.Int64, "messages_normalized": pl.Int64,
        "messages_enriched": pl.Int64, "last_success_at": pl.Datetime("us"),
        "quality_failures_count": pl.Int64,
    }

    if not rows:
        conn.register("_stg_health", pl.DataFrame(schema=empty_schema))
    else:
        runs = pl.DataFrame(rows)
        # счётчики сообщений по дням из warehouse
        def _day_counts_sql(sql: str) -> pl.DataFrame:
            # .df() вместо .arrow(): пустой arrow-результат без схемы падает
            return pl.from_pandas(conn.execute(sql).df())

        enriched = _day_counts_sql(
            "SELECT CAST(processed_at AS DATE) AS d, count(*) AS n "
            "FROM message_meta WHERE status='success' GROUP BY 1"
        )
        normalized = _day_counts_sql(
            "SELECT CAST(updated_at AS DATE) AS d, count(*) AS n "
            "FROM messages GROUP BY 1"
        )
        qfail = _day_counts_sql(
            "SELECT CAST(checked_at AS DATE) AS d, count(*) AS n "
            "FROM quality_results WHERE result='FAIL' GROUP BY 1"
        )

        def day_counts(frame, col):
            return (
                runs.select(pl.col("activity_date").unique().sort())
                .join(
                    frame.with_columns(pl.col("d").cast(pl.Date)),
                    left_on="activity_date", right_on="d", how="left",
                )
                .with_columns(pl.col("n").fill_null(0).alias(col))
                .select(["activity_date", col])
            )

        e_by_day = day_counts(enriched, "messages_enriched")
        n_by_day = day_counts(normalized, "messages_normalized")
        q_by_day = day_counts(qfail, "quality_failures_count")

        agg = (
            runs.group_by(["activity_date", "dag_id"])
            .agg(
                pl.len().alias("runs_count"),
                pl.col("ended_success").sum().alias("success_count"),
                (pl.col("state") == "failed").sum().alias("failed_count"),
                pl.col("duration").mean().alias("avg_duration_seconds"),
                pl.col("started_at").filter(pl.col("ended_success")).max().alias("last_success_at"),
            )
        )
        df = (
            agg.join(e_by_day, on="activity_date", how="left")
            .join(n_by_day, on="activity_date", how="left")
            .join(q_by_day, on="activity_date", how="left")
            .with_columns(
                pl.col("messages_enriched").fill_null(0),
                pl.col("messages_normalized").fill_null(0),
                pl.col("quality_failures_count").fill_null(0),
                pl.lit(None, dtype=pl.Int64).alias("messages_collected"),
            )
            .select(list(empty_schema.keys()))
        )
        conn.register("_stg_health", df)

    conn.execute(
        "CREATE OR REPLACE TABLE mart_pipeline_health_daily AS "
        "SELECT * FROM _stg_health"
    ).fetchall()
    conn.unregister("_stg_health")
    return len(rows)
