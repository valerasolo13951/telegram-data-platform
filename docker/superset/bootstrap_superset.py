"""Bootstrap Superset: админ, подключение DuckDB, датасеты, 6 дашбордов.

Запускается внутри образа superset с контекстом приложения (без webserver).
Идемпотентен: существующие объекты пропускаются.
"""
from __future__ import annotations

import json
import os
import uuid

DUCKDB_URI = os.environ.get(
    "SUPERSET_DUCKDB_URI",
    # access_mode=read_only в URI: несколько read-only соединений Superset
    # сосуществуют с одиночным писателем Airflow в тот же файл
    "duckdb:////opt/airflow/data/warehouse/telegram.duckdb?access_mode=read_only",
)
ADMIN_USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")
ADMIN_EMAIL = "admin@example.org"

# Дашборды из спецификации §9: название -> витрина (основная) + колонки таблицы
DASHBOARDS = {
    "Channel Activity": {
        "table": "mart_channel_activity_daily",
        "columns": ["activity_date", "channel_id", "messages_count",
                    "messages_with_links_count", "avg_text_length"],
        "timeseries": ("activity_date", "messages_count"),
    },
    "Sentiment": {
        "table": "mart_message_sentiment_daily",
        "columns": ["activity_date", "channel_id", "messages_processed",
                    "positive_share", "neutral_share", "negative_share"],
        "timeseries": ("activity_date", None),
    },
    "Toxicity": {
        "table": "mart_message_toxicity_daily",
        "columns": ["activity_date", "channel_id", "messages_processed",
                    "insult_count", "obscenity_count", "dangerous_count"],
        "timeseries": ("activity_date", None),
    },
    "Emotions": {
        "table": "mart_message_emotions_daily",
        "columns": ["activity_date", "channel_id", "joy_count", "sadness_count",
                    "fear_count", "anger_count", "no_emotion_count"],
        "timeseries": ("activity_date", None),
    },
    "External Domains": {
        "table": "mart_domain_mentions_daily",
        "columns": ["activity_date", "domain", "mentions_count",
                    "unique_channels_count", "external_links_count"],
        "timeseries": ("activity_date", None),
    },
    "Pipeline Health": {
        "table": "mart_pipeline_health_daily",
        "columns": ["activity_date", "dag_id", "runs_count", "success_count",
                    "failed_count", "quality_failures_count"],
        "timeseries": ("activity_date", None),
    },
}

DASHBOARD_VERSION_KEY = "DASHBOARD_VERSION_KEY"


def main() -> None:
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        # импорты моделей требуют контекста приложения
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        # --- admin ---
        sm = app.appbuilder.sm
        if not sm.find_user(ADMIN_USER):
            sm.add_user(
                ADMIN_USER, "Admin", "User", ADMIN_EMAIL,
                sm.find_role("Admin"), ADMIN_PASSWORD,
            )
            print(f"user {ADMIN_USER} created")

        # --- база данных (duckdb read-only) ---
        database = (
            db.session.query(Database)
            .filter_by(database_name="telegram_warehouse")
            .one_or_none()
        )
        if not database:
            database = Database(database_name="telegram_warehouse", sqlalchemy_uri=DUCKDB_URI)
            db.session.add(database)
            db.session.flush()
            print("database telegram_warehouse created")
        elif database.sqlalchemy_uri != DUCKDB_URI:
            database.sqlalchemy_uri = DUCKDB_URI
            database.extra = "{}"
            db.session.flush()

        # --- datasets ---
        datasets = {}
        for name in {cfg["table"] for cfg in DASHBOARDS.values()}:
            ds = db.session.query(SqlaTable).filter_by(table_name=name).one_or_none()
            if ds is None:
                ds = SqlaTable(table_name=name)
                ds.database = database
                ds.database_id = database.id
                db.session.add(ds)
                db.session.flush()
            # колонки обновляем всегда: витрины пересоздаются DAG-ом marts
            try:
                ds.fetch_metadata()
                db.session.commit()
            except Exception as exc:  # витрина ещё не построена
                print(f"WARN: {name}: metadata refresh failed: {repr(exc)[:120]}")
                db.session.rollback()
            datasets[name] = ds
        db.session.commit()
        print(f"datasets ready: {len(datasets)}")

        # --- чарты + дашборды ---
        for title, cfg in DASHBOARDS.items():
            existing = db.session.query(Dashboard).filter_by(dashboard_title=title).one_or_none()
            if existing:
                print(f"dashboard exists: {title}")
                continue
            ds = datasets[cfg["table"]]
            slices = []

            table_params = {
                "datasource": f"{ds.id}__table",
                "viz_type": "table",
                "groupby": [],
                "all_columns": cfg["columns"],
                "metrics": [],
                "percent_metrics": [],
                "row_limit": 1000,
                "time_range": "Last 5 years",
            }
            slices.append(Slice(
                slice_name=f"{title} — таблица",
                datasource_id=ds.id, datasource_type="table",
                viz_type="table", params=json.dumps(table_params),
            ))
            db.session.add(slices[0])
            db.session.flush()

            if cfg["timeseries"][1]:
                x_col, metric_col = cfg["timeseries"]
                metric = {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": metric_col, "type": "BIGINT"},
                    "aggregate": "SUM",
                    "sqlExpression": None,
                    "label": f"SUM({metric_col})",
                }
                line_params = {
                    "datasource": f"{ds.id}__table",
                    "viz_type": "echarts_timeseries_line",
                    "granularity_sqla": x_col,
                    "time_grain_sqla": "P1D",
                    "x_axis": x_col,
                    "metrics": [metric],
                    "groupby": [],
                    "time_range": "Last 5 years",
                }
                slices.append(Slice(
                    slice_name=f"{title} — динамика",
                    datasource_id=ds.id, datasource_type="table",
                    viz_type="echarts_timeseries_line", params=json.dumps(line_params),
                ))
                db.session.add(slices[1])
                db.session.flush()

            dash = Dashboard(dashboard_title=title, slices=slices)
            dash.position_json = _layout(slices)
            db.session.add(dash)
            db.session.commit()
            print(f"dashboard created: {title} ({len(slices)} charts)")

        # --- контрольный прогон метаданных датасетов ---
        # На чистой установке fetch_metadata внутри цикла создания может
        # не зафиксировать колонки; повторяем в конце и проверяем результат.
        for name in sorted(datasets):
            ds = db.session.query(SqlaTable).filter_by(table_name=name).one_or_none()
            if ds is None:
                continue
            db.session.expire(ds, ["columns"])
            if ds.columns:
                continue
            try:
                ds.fetch_metadata()
                db.session.commit()
                print(f"metadata fixed: {name} ({len(ds.columns)} columns)")
            except Exception as exc:
                db.session.rollback()
                print(f"WARN: {name}: late metadata refresh failed: {repr(exc)[:120]}")


def _layout(slices) -> str:
    """Корректный v2-layout: обязателен корень ROOT_ID, от которого
    разрешается цепочка parents у GRID/ROW/CHART, иначе фронтенд
    рендерит бесконечную загрузку."""
    grid = "GRID_ID"
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "id": "ROOT_ID", "type": "ROOT",
            "props": {"background": "BACKGROUND_TEXTURE"},
        },
        grid: {
            "id": grid, "type": "GRID", "children": [],
            "parents": ["ROOT_ID"], "meta": {},
        },
    }
    for i, s in enumerate(slices):
        row_id = f"ROW-{i}"
        chart_id = f"CHART-{s.id}"
        position[row_id] = {
            "id": row_id, "type": "ROW", "children": [chart_id],
            "parents": ["ROOT_ID", grid], "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        position[chart_id] = {
            "id": chart_id, "type": "CHART", "children": [],
            "parents": ["ROOT_ID", grid, row_id],
            "meta": {"chartId": s.id, "uuid": str(uuid.uuid4()), "width": 12, "height": 50},
        }
        position[grid]["children"].append(row_id)
    return json.dumps(position)


if __name__ == "__main__":
    main()
