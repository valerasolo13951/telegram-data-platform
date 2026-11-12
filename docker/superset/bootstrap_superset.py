"""Bootstrap Superset: админ, подключение DuckDB, датасеты, 6 дашбордов.

Запускается внутри образа superset с контекстом приложения (без webserver).
Идемпотентен: существующие объекты пропускаются.

Каждый дашборд = KPI/график динамики + детальная таблица:
- линейные графики (echarts_timeseries_line) показывают динамику по месяцам;
- pie — топ доменов; big_number_total — ключевые цифры;
- таблицы дают детали по дням.
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

# ---- переиспользуемые элементы form_data ----

def _metric(col: str, agg: str, label: str, dtype: str = "BIGINT") -> dict:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col, "type": dtype},
        "aggregate": agg,
        "sqlExpression": None,
        "label": label,
    }


def _sql_metric(expr: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


LINE_BASE = {
    "viz_type": "echarts_timeseries_line",
    "x_axis": "activity_date",
    "time_grain_sqla": "P1M",
    "groupby": [],
    "time_range": "Last 10 years",
    "row_limit": 10000,
    "zoomable": True,
}
PIE_BASE = {
    "viz_type": "pie",
    "time_range": "Last 10 years",
    "donut": True,
    "sort_by_metric": True,
    "labelLine": False,
    "outerRadius": 70,
}
TABLE_BASE = {
    "viz_type": "table",
    "metrics": [],
    "percent_metrics": [],
    "row_limit": 1000,
    "time_range": "No filter",
    "order_desc": True,
}
KPI_BASE = {
    "viz_type": "big_number_total",
    "time_range": "Last 10 years",
}


def _line(metrics: list[dict], groupby: list[str] | None = None) -> dict:
    p = dict(LINE_BASE)
    p["metrics"] = metrics
    if groupby:
        p["groupby"] = groupby
    return p


def _table(columns: list[str]) -> dict:
    p = dict(TABLE_BASE)
    p["all_columns"] = columns
    return p


DASHBOARDS = {
    "Channel Activity": {
        "table": "mart_channel_activity_daily",
        "charts": [
            {
                "name": "Всего сообщений",
                "width": 4,
                "params": {
                    **KPI_BASE,
                    "metric": _metric("messages_count", "SUM", "Сообщений"),
                },
            },
            {
                "name": "Динамика публикаций по месяцам",
                "width": 8,
                "params": _line(
                    [_metric("messages_count", "SUM", "Сообщений")],
                    groupby=["channel_id"],
                ),
            },
            {
                "name": "Сообщения и ссылки по дням",
                "width": 12,
                "params": _line(
                    [
                        _metric("messages_count", "SUM", "Сообщений"),
                        _metric("messages_with_links_count", "SUM", "С ссылками"),
                    ],
                ),
            },
            {
                "name": "Детали по дням",
                "width": 12,
                "params": _table([
                    "activity_date", "channel_id", "messages_count",
                    "messages_with_text_count", "messages_with_links_count",
                    "avg_text_length", "last_message_at",
                ]),
            },
        ],
    },
    "Sentiment": {
        "table": "mart_message_sentiment_daily",
        "charts": [
            {
                "name": "Тональность публикаций по месяцам, %",
                "width": 12,
                "params": _line([
                    _sql_metric("AVG(negative_share) * 100", "Негатив, %"),
                    _sql_metric("AVG(neutral_share) * 100", "Нейтрал, %"),
                    _sql_metric("AVG(positive_share) * 100", "Позитив, %"),
                ]),
            },
            {
                "name": "Детали по дням и каналам",
                "width": 12,
                "params": _table([
                    "activity_date", "channel_id", "model_version",
                    "messages_processed", "negative_count", "neutral_count",
                    "positive_count", "negative_share", "positive_share",
                ]),
            },
        ],
    },
    "Toxicity": {
        "table": "mart_message_toxicity_daily",
        "charts": [
            {
                "name": "Категории токсичности по месяцам",
                "width": 12,
                "params": _line([
                    _metric("insult_count", "SUM", "Оскорбления"),
                    _metric("obscenity_count", "SUM", "Обсценная лексика"),
                    _metric("threat_count", "SUM", "Угрозы"),
                    _metric("dangerous_count", "SUM", "Опасный контент"),
                ]),
            },
            {
                "name": "Средний уровень insult по месяцам",
                "width": 12,
                "params": _line([
                    _sql_metric("AVG(avg_insult_score) * 100", "Insult, балл ×100"),
                    _sql_metric("AVG(avg_dangerous_score) * 100", "Dangerous, балл ×100"),
                ]),
            },
            {
                "name": "Детали по дням и каналам",
                "width": 12,
                "params": _table([
                    "activity_date", "channel_id", "messages_processed",
                    "insult_count", "obscenity_count", "threat_count",
                    "dangerous_count", "non_toxic_count", "avg_insult_score",
                ]),
            },
        ],
    },
    "Emotions": {
        "table": "mart_message_emotions_daily",
        "charts": [
            {
                "name": "Эмоции публикаций по месяцам",
                "width": 12,
                "params": _line([
                    _metric("joy_count", "SUM", "Радость"),
                    _metric("sadness_count", "SUM", "Грусть"),
                    _metric("surprise_count", "SUM", "Удивление"),
                    _metric("fear_count", "SUM", "Страх"),
                    _metric("anger_count", "SUM", "Злость"),
                ]),
            },
            {
                "name": "Детали по дням и каналам",
                "width": 12,
                "params": _table([
                    "activity_date", "channel_id", "messages_processed",
                    "joy_count", "sadness_count", "surprise_count",
                    "fear_count", "anger_count", "no_emotion_count",
                ]),
            },
        ],
    },
    "External Domains": {
        "table": "mart_domain_mentions_daily",
        "charts": [
            {
                "name": "Топ-10 доменов по упоминаниям",
                "width": 6,
                "params": {
                    **PIE_BASE,
                    "groupby": ["domain"],
                    "metric": _metric("mentions_count", "SUM", "Упоминаний"),
                    "row_limit": 10,
                },
            },
            {
                "name": "Упоминания внешних доменов по месяцам",
                "width": 6,
                "params": _line([
                    _metric("external_links_count", "SUM", "Внешние ссылки"),
                    _metric("telegram_links_count", "SUM", "Ссылки на Telegram"),
                ]),
            },
            {
                "name": "Детали по дням и доменам",
                "width": 12,
                "params": _table([
                    "activity_date", "domain", "mentions_count",
                    "unique_messages_count", "unique_channels_count",
                    "telegram_links_count", "external_links_count",
                ]),
            },
        ],
    },
    "Pipeline Health": {
        "table": "mart_pipeline_health_daily",
        "charts": [
            {
                "name": "Неуспешных запусков",
                "width": 4,
                "params": {
                    **KPI_BASE,
                    "metric": _metric("failed_count", "SUM", "Fail-запусков"),
                },
            },
            {
                "name": "Успешные и неуспешные запуски DAG по месяцам",
                "width": 8,
                "params": _line([
                    _metric("success_count", "SUM", "Успешные"),
                    _metric("failed_count", "SUM", "Неуспешные"),
                ]),
            },
            {
                "name": "Детали по дням и DAG-ам",
                "width": 12,
                "params": _table([
                    "activity_date", "dag_id", "runs_count", "success_count",
                    "failed_count", "avg_duration_seconds", "quality_failures_count",
                ]),
            },
        ],
    },
}


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

        # --- датасеты ---
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
            except Exception as exc:
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
            for chart in cfg["charts"]:
                params = {"datasource": f"{ds.id}__table", **chart["params"]}
                s = Slice(
                    slice_name=chart["name"],
                    datasource_id=ds.id, datasource_type="table",
                    viz_type=params["viz_type"], params=json.dumps(params, ensure_ascii=False),
                )
                db.session.add(s)
                db.session.flush()
                slices.append(s)

            dash = Dashboard(dashboard_title=title, slices=slices)
            dash.position_json = _layout(slices, cfg["charts"])
            # ВАЖНО: chart_configuration должен быть ПУСТЫМ словарём — записи
            # {id, slice_id} без обязательных полей ломают гидрацию дашборда
            # (бесконечный Loading). color_scheme и остальные ключи —
            # стандартный набор дашбордов, созданных через UI.
            meta = {
                "color_scheme": "supersetColors",
                "chart_configuration": {},
                "default_filters": "{}",
                "expanded_slices": {},
                "label_colors": {},
                "native_filter_configuration": [],
                "refresh_frequency": 0,
                "timed_refresh_immune_slices": [],
            }
            dash.json_metadata = json.dumps(meta)
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


def _layout(slices, chart_cfgs) -> str:
    """Корректный v2-layout: обязателен корень ROOT_ID с children,
    от которого разрешается цепочка parents у GRID/ROW/CHART, иначе
    фронтенд рендерит бесконечную загрузку."""
    grid = "GRID_ID"
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"],
            "props": {"background": "BACKGROUND_TEXTURE"},
        },
        grid: {
            "id": grid, "type": "GRID", "children": [],
            "parents": ["ROOT_ID"], "meta": {},
        },
    }
    # группируем чарты в ряды: подряд идущие узкие (сумма ширин <= 12)
    rows: list[list[int]] = []
    current: list[int] = []
    width = 0
    for i, cfg in enumerate(chart_cfgs):
        w = cfg.get("width", 12)
        if width + w > 12 and current:
            rows.append(current)
            current, width = [], 0
        current.append(i)
        width += w
    if current:
        rows.append(current)

    for r, row in enumerate(rows):
        row_id = f"ROW-{r}"
        position[row_id] = {
            "id": row_id, "type": "ROW",
            "children": [f"CHART-{slices[i].id}" for i in row],
            "parents": ["ROOT_ID", grid], "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for i in row:
            chart_id = f"CHART-{slices[i].id}"
            position[chart_id] = {
                "id": chart_id, "type": "CHART", "children": [],
                "parents": ["ROOT_ID", grid, row_id],
                "meta": {
                    "chartId": slices[i].id,
                    # uuid ДОЛЖЕН совпадать с uuid чарта — фронтенд связывает
                    # чарты с layout по нему, иначе чарты не стартуют
                    "uuid": str(slices[i].uuid),
                    "width": chart_cfgs[i].get("width", 12),
                    "height": 50,
                },
            }
        position[grid]["children"].append(row_id)
    return json.dumps(position)


if __name__ == "__main__":
    main()
