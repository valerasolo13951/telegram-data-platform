# Эксплуатационная документация (Operations)

## Состав системы

| Сервис | Порт | Назначение |
|---|---|---|
| airflow-webserver | 8080 | UI Airflow: запуск/мониторинг DAG-ов |
| airflow-scheduler | — | Исполнение DAG-ов (LocalExecutor) |
| postgres | 5432 (внутр.) | Метаданные Airflow + Superset |
| superset | 8088 | BI-дашборды над витринами Gold |

Данные: `data/` на хосте (Bronze/staging/warehouse/logs/models/backups) — переживают перезапуск контейнеров.

## Старт из чистого окружения

```bash
cp .env.example .env          # заполнить пароли (см. комментарии)
docker compose build          # airflow (~4 ГБ) + superset (~2 ГБ)
docker compose up airflow-init        # инициализация Airflow
docker compose up -d                  # все сервисы
```

Первый запуск пайплайна:
1. В UI (http://localhost:8080, admin из `.env`) разлочить `telegram_backfill` и запустить.
2. `telegram_normalization` → `telegram_enrichment` → `telegram_analytics_marts` (запускаются вручную или дождаться расписания).
3. `telegram_quality` — проверки; FAIL роняет задачу (см. «Качество»).
4. Superset: http://localhost:8088, админ из `.env`; 6 дашбордов создаются автоматически при старте.

## DAG-и и расписания

| DAG | Расписание | Назначение |
|---|---|---|
| `telegram_backfill` | вручную | Первичная история каналов (`config/channels.csv`) |
| `telegram_ingestion` | ежечасно | Инкрементальный сбор, перекрытие `TG_OVERLAP_SIZE` |
| `telegram_normalization` | ежечасно | Bronze → Silver (дедуп, ссылки), учитывает `bronze_processed_files` |
| `telegram_enrichment` | ежечасно | 3 ML-модели (закреплённые ревизии), запись `message_meta` |
| `telegram_analytics_marts` | ежечасно | Пересборка 8 витрин Gold + Gold-проверки |
| `telegram_quality` | ежечасно | GX-проверки Bronze/Silver, PASS/WARN/FAIL |
| `telegram_reconciliation` | ежедневно 04:00 | Сверка окна истории, пробелы → Bronze |
| `tdp_backup` | ежедневно | Бэкап warehouse + метаданных Superset, проверка восстановления |

Правила повторного запуска каждого DAG — в docstring файла DAG (`dags/*.py`).

## Качество (PASS / WARN / FAIL)

- `FAIL` критической проверки роняет задачу — виден в UI/алертах Airflow, блокирует downstream (в т.ч. пересборку витрин при Gold-FAIL).
- `WARN` записывается и не блокирует.
- История: таблица `quality_results` в DuckDB (check/suite/layer/результат/rows/run_id).
- Пороги проверок: `src/quality/suites.py`, `src/quality/gold.py`.

## Мониторинг и алертинг

- Базовый уровень — Airflow: статус DAG-ов, retry, длительности; визуально в UI + `mart_pipeline_health_daily` (дашборд Pipeline Health).
- Email-алерты: задать `AIRFLOW__SMTP__*` в compose и `email_on_failure: True` в `default_args` DAG-ов.
- Состояние здоровья сборщиков: таблица `collection_state` (`collection_status`, `last_error`).

## Резервное копирование и восстановление

- DAG `tdp_backup` (ежедневно) создаёт: согласованный снапшот DuckDB (ATTACH read_only + копирование в одной транзакции — НЕ прямая копия живого файла) и `pg_dump` метаданных Superset. Скрипт сам проверяет снапшот восстановлением (счёт таблиц/строк/витрин).
- Восстановление warehouse:
  ```bash
  cp data/backups/<ts>/telegram.duckdb data/warehouse/telegram.duckdb
  ```
  Восстановление метаданных Superset (пример):
  ```bash
  createdb -h localhost -U airflow superset_restored
  psql -U airflow -d superset_restored < data/backups/<ts>/superset_meta.sql
  ```
- Срок хранения не настраивается автоматически: периодически чистите `data/backups/` вручную.

## Типовые операции

**Добавить каналы** — дописать в `config/channels.csv` (`username,is_active`), запустить `telegram_backfill` (для истории) — далее сбор идёт автоматически.

**Обновить ML-модель** — правка только в `src/enrichment/models.py` (новый commit SHA). Старые результаты в `message_meta` не трогаются, сообщения пересчитываются с новой версией. Smoke-тесты: `uv run pytest -m slow`.

**Миграция схемы DuckDB** — bump `SCHEMA_VERSION` в `src/warehouse/schema.py` + миграция; идемпотентные запуски не сломаются.

**Проверка витрин глазами** — дашборды Superset либо:
```bash
python scripts/superset_login.py && python scripts/verify_charts.py
```

**Полный сброс склада** (Bronze сохраняется, пересборка без Telegram):
```bash
docker compose exec airflow-scheduler python -c "
import duckdb; c=duckdb.connect('/opt/airflow/data/warehouse/telegram.duckdb')
[c.execute(f'DROP TABLE IF EXISTS {t[0]}') for t in c.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchall()]"
# затем: init_warehouse.py → telegram_normalization → telegram_enrichment → telegram_analytics_marts
```

## Известные ограничения

- Источник — публичный веб-предпросмотр t.me: просмотры/пересылки/ответы отсутствуют (NULL), глубина истории ограничена, каналы без предпросмотра → `channel_unavailable`.
- DuckDB — один писатель: запись только из DAG-ов через publisher; Superset подключён строго `access_mode=read_only`.
- Superset 5.0 требует duckdb ≤1.5.x (python 3.10) — версия выровнена с образом Airflow; при обновлении duckdb хранилище потребует пересоздания (данные восстановимы из Bronze).
- В `.env` хранятся пароли; файл в `.gitignore`, не коммитить.

## Контакты/эскалация

Логи задач: `data/logs/` (или UI Airflow). Логи контейнеров: `docker compose logs <service>`.
