"""Слой качества данных на Great Expectations (спецификация, §7).

Уровни: FAIL блокирует downstream (критично), WARN записывается, но не роняет
запуск. История хранится в таблице quality_results в DuckDB.
"""
