FROM apache/superset:5.0.0

USER root
COPY superset_config.py /app/superset_config.py
COPY bootstrap_superset.py /app/bootstrap_superset.py
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py
# Драйвер DuckDB: версия выровнена со складом (duckdb 1.5.6);
# read-only доступ задаётся в URI подключения (access_mode=read_only, см. bootstrap).
# Образ 5.x: venv в /app/.venv без pip — установка через uv.
RUN uv pip install --python /app/.venv/bin/python --no-cache \
    "duckdb==1.5.6" duckdb-engine "sqlalchemy<2.1" psycopg2-binary
USER superset
