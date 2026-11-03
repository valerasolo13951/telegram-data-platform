#!/usr/bin/env bash
# Backup: warehouse (DuckDB consistent snapshot) + Superset metadata (pg_dump).
# Runs inside the airflow-scheduler container. Backups: /opt/airflow/data/backups.
set -euo pipefail

TS=$(date +%Y%m%d-%H%M%S)
BACKUP_ROOT="${BACKUP_ROOT:-/opt/airflow/data/backups}"
DUCKDB_FILE="${DUCKDB_FILE:-/opt/airflow/data/warehouse/telegram.duckdb}"

mkdir -p "$BACKUP_ROOT/$TS"

# --- 1. Warehouse: consistent snapshot (не копируем живой файл напрямую!) ---
python - "$DUCKDB_FILE" "$BACKUP_ROOT/$TS/telegram.duckdb" <<'EOF'
import sys
import duckdb

src, dst = sys.argv[1], sys.argv[2]
# подключаемся к НОВОМУ файлу, источник ATTACH-им read_only:
# чтение согласованное, параллельно с писателем Airflow
conn = duckdb.connect(dst)
conn.execute(f"ATTACH '{src}' AS src_main (READ_ONLY)")
conn.execute("BEGIN TRANSACTION")
for (name,) in conn.execute(
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_catalog='src_main' AND table_schema='main' AND table_type='BASE TABLE'"
).fetchall():
    conn.execute(f"CREATE TABLE {name} AS SELECT * FROM src_main.main.{name}")
conn.execute("COMMIT")
conn.execute("DETACH src_main")
conn.close()
print("warehouse snapshot written:", dst)
EOF

# --- 2. Superset metadata: pg_dump (контейнер postgres доступен по сети tdp-net) ---
# пароль берём из URI метаданных Airflow (он всегда есть в окружении контейнера)
PGPASSWORD=$(python - <<'EOF'
import os
from urllib.parse import urlparse
uri = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
pwd = urlparse(uri).password or ""
print(pwd)
EOF
)
PGPASSWORD="$PGPASSWORD" pg_dump \
    -h postgres -U airflow -d superset > "$BACKUP_ROOT/$TS/superset_meta.sql"
echo "superset metadata dump written: $(wc -l < "$BACKUP_ROOT/$TS/superset_meta.sql") lines"

# --- 3. Проверка восстановления (spec: копия проверяется восстановлением) ---
python - "$BACKUP_ROOT/$TS/telegram.duckdb" <<'EOF'
import sys
import duckdb

path = sys.argv[1]
conn = duckdb.connect(path, read_only=True)
tables = conn.execute(
    "SELECT count(*) FROM information_schema.tables WHERE table_type='BASE TABLE'"
).fetchone()[0]
msgs = conn.execute("SELECT count(*) FROM messages").fetchone()[0]
marts = conn.execute(
    "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'mart%'"
).fetchone()[0]
conn.close()
assert tables >= 4 and msgs >= 1, "backup verification failed"
print(f"restore check OK: {tables} tables, messages={msgs}, marts={marts}")
EOF

echo "BACKUP_DIR=$BACKUP_ROOT/$TS"
