"""DuckDB DDL for Silver tables.

Schema versions: bump SCHEMA_VERSION and add a migration when changing
tables incompatibly (spec §11.8).
"""
from __future__ import annotations

import duckdb

SCHEMA_VERSION = 1

CHANNELS_DDL = """
CREATE TABLE IF NOT EXISTS channels (
    channel_id      VARCHAR PRIMARY KEY,
    username        VARCHAR,
    title           VARCHAR,
    description     VARCHAR,
    channel_url     VARCHAR,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at   TIMESTAMP,
    last_seen_at    TIMESTAMP,
    last_message_at TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
)
"""

MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS messages (
    message_key        VARCHAR PRIMARY KEY,
    channel_id         VARCHAR NOT NULL,
    message_id         BIGINT NOT NULL,
    message_date       TIMESTAMP,
    edited_at          TIMESTAMP,
    text               VARCHAR,
    content_hash       VARCHAR,
    message_url        VARCHAR,
    views_count        BIGINT,
    forwards_count     BIGINT,
    replies_count      BIGINT,
    has_media          BOOLEAN,
    media_type         VARCHAR,
    is_deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    first_collected_at TIMESTAMP NOT NULL,
    last_collected_at  TIMESTAMP NOT NULL,
    updated_at         TIMESTAMP NOT NULL,
    UNIQUE (channel_id, message_id)
)
"""

MESSAGE_LINKS_DDL = """
CREATE TABLE IF NOT EXISTS message_links (
    link_id            VARCHAR PRIMARY KEY,
    message_key        VARCHAR NOT NULL,
    raw_url            VARCHAR NOT NULL,
    normalized_url     VARCHAR,
    domain             VARCHAR,
    link_type          VARCHAR NOT NULL,
    position_start     INTEGER NOT NULL,
    position_end       INTEGER NOT NULL,
    extraction_version VARCHAR NOT NULL,
    extracted_at       TIMESTAMP NOT NULL
)
"""

MESSAGE_META_DDL = """
CREATE TABLE IF NOT EXISTS message_meta (
    enrichment_id      VARCHAR PRIMARY KEY,
    message_key        VARCHAR NOT NULL,
    task_name          VARCHAR NOT NULL,
    label              VARCHAR,
    scores_json        JSON,
    confidence         DOUBLE,
    model_name         VARCHAR NOT NULL,
    model_version      VARCHAR NOT NULL,
    inference_version  VARCHAR NOT NULL,
    input_hash         VARCHAR NOT NULL,
    processed_at       TIMESTAMP NOT NULL,
    status             VARCHAR NOT NULL,
    error_message      VARCHAR,
    threshold_version  VARCHAR,
    UNIQUE (message_key, task_name, model_version, inference_version, input_hash)
)
"""


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("BEGIN")
    try:
        for ddl in (CHANNELS_DDL, MESSAGES_DDL, MESSAGE_LINKS_DDL, MESSAGE_META_DDL):
            conn.execute(ddl)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS warehouse_meta ("
            " key VARCHAR PRIMARY KEY, value VARCHAR, updated_at TIMESTAMP)"
        )
        from datetime import datetime, timezone

        conn.execute(
            "INSERT INTO warehouse_meta VALUES ('schema_version', ?, ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            [str(SCHEMA_VERSION), datetime.now(timezone.utc)],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
