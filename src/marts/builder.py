"""Mart SQL definitions and builder.

Each mart is rebuilt wholesale (CREATE OR REPLACE) from Silver — витрины
пересчитываются без повторного сбора. Aggregates use only successful
enrichment results of the matching model version(s).
"""
from __future__ import annotations

from duckdb import DuckDBPyConnection

# Порог токсичности/эмоций в витринах (соответствует дефолтным порогам
# реестра моделей; версионируется через threshold_version в message_meta).
SCORE_THRESHOLD = 0.5

# Гранулярности витрин (используются тестами и Gold-проверками)
MART_GRAIN = {
    "mart_channel_activity_daily": ["activity_date", "channel_id"],
    "mart_message_sentiment_daily": ["activity_date", "channel_id", "model_version"],
    "mart_message_toxicity_daily": ["activity_date", "channel_id", "model_version"],
    "mart_message_emotions_daily": ["activity_date", "channel_id", "model_version"],
    "mart_channel_content_daily": ["activity_date", "channel_id"],
    "mart_domain_mentions_daily": ["activity_date", "domain"],
    "mart_channel_link_edges": ["period_start", "source_channel_id", "target_channel_id"],
    "mart_pipeline_health_daily": ["activity_date", "dag_id"],
}

MART_SQL: dict[str, str] = {
    # 1. Активность каналов по дням
    "mart_channel_activity_daily": """
    CREATE OR REPLACE TABLE mart_channel_activity_daily AS
    SELECT
        CAST(m.message_date AS DATE)                       AS activity_date,
        m.channel_id,
        count(*)                                            AS messages_count,
        count(*) FILTER (WHERE m.text IS NOT NULL AND length(m.text) > 0) AS messages_with_text_count,
        count(*) FILTER (WHERE m.has_media)                 AS messages_with_media_count,
        count(*) FILTER (WHERE EXISTS (
            SELECT 1 FROM message_links x WHERE x.message_key = m.message_key
        )) AS messages_with_links_count,
        avg(length(coalesce(m.text, '')))                   AS avg_text_length,
        sum(m.views_count)                                  AS views_sum,
        avg(m.views_count)                                  AS views_avg,
        sum(m.forwards_count)                               AS forwards_sum,
        max(m.message_date)                                 AS last_message_at
    FROM messages m
    WHERE m.message_date IS NOT NULL
    GROUP BY 1, 2
    """,
    # 2. Тональность (успешные результаты, по версиям моделей)
    "mart_message_sentiment_daily": f"""
    CREATE OR REPLACE TABLE mart_message_sentiment_daily AS
    WITH s AS (
        SELECT
            CAST(m.message_date AS DATE) AS activity_date,
            m.channel_id,
            mm.model_version,
            mm.label,
            json_extract_string(mm.scores_json, '$.positive')::DOUBLE AS positive_score
        FROM message_meta mm
        JOIN messages m USING (message_key)
        WHERE mm.task_name = 'sentiment'
          AND mm.status = 'success'
          AND m.message_date IS NOT NULL
    )
    SELECT
        activity_date, channel_id, model_version,
        count(*)                                                        AS messages_processed,
        count(*) FILTER (WHERE label = 'negative')                       AS negative_count,
        count(*) FILTER (WHERE label = 'neutral')                        AS neutral_count,
        count(*) FILTER (WHERE label = 'positive')                       AS positive_count,
        count(*) FILTER (WHERE label = 'negative')::DOUBLE / count(*)    AS negative_share,
        count(*) FILTER (WHERE label = 'neutral')::DOUBLE  / count(*)    AS neutral_share,
        count(*) FILTER (WHERE label = 'positive')::DOUBLE / count(*)    AS positive_share,
        avg(positive_score)                                             AS avg_sentiment_score
    FROM s
    GROUP BY 1, 2, 3
    """,
    # 3. Токсичность (multilabel: сообщение может входить в несколько категорий)
    "mart_message_toxicity_daily": f"""
    CREATE OR REPLACE TABLE mart_message_toxicity_daily AS
    WITH t AS (
        SELECT
            CAST(m.message_date AS DATE) AS activity_date,
            m.channel_id,
            mm.model_version,
            json_extract_string(mm.scores_json, '$.insult')::DOUBLE   AS insult,
            json_extract_string(mm.scores_json, '$.obscenity')::DOUBLE AS obscenity,
            json_extract_string(mm.scores_json, '$.threat')::DOUBLE   AS threat,
            json_extract_string(mm.scores_json, '$.dangerous')::DOUBLE AS dangerous,
            mm.label = 'non-toxic'                                    AS is_non_toxic
        FROM message_meta mm
        JOIN messages m USING (message_key)
        WHERE mm.task_name = 'toxicity'
          AND mm.status = 'success'
          AND m.message_date IS NOT NULL
    )
    SELECT
        activity_date, channel_id, model_version,
        count(*)                                  AS messages_processed,
        count(*) FILTER (WHERE insult   >= {SCORE_THRESHOLD}) AS insult_count,
        count(*) FILTER (WHERE obscenity >= {SCORE_THRESHOLD}) AS obscenity_count,
        count(*) FILTER (WHERE threat   >= {SCORE_THRESHOLD}) AS threat_count,
        count(*) FILTER (WHERE dangerous >= {SCORE_THRESHOLD}) AS dangerous_count,
        count(*) FILTER (WHERE is_non_toxic)      AS non_toxic_count,
        avg(insult)   AS avg_insult_score,
        avg(obscenity) AS avg_obscenity_score,
        avg(threat)   AS avg_threat_score,
        avg(dangerous) AS avg_dangerous_score
    FROM t
    GROUP BY 1, 2, 3
    """,
    # 4. Эмоции (пороги применены на этапе enrichment; label = список эмоций)
    "mart_message_emotions_daily": """
    CREATE OR REPLACE TABLE mart_message_emotions_daily AS
    WITH e AS (
        SELECT
            CAST(m.message_date AS DATE) AS activity_date,
            m.channel_id,
            mm.model_version,
            mm.label,
            json_extract_string(mm.scores_json, '$.joy')::DOUBLE      AS joy,
            json_extract_string(mm.scores_json, '$.sadness')::DOUBLE  AS sadness,
            json_extract_string(mm.scores_json, '$.surprise')::DOUBLE AS surprise,
            json_extract_string(mm.scores_json, '$.fear')::DOUBLE     AS fear,
            json_extract_string(mm.scores_json, '$.anger')::DOUBLE    AS anger
        FROM message_meta mm
        JOIN messages m USING (message_key)
        WHERE mm.task_name = 'emotion'
          AND mm.status = 'success'
          AND m.message_date IS NOT NULL
    )
    SELECT
        activity_date, channel_id, model_version,
        count(*)                                   AS messages_processed,
        count(*) FILTER (WHERE contains(label, 'joy'))      AS joy_count,
        count(*) FILTER (WHERE contains(label, 'sadness'))  AS sadness_count,
        count(*) FILTER (WHERE contains(label, 'surprise')) AS surprise_count,
        count(*) FILTER (WHERE contains(label, 'fear'))     AS fear_count,
        count(*) FILTER (WHERE contains(label, 'anger'))    AS anger_count,
        count(*) FILTER (WHERE label IS NULL)                AS no_emotion_count,
        avg(joy)      AS avg_joy_score,
        avg(sadness)  AS avg_sadness_score,
        avg(surprise) AS avg_surprise_score,
        avg(fear)     AS avg_fear_score,
        avg(anger)    AS avg_anger_score
    FROM e
    GROUP BY 1, 2, 3
    """,
    # 5. Контент каналов: покрытие обогащения, домены
    "mart_channel_content_daily": """
    CREATE OR REPLACE TABLE mart_channel_content_daily AS
    WITH enriched AS (
        SELECT DISTINCT message_key FROM message_meta WHERE status = 'success'
    ), links AS (
        SELECT message_key, count(*) AS link_count,
               count(DISTINCT domain) FILTER (WHERE link_type = 'external') AS unique_domains
        FROM message_links GROUP BY message_key
    )
    SELECT
        CAST(m.message_date AS DATE)                    AS activity_date,
        m.channel_id,
        count(*)                                         AS messages_count,
        avg(length(coalesce(m.text, '')))                AS avg_message_length,
        count(*) FILTER (WHERE m.has_media)              AS messages_with_media_count,
        count(*) FILTER (WHERE l.message_key IS NOT NULL) AS messages_with_links_count,
        sum(coalesce(l.unique_domains, 0))               AS unique_domains_count,
        count(*) FILTER (WHERE e.message_key IS NOT NULL)::DOUBLE / count(*) AS enrichment_coverage
    FROM messages m
    LEFT JOIN enriched e USING (message_key)
    LEFT JOIN links l USING (message_key)
    WHERE m.message_date IS NOT NULL
    GROUP BY 1, 2
    """,
    # 6. Домены: упоминания по дням
    "mart_domain_mentions_daily": """
    CREATE OR REPLACE TABLE mart_domain_mentions_daily AS
    SELECT
        CAST(m.message_date AS DATE)        AS activity_date,
        ml.domain,
        count(*)                             AS mentions_count,
        count(DISTINCT ml.message_key)       AS unique_messages_count,
        count(DISTINCT m.channel_id)         AS unique_channels_count,
        count(*) FILTER (WHERE ml.link_type = 'telegram') AS telegram_links_count,
        count(*) FILTER (WHERE ml.link_type = 'external') AS external_links_count
    FROM message_links ml
    JOIN messages m USING (message_key)
    WHERE ml.domain IS NOT NULL AND m.message_date IS NOT NULL
    GROUP BY 1, 2
    """,
    # 7. Рёбра между каналами (ссылки, однозначно ведущие на канал Telegram)
    "mart_channel_link_edges": """
    CREATE OR REPLACE TABLE mart_channel_link_edges AS
    WITH tg AS (
        SELECT
            ml.link_id,
            m.channel_id AS source_channel_id,
            lower(regexp_extract(ml.normalized_url, '^https?://[^/]+/([A-Za-z0-9_]{4,})(?:/\\d+)?$', 1)) AS target_channel_id
        FROM message_links ml
        JOIN messages m USING (message_key)
        WHERE ml.link_type = 'telegram'
          AND regexp_matches(ml.normalized_url, '^https?://[^/]+/[A-Za-z0-9_]{4,}(/\\d+)?$')
    )
    SELECT
        date_trunc('month', CAST(m.message_date AS DATE)) AS period_start,
        date_trunc('month', CAST(m.message_date AS DATE)) + INTERVAL 1 MONTH - INTERVAL 1 DAY AS period_end,
        tg.source_channel_id,
        tg.target_channel_id,
        count(*)                        AS link_count,
        count(DISTINCT ml.message_key)  AS unique_messages_count
    FROM tg
    JOIN message_links ml USING (link_id)
    JOIN messages m USING (message_key)
    WHERE tg.source_channel_id <> tg.target_channel_id
      AND m.message_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
    """,
}

MART_COLUMNS = {
    "mart_channel_activity_daily": [
        "activity_date", "channel_id", "messages_count", "messages_with_text_count",
        "messages_with_media_count", "messages_with_links_count", "avg_text_length",
        "views_sum", "views_avg", "forwards_sum", "last_message_at",
    ],
    "mart_message_sentiment_daily": [
        "activity_date", "channel_id", "model_version", "messages_processed",
        "negative_count", "neutral_count", "positive_count",
        "negative_share", "neutral_share", "positive_share", "avg_sentiment_score",
    ],
    "mart_message_toxicity_daily": [
        "activity_date", "channel_id", "model_version", "messages_processed",
        "insult_count", "obscenity_count", "threat_count", "dangerous_count",
        "non_toxic_count", "avg_insult_score", "avg_obscenity_score",
        "avg_threat_score", "avg_dangerous_score",
    ],
    "mart_message_emotions_daily": [
        "activity_date", "channel_id", "model_version", "messages_processed",
        "joy_count", "sadness_count", "surprise_count", "fear_count", "anger_count",
        "no_emotion_count", "avg_joy_score", "avg_sadness_score", "avg_surprise_score",
        "avg_fear_score", "avg_anger_score",
    ],
    "mart_channel_content_daily": [
        "activity_date", "channel_id", "messages_count", "avg_message_length",
        "messages_with_media_count", "messages_with_links_count",
        "unique_domains_count", "enrichment_coverage",
    ],
    "mart_domain_mentions_daily": [
        "activity_date", "domain", "mentions_count", "unique_messages_count",
        "unique_channels_count", "telegram_links_count", "external_links_count",
    ],
    "mart_channel_link_edges": [
        "period_start", "period_end", "source_channel_id", "target_channel_id",
        "link_count", "unique_messages_count",
    ],
    "mart_pipeline_health_daily": [
        "activity_date", "dag_id", "runs_count", "success_count", "failed_count",
        "avg_duration_seconds", "messages_collected", "messages_normalized",
        "messages_enriched", "last_success_at", "quality_failures_count",
    ],
}


def build_marts(conn: DuckDBPyConnection, exclude: set[str] | None = None) -> list[str]:
    """Rebuild all SQL-defined marts in one connection (single writer)."""
    built = []
    for name, sql in MART_SQL.items():
        if exclude and name in exclude:
            continue
        conn.execute(sql).fetchall()
        built.append(name)
    return built
