"""Преобразование строк Bronze в кадры Silver (channels, messages, message_links).

Дедупликация: в Bronze одно сообщение может попадать в несколько файлов
(перекрытие, повторные запуски). Для каждого message_key побеждает строка
с самой поздней датой сбора.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from warehouse.keys import content_hash as make_content_hash
from warehouse.keys import link_id as make_link_id
from warehouse.keys import message_key as make_message_key

from .links import EXTRACTION_VERSION, extract_links

SOURCE = "telegram"


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def dedup_messages(df: pl.DataFrame) -> pl.DataFrame:
    """Оставить одну строку на message_key — с самой поздней коллекцией.

    Вход — кадр silver-схемы; побеждает последняя версия (правка) сообщения.
    """
    if df.is_empty():
        return df
    latest = df.sort("last_collected_at").group_by("message_key").last()
    earliest = df.group_by("message_key").agg(pl.col("first_collected_at").min())
    return latest.drop("first_collected_at").join(earliest, on="message_key")


def build_messages(bronze_df: pl.DataFrame) -> pl.DataFrame:
    """Посты Bronze → строки silver `messages` (схема publisher.MESSAGE_COLUMNS)."""
    now = now_utc()
    df = bronze_df.with_columns(
        pl.struct(["username", "message_id"])
        .map_elements(
            lambda s: make_message_key(SOURCE, s["username"], s["message_id"]),
            return_dtype=pl.String,
        )
        .alias("message_key"),
        pl.col("username").alias("channel_id"),
        # fill_null: polars map_elements пропускает NULL-строки, а хеш пустого
        # текста должен быть детерминированным, не NULL
        pl.col("text").fill_null("").map_elements(make_content_hash, return_dtype=pl.String).alias("content_hash"),
        pl.lit(None, dtype=pl.Int64).alias("views_count"),
        pl.lit(None, dtype=pl.Int64).alias("forwards_count"),
        pl.lit(None, dtype=pl.Int64).alias("replies_count"),
        # веб-предпросмотр не даёт надёжного признака медиа: NULL, а не выдумка
        pl.lit(None, dtype=pl.Boolean).alias("has_media"),
        pl.lit(None, dtype=pl.String).alias("media_type"),
        pl.lit(False).alias("is_deleted"),
        pl.lit(None, dtype=pl.Datetime("us")).alias("edited_at"),
        pl.col("collected_at").alias("first_collected_at"),
        pl.col("collected_at").alias("last_collected_at"),
        pl.lit(now).alias("updated_at"),
    ).drop("outlinks", "preview_url", "preview_title", "preview_site_name")

    return df.select(
        "message_key", "channel_id", "message_id", "message_date", "edited_at",
        "text", "content_hash", "message_url", "views_count", "forwards_count",
        "replies_count", "has_media", "media_type", "is_deleted",
        "first_collected_at", "last_collected_at", "updated_at",
    )


def build_links(bronze_df: pl.DataFrame) -> pl.DataFrame:
    """Извлечь вхождения ссылок из текста сообщения с позициями."""
    now = now_utc()
    rows: list[dict] = []
    for row in bronze_df.iter_rows(named=True):
        mk = make_message_key(SOURCE, row["username"], row["message_id"])
        for link in extract_links(row["text"]):
            rows.append(
                {
                    "link_id": make_link_id(mk, link.raw_url, link.position_start),
                    "message_key": mk,
                    "raw_url": link.raw_url,
                    "normalized_url": link.normalized_url,
                    "domain": link.domain,
                    "link_type": link.link_type,
                    "position_start": link.position_start,
                    "position_end": link.position_end,
                    "extraction_version": EXTRACTION_VERSION,
                    "extracted_at": now,
                }
            )
    if not rows:
        return pl.DataFrame(schema=LINKS_SCHEMA)
    return pl.DataFrame(rows)


LINKS_SCHEMA = {
    "link_id": pl.String,
    "message_key": pl.String,
    "raw_url": pl.String,
    "normalized_url": pl.String,
    "domain": pl.String,
    "link_type": pl.String,
    "position_start": pl.Int64,
    "position_end": pl.Int64,
    "extraction_version": pl.String,
    "extracted_at": pl.Datetime("us"),
}


def build_channels(channel_info_files: list[Path]) -> pl.DataFrame:
    """Последние метаданные канала → строки silver `channels`."""
    now = now_utc()
    frames = [pl.read_parquet(f) for f in channel_info_files if f.exists()]
    if not frames:
        return pl.DataFrame()
    latest = pl.concat(frames).sort("collected_at").group_by("username").last()
    return latest.select(
        pl.col("username").alias("channel_id"),
        "username",
        "title",
        "description",
        "channel_url",
        pl.lit(True).alias("is_active"),
        pl.col("collected_at").alias("first_seen_at"),
        pl.col("collected_at").alias("last_seen_at"),
        pl.lit(None, dtype=pl.Datetime("us")).alias("last_message_at"),
        pl.col("collected_at").alias("created_at"),
        pl.lit(now).alias("updated_at"),
    )
