"""Telegram collector based on snscrape's public web scraper (t.me/s/<name>).

The web interface gives: post url, date, text, outlinks. It does not provide
message ids (parsed from url), views, forwards or numeric channel ids — those
fields stay NULL downstream. Channel metadata (title/description) is parsed
from the channel landing page HTML because snscrape does not expose it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

import polars as pl
import requests
from bs4 import BeautifulSoup
from snscrape.base import ScraperException
from snscrape.modules.telegram import TelegramChannelScraper

COLLECTOR_VERSION = "snscrape-telegram-web-1"

_POST_URL_RE = re.compile(r"^https://t\.me/s/([^/]+)/(\d+)$")

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"
}


@dataclass
class ChannelUnavailable(Exception):
    """Channel does not exist or preview is not available (permanent)."""

    username: str
    detail: str = ""

    def __str__(self) -> str:
        return f"channel unavailable: {self.username} {self.detail}"


@dataclass
class TemporaryCollectError(Exception):
    """Transient network/source error; retry with backoff."""

    username: str
    detail: str = ""

    def __str__(self) -> str:
        return f"temporary collect error: {self.username} {self.detail}"


def parse_message_id(url: str) -> tuple[str, int] | None:
    """Extract (username, message_id) from https://t.me/s/<user>/<id>."""
    m = _POST_URL_RE.match(url)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def fetch_channel_info(username: str, timeout: int = 30) -> dict:
    """Fetch channel metadata from the landing page; None values when absent."""
    resp = requests.get(f"https://t.me/{username}", headers=UA_HEADERS, timeout=timeout)
    if resp.status_code in (404, 410):
        raise ChannelUnavailable(username, f"status {resp.status_code}")
    if resp.status_code != 200:
        raise TemporaryCollectError(username, f"status {resp.status_code}")
    soup = BeautifulSoup(resp.text, "lxml")

    def meta(prop: str) -> str | None:
        tag = soup.find("meta", attrs={"property": prop})
        return tag["content"] if tag and tag.get("content") else None

    title = meta("og:title") or soup.find("title")
    return {
        "username": username,
        "title": meta("og:title"),
        "description": meta("og:description"),
        "channel_url": f"https://t.me/{username}",
    }


def collect_channel(
    username: str,
    max_messages: int | None = None,
    stop_at_message_id: int | None = None,
    timeout: int = 30,
) -> pl.DataFrame:
    """Collect posts of a channel via the public web preview.

    Posts are yielded newest-first; collection stops after `max_messages`
    or when reaching `stop_at_message_id` (inclusive) — used both for
    backfill limits and incremental overlap windows.
    """
    scraper = TelegramChannelScraper(username)
    rows: list[dict] = []
    try:
        for post in scraper.get_items():
            parsed = parse_message_id(post.url)
            if parsed is None:
                continue
            _, message_id = parsed
            preview = post.linkPreview
            rows.append(
                {
                    "username": username,
                    "message_id": message_id,
                    "message_url": post.url,
                    "message_date": post.date,
                    "text": post.content,
                    "outlinks": post.outlinks,
                    "preview_url": preview.href if preview else None,
                    "preview_title": preview.title if preview else None,
                    "preview_site_name": preview.siteName if preview else None,
                }
            )
            if stop_at_message_id is not None and message_id <= stop_at_message_id:
                break
            if max_messages is not None and len(rows) >= max_messages:
                break
    except ScraperException as exc:
        msg = str(exc)
        if "status code 404" in msg or "status code 410" in msg:
            raise ChannelUnavailable(username, msg) from exc
        raise TemporaryCollectError(username, msg) from exc
    except requests.RequestException as exc:
        raise TemporaryCollectError(username, repr(exc)) from exc

    if not rows:
        return pl.DataFrame(schema=BRONZE_POST_SCHEMA)
    return pl.DataFrame(rows).with_columns(
        pl.col("message_date").dt.convert_time_zone("UTC").dt.replace_time_zone(None),
        pl.col("outlinks").fill_null([]),
    )


BRONZE_POST_SCHEMA = {
    "username": pl.String,
    "message_id": pl.Int64,
    "message_url": pl.String,
    "message_date": pl.Datetime("us"),
    "text": pl.String,
    "outlinks": pl.List(pl.String),
    "preview_url": pl.String,
    "preview_title": pl.String,
    "preview_site_name": pl.String,
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
