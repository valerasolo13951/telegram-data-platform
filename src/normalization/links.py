"""Извлечение и классификация URL из текста сообщения.

Позиции — индексы в Unicode code points исходной строки
(спецификация: message_links). Query-параметры не удаляются.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

EXTRACTION_VERSION = "links-v1"

_URL_RE = re.compile(r"(?:https?://|www\.)[^\s<>\"']+", re.IGNORECASE)

_TELEGRAM_HOSTS = {"t.me", "telegram.me", "telegram.dog", "telex.io"}
_INVITE_PATH_RE = re.compile(r"^/(\+[A-Za-z0-9_-]+|joinchat/\S+)$", re.IGNORECASE)
_CHANNEL_PATH_RE = re.compile(r"^/([A-Za-z0-9_]{4,})(/(\d+))?$")


@dataclass
class ExtractedLink:
    raw_url: str
    normalized_url: str
    domain: str
    link_type: str  # telegram | external | invite | unknown
    position_start: int
    position_end: int


def normalize_url(raw: str) -> str:
    """Привести scheme/host к нижнему регистру, path/query/fragment не трогать."""
    s = urlsplit(raw.strip())
    scheme = s.scheme.lower() or ("https" if raw.lower().startswith("www.") else "")
    netloc = s.netloc.lower()
    return urlunsplit((scheme, netloc, s.path, s.query, s.fragment))


def classify_url(raw: str) -> tuple[str, str]:
    """Вернуть (normalized_url, link_type) для исходного URL."""
    normalized = normalize_url(raw)
    s = urlsplit(normalized)
    host = (s.hostname or "").lower().removeprefix("www.")
    if not host:
        return normalized, "unknown"
    if host in _TELEGRAM_HOSTS:
        if _INVITE_PATH_RE.match(s.path or ""):
            return normalized, "invite"
        if _CHANNEL_PATH_RE.match(s.path or ""):
            return normalized, "telegram"
        return normalized, "telegram"
    return normalized, "external"


def domain_of(raw: str) -> str:
    host = (urlsplit(normalize_url(raw)).hostname or "").lower()
    return host.removeprefix("www.")


def extract_links(text: str | None) -> list[ExtractedLink]:
    """Извлечь все вхождения ссылок с позициями в code points.

    Сохраняются все вхождения, включая повторные URL. Конечная пунктуация
    срезается консервативно (только небалансные закрывающие скобки/кавычки).
    """
    if not text:
        return []
    out: list[ExtractedLink] = []
    for m in _URL_RE.finditer(text):
        raw = m.group(0).rstrip(".,;:!?)]}>'\"")
        raw = raw.lstrip("(")
        if not raw:
            continue
        start = m.start()
        normalized, link_type = classify_url(raw)
        out.append(
            ExtractedLink(
                raw_url=raw,
                normalized_url=normalized,
                domain=domain_of(raw),
                link_type=link_type,
                position_start=start,
                position_end=start + len(raw),
            )
        )
    return out
