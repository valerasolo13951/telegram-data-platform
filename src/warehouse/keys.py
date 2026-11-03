"""Генерация стабильных ключей сущностей склада.

Все ключи детерминированы: одна логическая сущность всегда даёт один ключ,
поэтому повторные загрузки идемпотентны.
"""
from __future__ import annotations

import hashlib

SOURCE = "telegram"


def _digest(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()


def message_key(source: str, channel_id: str, message_id: int) -> str:
    """Стабильное кодирование source, channel_id, message_id."""
    return _digest(source, str(channel_id), str(message_id))


def content_hash(text: str | None) -> str:
    """Хеш нормализованного содержимого сообщения (пустой текст даёт константу)."""
    normalized = (text or "").strip()
    return _digest(normalized)


def link_id(message_key_: str, raw_url: str, position_start: int) -> str:
    """Стабильный ID одного вхождения ссылки в сообщении."""
    return _digest(message_key_, raw_url, str(position_start))


def enrichment_id(
    message_key_: str,
    task_name: str,
    model_name: str,
    model_version: str,
    inference_version: str,
    input_hash: str,
) -> str:
    """Стабильный ID одного результата обогащения (сообщение, задача, модель, версии, вход)."""
    return _digest(
        message_key_, task_name, model_name, model_version, inference_version, input_hash
    )
