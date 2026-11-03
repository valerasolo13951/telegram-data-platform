"""Stable key generation for warehouse entities.

All keys are deterministic: the same logical entity always maps to the same
key, which makes repeated loads idempotent.
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
    """Stable encoding of source, channel_id, message_id."""
    return _digest(source, str(channel_id), str(message_id))


def content_hash(text: str | None) -> str:
    """Hash of normalized message content (empty text hashes to a constant)."""
    normalized = (text or "").strip()
    return _digest(normalized)


def link_id(message_key_: str, raw_url: str, position_start: int) -> str:
    """Stable ID of one link occurrence within a message."""
    return _digest(message_key_, raw_url, str(position_start))


def enrichment_id(
    message_key_: str,
    task_name: str,
    model_name: str,
    model_version: str,
    inference_version: str,
    input_hash: str,
) -> str:
    """Stable ID of one enrichment result (message, task, model, versions, input)."""
    return _digest(
        message_key_, task_name, model_name, model_version, inference_version, input_hash
    )
