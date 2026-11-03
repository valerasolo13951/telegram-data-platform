"""Реестр каналов: config/channels.csv с колонками username,is_active."""
from __future__ import annotations

import csv
from pathlib import Path


def load_channel_registry(path: str | Path, active_only: bool = True) -> list[dict]:
    rows: list[dict] = []
    with open(path, newline="", encoding="utf-8") as fh:
        for raw in csv.DictReader(fh):
            username = (raw.get("username") or "").strip().lstrip("@")
            if not username:
                continue
            is_active = (raw.get("is_active") or "true").strip().lower() in ("1", "true", "yes")
            if active_only and not is_active:
                continue
            rows.append({"username": username, "is_active": is_active})
    if not rows:
        raise ValueError(f"реестр каналов пуст: {path}")
    return rows
