"""Check suites for Bronze and Silver layers (spec §7)."""
from __future__ import annotations

import json

import great_expectations.expectations as gxe

from .runner import Check, custom_check, gx_check

LINK_TYPES = ["telegram", "external", "invite", "unknown"]
TASK_NAMES = ["toxicity", "sentiment", "emotion"]
STATUSES = ["success", "failed", "skipped"]


# ---------- Bronze ----------

def bronze_checks() -> list[Check]:
    def dates_typed(df):
        import pandas as pd

        bad = int((~df["message_date"].apply(lambda v: pd.isna(v) or hasattr(v, "isoformat"))).sum())
        return len(df), bad, "message_date has non-datetime values"

    def file_has_rows(df):
        return max(len(df), 1), 0 if len(df) > 0 else 1, "bronze partition is empty"

    return [
        gx_check("bronze.username_not_null", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="username")),
        gx_check("bronze.message_id_not_null", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="message_id")),
        gx_check("bronze.run_id_present", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="run_id")),
        gx_check("bronze.collected_at_present", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="collected_at")),
        custom_check("bronze.message_date_type", "fail", dates_typed),
        custom_check("bronze.file_not_empty", "fail", file_has_rows),
        # доля пустых текстов отслеживается, но не ошибка
        gx_check("bronze.text_missing_share", "warn",
                 gxe.ExpectColumnValuesToNotBeNull(column="text", mostly=0.7)),
    ]


# ---------- Silver ----------

def channels_checks() -> list[Check]:
    return [
        gx_check("channels.id_unique", "fail",
                 gxe.ExpectColumnValuesToBeUnique(column="channel_id")),
        gx_check("channels.id_not_null", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="channel_id")),
        gx_check("channels.is_active_not_null", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="is_active")),
        gx_check("channels.url_format", "warn",
                 gxe.ExpectColumnValuesToMatchRegex(
                     column="channel_url", regex=r"^https://t\.me/[A-Za-z0-9_]+$")),
    ]


def messages_checks() -> list[Check]:
    counters = ["views_count", "forwards_count", "replies_count"]

    def counters_non_negative(df):
        import pandas as pd

        bad = 0
        for c in counters:
            if c in df.columns:
                bad += int((df[c].dropna() < 0).sum())
        return len(df), bad, "negative counters found"

    def no_orphan_channels(df):
        # заполняется вызывающей стороной через channels_ids (см. tasks.py)
        ids = _CHANNEL_IDS
        orphans = int((~df["channel_id"].isin(ids)).sum()) if ids is not None else 0
        return len(df), orphans, "messages reference unknown channels"

    def content_hash_present(df):
        bad = int(df["content_hash"].isna().sum())
        return len(df), bad, "content_hash is NULL"

    checks = [
        gx_check("messages.key_unique", "fail",
                 gxe.ExpectColumnValuesToBeUnique(column="message_key")),
        gx_check("messages.key_not_null", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="message_key")),
        gx_check("messages.channel_id_not_null", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="channel_id")),
        gx_check("messages.message_id_not_null", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="message_id")),
        custom_check("messages.counters_non_negative", "fail", counters_non_negative),
        custom_check("messages.no_orphan_channels", "fail", no_orphan_channels),
        custom_check("messages.content_hash_present", "fail", content_hash_present),
        # пустые тексты отслеживаются, но не блокируют (>10% пустых → WARN)
        gx_check("messages.text_empty_share", "warn",
                 gxe.ExpectColumnValuesToNotBeNull(column="text", mostly=0.9)),
    ]
    return checks


_CHANNEL_IDS: set | None = None


def set_channel_ids(ids: set) -> None:
    global _CHANNEL_IDS
    _CHANNEL_IDS = ids


def message_links_checks() -> list[Check]:
    def positions_ordered(df):
        bad = int((df["position_end"] <= df["position_start"]).sum())
        return len(df), bad, "position_end <= position_start"

    return [
        gx_check("links.link_id_unique", "fail",
                 gxe.ExpectColumnValuesToBeUnique(column="link_id")),
        gx_check("links.message_key_not_null", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="message_key")),
        gx_check("links.raw_url_not_null", "fail",
                 gxe.ExpectColumnValuesToNotBeNull(column="raw_url")),
        gx_check("links.link_type_in_set", "fail",
                 gxe.ExpectColumnValuesToBeInSet(column="link_type", value_set=LINK_TYPES)),
        gx_check("links.position_start_nonneg", "fail",
                 gxe.ExpectColumnValuesToBeBetween(column="position_start", min_value=0)),
        custom_check("links.positions_ordered", "fail", positions_ordered),
    ]


def message_meta_checks() -> list[Check]:
    def scores_valid(df):
        bad = 0
        for s in df.loc[df["status"] == "success", "scores_json"]:
            try:
                vals = json.loads(s).values() if s else []
                if any(not (0.0 <= float(v) <= 1.0) for v in vals):
                    bad += 1
            except Exception:  # noqa: BLE001
                bad += 1
        return len(df), bad, "scores_json invalid or outside [0,1]"

    def versions_filled_on_success(df):
        ok = df["status"] == "success"
        bad = int((df.loc[ok, ["model_version", "inference_version", "input_hash"]].isna().any(axis=1)).sum())
        return len(df), bad, "model/inference version missing on success"

    def no_result_duplicates(df):
        dup = int(df.duplicated(
            subset=["message_key", "task_name", "model_version", "inference_version", "input_hash"]
        ).sum())
        return len(df), dup, "logical result key duplicated"

    return [
        gx_check("meta.enrichment_id_unique", "fail",
                 gxe.ExpectColumnValuesToBeUnique(column="enrichment_id")),
        gx_check("meta.task_name_in_set", "fail",
                 gxe.ExpectColumnValuesToBeInSet(column="task_name", value_set=TASK_NAMES)),
        gx_check("meta.status_in_set", "fail",
                 gxe.ExpectColumnValuesToBeInSet(column="status", value_set=STATUSES)),
        custom_check("meta.scores_valid_range", "fail", scores_valid),
        custom_check("meta.versions_filled_on_success", "fail", versions_filled_on_success),
        custom_check("meta.no_result_duplicates", "fail", no_result_duplicates),
    ]
