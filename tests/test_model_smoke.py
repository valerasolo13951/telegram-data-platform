"""Smoke tests of real models on a fixed example set (spec §4).

Marked `slow`: download pinned revisions on first run (cached afterwards).
Run: uv run pytest -m slow
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from enrichment import adapters, models  # noqa: E402

pytestmark = pytest.mark.slow

EXAMPLES = [
    "Какой замечательный день, спасибо!",
    "Ты полный идиот, заткнись!",
    "Завтра в 18:00 встреча, жду всех.",
    "",  # пустой текст обрабатывается отдельно, в inference не попадает
]


@pytest.mark.parametrize("task_name", ["toxicity", "sentiment", "emotion"])
def test_model_predicts_expected_classes(task_name, tmp_path):
    cfg = models.get_task_config(task_name)
    texts = [t for t in EXAMPLES if t]
    results = adapters.predict_batch(task_name, texts, batch_size=2)
    assert len(results) == len(texts)
    for r in results:
        assert set(r["scores"]) == set(cfg["classes"])
        assert adapters.sanity_check_scores(r["scores"])
        if task_name == "emotion":
            # эмоции по порогам: label может быть пустым (нет эмоций выше порога)
            assert r["label"] is None or all(
                e in cfg["classes"] for e in r["label"].split(",")
            )
        else:
            assert r["label"]


def test_sentiment_direction():
    results = adapters.predict_batch("sentiment", ["Какой замечательный день!", "Ты полный идиот, заткнись!"])
    labels = [r["label"] for r in results]
    assert labels[0] in ("positive", "neutral")
    assert labels[1] in ("negative", "neutral")


def test_toxicity_direction():
    results = adapters.predict_batch("toxicity", ["Ты полный идиот, заткнись!", "Завтра встреча в 18:00."])
    toxic_score = results[0]["scores"]
    clean_score = results[1]["scores"]
    assert toxic_score["insult"] + toxic_score["obscenity"] > clean_score["insult"] + clean_score["obscenity"]
