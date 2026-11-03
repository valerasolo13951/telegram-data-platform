"""Per-model inference adapters (local, CPU, batch).

Each adapter is verified against the model card's recommended usage:
sigmoid (not softmax) for toxicity/sentiment/emotion, argmax label for
single-label tasks, per-label thresholds for multilabel emotions.

torch/transformers импортируются лениво (внутри load_model), чтобы
тесты чистых частей и CI работали без ML-зависимостей.
"""
from __future__ import annotations

import numpy as np

from .models import INFERENCE_VERSION, get_task_config

_label_cache: dict[str, list[str]] = {}
_model_cache: dict[str, tuple] = {}


def load_model(task_name: str, cache_dir: str | None = None):
    """Load tokenizer+model at the PINNED revision; cached per process."""
    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    cfg = get_task_config(task_name)
    key = f"{cfg['model_name']}@{cfg['revision']}"
    if key in _model_cache:
        return _model_cache[key]
    tokenizer = AutoTokenizer.from_pretrained(
        cfg["model_name"], revision=cfg["revision"], cache_dir=cache_dir
    )
    model = AutoModelForSequenceClassification.from_pretrained(
        cfg["model_name"], revision=cfg["revision"], cache_dir=cache_dir
    )
    model.eval()
    # порядок классов берём из конфига модели, сверяем с реестром
    labels = [model.config.id2label[i] for i in range(model.config.num_labels)]
    _label_cache[task_name] = labels
    _model_cache[key] = (tokenizer, model)
    return tokenizer, model


def predict_batch(task_name: str, texts: list[str], batch_size: int = 16) -> list[dict]:
    """Run inference on non-empty texts.

    Returns per text: {"scores": {label: prob}, "label": str|None}.
    All scores are finite floats in [0, 1] (sigmoid outputs).
    """
    import torch

    if not texts:
        return []
    cfg = get_task_config(task_name)
    tokenizer, model = load_model(task_name)
    model_labels = _label_cache[task_name]
    expected = set(cfg["classes"])
    if set(model_labels) != expected:
        raise RuntimeError(
            f"{task_name}: model classes {model_labels} != registry {cfg['classes']}"
        )

    results: list[dict] = []
    with torch.inference_mode():
        for i in range(0, len(texts), batch_size):
            chunk = [t if t else " " for t in texts[i : i + batch_size]]
            inputs = tokenizer(
                chunk,
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=cfg["max_length"],
            )
            logits = model(**inputs).logits
            probs = torch.sigmoid(logits).cpu().numpy().astype(float)
            for row in probs:
                scores = {label: float(p) for label, p in zip(model_labels, row)}
                results.append(_interpret(cfg, scores))
    return results


def _interpret(cfg: dict, scores: dict[str, float]) -> dict:
    """Apply per-task label rules; all class scores preserved."""
    if cfg["task_name"] == "sentiment":
        # sigmoid + выбор метки по максимуму (карточка модели), не softmax
        label = max(scores, key=scores.get)
        return {"scores": scores, "label": label}
    if cfg["task_name"] == "toxicity":
        # multilabel: основная метка по максимуму, включая non-toxic
        label = max(scores, key=scores.get)
        return {"scores": scores, "label": label}
    if cfg["task_name"] == "emotion":
        # эмоции по версионируемым порогам; no_emotion не считается эмоцией
        thresholds = cfg["thresholds"]
        hits = [e for e, t in thresholds.items() if scores.get(e, 0.0) >= t]
        label = ",".join(sorted(hits)) if hits else None
        return {"scores": scores, "label": label}
    raise RuntimeError(f"unknown task config {cfg['task_name']}")


def sanity_check_scores(scores: dict[str, float]) -> bool:
    return all(np.isfinite(v) and 0.0 <= v <= 1.0 for v in scores.values())
