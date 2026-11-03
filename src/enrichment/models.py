"""Model registry: pinned revisions, task config, thresholds.

Модели обновляются ТОЛЬКО здесь явным изменением revision (spec §4:
запрещено автоматическое обновление до последней ревизии при запуске DAG).
Пороги эмоций версионированы отдельно от кода inference.
"""
from __future__ import annotations

INFERENCE_VERSION = "inf-v1"

MODEL_REGISTRY: dict[str, dict] = {
    "toxicity": {
        "task_name": "toxicity",
        "model_name": "cointegrated/rubert-tiny-toxicity",
        "revision": "5d37eff844868e243467e4c38898bad46c271af2",
        "multilabel": True,
        "classes": ["non-toxic", "insult", "obscenity", "threat", "dangerous"],
        # multilabel: метка = класс с максимальной оценкой; все оценки сохраняются
        "thresholds": {},
        "threshold_version": None,
        "max_length": 256,
    },
    "sentiment": {
        "task_name": "sentiment",
        "model_name": "cointegrated/rubert-tiny-sentiment-balanced",
        "revision": "e0679dd4460e48648f5d4d819ae0f505384f5a9d",
        "multilabel": False,
        # порядок классов модели (config id2label): 0 neutral, 1 negative, 2 positive
        "classes": ["neutral", "negative", "positive"],
        # sigmoid + argmax по правилам карточки модели, НЕ softmax
        "thresholds": {},
        "threshold_version": None,
        "max_length": 512,
    },
    "emotion": {
        "task_name": "emotion",
        "model_name": "cointegrated/rubert-tiny2-cedr-emotion-detection",
        "revision": "453ae93ca895c98cda29522c72b6fbc5a08067b9",
        "multilabel": True,
        "classes": ["no_emotion", "joy", "sadness", "surprise", "fear", "anger"],
        # порог по каждой эмоции — настраиваемый и версионируемый
        "thresholds": {"joy": 0.5, "sadness": 0.5, "surprise": 0.5, "fear": 0.5, "anger": 0.5},
        "threshold_version": "th-v1",
        "max_length": 256,
    },
}


def get_task_config(task_name: str) -> dict:
    if task_name not in MODEL_REGISTRY:
        raise KeyError(f"unknown task {task_name!r}; known: {list(MODEL_REGISTRY)}")
    return MODEL_REGISTRY[task_name]
