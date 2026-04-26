"""
ML Scorer
Loads trained model and provides fraud scoring.
Called by the FastAPI decision endpoint.
"""

import logging
import os
import pickle
from pathlib import Path

import numpy as np

log = logging.getLogger(__name__)
MODEL_PATH = os.getenv("MODEL_PATH", "/models/fraud_model.pkl")

_model = None


def load_model():
    global _model
    if _model is None:
        if not Path(MODEL_PATH).exists():
            log.warning("Model not found at %s — using rule-based fallback", MODEL_PATH)
            return None
        with open(MODEL_PATH, "rb") as f:
            _model = pickle.load(f)
        log.info("ML model loaded from %s", MODEL_PATH)
    return _model


def build_feature_vector(tx: dict, features: dict) -> np.ndarray:
    """Build feature vector from transaction + user features."""
    amount   = float(tx.get("amount", 0))
    country  = tx.get("country", "")
    card     = tx.get("card_type", "")
    ip       = tx.get("ip_address", "")

    return np.array([[
        amount,
        float(features.get("tx_count_1h", 0)),
        float(features.get("amount_sum_1h", 0)),
        float(features.get("avg_amount", 0)),
        float(features.get("distinct_countries_24h", 1)),
        1.0 if country in ["NG","CN","RU","UA"] else 0.0,
        1.0 if str(ip).startswith("185.") else 0.0,
        1.0 if card == "amex" else 0.0,
    ]])


def score(tx: dict, features: dict) -> tuple[float, str]:
    """
    Returns (fraud_score 0.0-1.0, model_version).
    Falls back to rule-based scoring if model unavailable.
    """
    model = load_model()

    if model is not None:
        X = build_feature_vector(tx, features)
        proba = model.predict_proba(X)[0, 1]
        return float(proba), "gbm-v1"

    # Rule-based fallback
    score_val = 0.0
    amount = float(tx.get("amount", 0))
    if amount > 9_000:             score_val += 0.45
    if tx.get("country") in ["NG","CN","RU","UA"] and amount > 500:
                                   score_val += 0.30
    if str(tx.get("ip_address","")).startswith("185."):
                                   score_val += 0.25
    if tx.get("card_type") == "amex" and amount > 5_000:
                                   score_val += 0.20
    if features.get("tx_count_1h", 0) > 10:
                                   score_val += 0.35
    return min(score_val, 1.0), "rule-based-v1"
