"""
ML Model Training
Trains a gradient-boosted classifier on synthetic fraud data.
Saves model to /models/fraud_model.pkl
"""

import logging
import os
import pickle
from pathlib import Path

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ml-train] %(message)s")
log = logging.getLogger(__name__)

MODEL_PATH   = os.getenv("MODEL_PATH",   "/models/fraud_model.pkl")
SCALER_PATH  = os.getenv("SCALER_PATH",  "/models/scaler.pkl")
N_SAMPLES    = int(os.getenv("N_SAMPLES", "50000"))


def generate_synthetic_data(n: int = 50_000):
    """Generate synthetic transaction features with realistic fraud patterns."""
    rng = np.random.default_rng(42)

    # Features:
    # 0: amount               (0 → 100_000)
    # 1: tx_count_1h          (0 → 50)
    # 2: amount_sum_1h        (0 → 500_000)
    # 3: avg_amount           (0 → 20_000)
    # 4: distinct_countries   (1 → 10)
    # 5: is_high_risk_country (0 / 1)
    # 6: is_suspicious_ip     (0 / 1)
    # 7: is_amex              (0 / 1)

    X = np.column_stack([
        rng.exponential(scale=500, size=n),               # amount
        rng.poisson(lam=2, size=n),                       # tx_count_1h
        rng.exponential(scale=1000, size=n),              # amount_sum_1h
        rng.exponential(scale=400, size=n),               # avg_amount
        rng.integers(1, 6, size=n),                       # distinct_countries
        rng.binomial(1, 0.12, size=n),                    # high_risk_country
        rng.binomial(1, 0.05, size=n),                    # suspicious_ip
        rng.binomial(1, 0.15, size=n),                    # is_amex
    ])

    # Fraud label: combination of risk signals
    y_prob = (
        (X[:, 0] > 9_000).astype(float) * 0.5 +
        (X[:, 1] > 10).astype(float)    * 0.4 +
        X[:, 5]                          * 0.3 +
        X[:, 6]                          * 0.25 +
        (X[:, 7] & (X[:, 0] > 5_000)).astype(float) * 0.2 +
        (X[:, 4] >= 3).astype(float)     * 0.15 +
        rng.uniform(0, 0.15, size=n)   # noise
    )
    y = (y_prob > 0.55).astype(int)

    log.info("Generated %d samples — fraud rate=%.2f%%", n, 100 * y.mean())
    return X, y


def train():
    Path("/models").mkdir(parents=True, exist_ok=True)

    X, y = generate_synthetic_data(N_SAMPLES)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", GradientBoostingClassifier(
            n_estimators=200,
            learning_rate=0.08,
            max_depth=4,
            subsample=0.8,
            random_state=42,
        )),
    ])

    log.info("Training GradientBoostingClassifier on %d samples…", len(X_train))
    pipeline.fit(X_train, y_train)

    y_pred  = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]
    auc     = roc_auc_score(y_test, y_proba)

    log.info("AUC-ROC: %.4f", auc)
    log.info("\n%s", classification_report(y_test, y_pred, target_names=["legit","fraud"]))

    with open(MODEL_PATH, "wb") as f:
        pickle.dump(pipeline, f)

    log.info("Model saved → %s", MODEL_PATH)
    return pipeline


if __name__ == "__main__":
    train()
