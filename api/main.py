"""
Fraud Decision API
POST /score  → returns fraud score < 50ms using Redis feature store + ML model
"""

import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Optional

import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
from pydantic import BaseModel, Field

# Add ml/ to path
sys.path.insert(0, "/app/ml")
from scorer import score, load_model

logging.basicConfig(level=logging.INFO, format="%(asctime)s [api] %(message)s")
log = logging.getLogger(__name__)

REDIS_URL  = os.getenv("REDIS_URL",  "redis://redis:6379/0")
MODEL_PATH = os.getenv("MODEL_PATH", "/models/fraud_model.pkl")

# ── Prometheus metrics ────────────────────────────────────────────────────────
REQUEST_COUNT    = Counter("fraud_api_requests_total", "Total API requests", ["endpoint","status"])
REQUEST_LATENCY  = Histogram("fraud_api_latency_seconds", "API latency", ["endpoint"],
                              buckets=[.005,.01,.025,.05,.1,.25,.5,1,2.5])
FRAUD_SCORE_HIST = Histogram("fraud_score_distribution", "Fraud score distribution",
                              buckets=[0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1.0])
DECISIONS_COUNT  = Counter("fraud_decisions_total", "Fraud decisions", ["is_fraud"])

_redis: aioredis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis
    _redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    load_model()
    log.info("API ready — Redis connected, model loaded")
    yield
    if _redis:
        await _redis.aclose()


app = FastAPI(
    title="Fraud Decision API",
    description="Real-time fraud scoring endpoint (target < 50ms p99)",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Schemas ───────────────────────────────────────────────────────────────────

class TransactionRequest(BaseModel):
    transaction_id: str
    user_id:        str
    amount:         float = Field(gt=0)
    currency:       str   = "EUR"
    merchant:       Optional[str] = None
    country:        Optional[str] = None
    card_type:      Optional[str] = None
    card_last4:     Optional[str] = None
    ip_address:     Optional[str] = None


class FraudDecisionResponse(BaseModel):
    transaction_id: str
    fraud_score:    float
    is_fraud:       bool
    threshold:      float
    model_version:  str
    decision_ms:    int
    features_used:  dict


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/", tags=["health"])
async def root():
    return {"status": "ok", "service": "fraud-decision-api", "version": "1.0.0"}


@app.get("/health", tags=["health"])
async def health():
    try:
        await _redis.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {
        "status":    "healthy" if redis_ok else "degraded",
        "redis":     "ok" if redis_ok else "unreachable",
        "model":     "loaded" if load_model() else "rule-based-fallback",
    }


@app.post("/score", response_model=FraudDecisionResponse, tags=["scoring"])
async def fraud_score(tx: TransactionRequest, request: Request):
    t0 = time.perf_counter()

    # ── Fetch features from Redis ──────────────────────────────────────────────
    key      = f"features:{tx.user_id}"
    cnt_key  = f"countries:{tx.user_id}"

    pipe = _redis.pipeline()
    pipe.hgetall(key)
    pipe.scard(cnt_key)
    results = await pipe.execute()

    raw_features, n_countries = results
    features = {
        "tx_count_1h":           int(float(raw_features.get("tx_count_1h",    0))),
        "tx_count_24h":          int(float(raw_features.get("tx_count_24h",   0))),
        "amount_sum_1h":         float(raw_features.get("amount_sum_1h",   0)),
        "amount_sum_24h":        float(raw_features.get("amount_sum_24h",  0)),
        "avg_amount":            float(raw_features.get("avg_amount",      0)),
        "distinct_countries_24h": int(n_countries),
    }

    # ── ML scoring ────────────────────────────────────────────────────────────
    fraud_score_val, model_version = score(tx.model_dump(), features)
    threshold = 0.5
    is_fraud  = fraud_score_val >= threshold

    # ── Cache decision in Redis ───────────────────────────────────────────────
    decision = {
        "transaction_id": tx.transaction_id,
        "fraud_score":    round(fraud_score_val, 4),
        "is_fraud":       is_fraud,
        "model_version":  model_version,
        "decided_at":     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    await _redis.setex(f"decision:{tx.transaction_id}", 3600, json.dumps(decision))

    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    # ── Metrics ───────────────────────────────────────────────────────────────
    REQUEST_COUNT.labels(endpoint="/score", status="200").inc()
    REQUEST_LATENCY.labels(endpoint="/score").observe(time.perf_counter() - t0)
    FRAUD_SCORE_HIST.observe(fraud_score_val)
    DECISIONS_COUNT.labels(is_fraud=str(is_fraud)).inc()

    log.info("SCORE tx=%s score=%.3f fraud=%s ms=%d model=%s",
             tx.transaction_id[:8], fraud_score_val, is_fraud, elapsed_ms, model_version)

    return FraudDecisionResponse(
        transaction_id = tx.transaction_id,
        fraud_score    = round(fraud_score_val, 4),
        is_fraud       = is_fraud,
        threshold      = threshold,
        model_version  = model_version,
        decision_ms    = elapsed_ms,
        features_used  = features,
    )


@app.get("/decision/{transaction_id}", tags=["scoring"])
async def get_decision(transaction_id: str):
    """Retrieve cached decision from Redis."""
    cached = await _redis.get(f"decision:{transaction_id}")
    if not cached:
        raise HTTPException(404, "Decision not found or expired")
    return json.loads(cached)


@app.get("/features/{user_id}", tags=["features"])
async def get_user_features(user_id: str):
    """Retrieve online features for a user."""
    key     = f"features:{user_id}"
    cnt_key = f"countries:{user_id}"
    data    = await _redis.hgetall(key)
    n_cnt   = await _redis.scard(cnt_key)
    if not data:
        raise HTTPException(404, "No features found for this user")
    return {**data, "distinct_countries_24h": n_cnt}


@app.get("/alerts/recent", tags=["monitoring"])
async def recent_alerts(limit: int = 50):
    """Get recent fraud alerts from Redis."""
    raw = await _redis.lrange("alerts:recent", 0, limit - 1)
    return [json.loads(r) for r in raw]


@app.get("/metrics", tags=["monitoring"])
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=4)
