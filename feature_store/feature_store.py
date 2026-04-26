"""
Feature Store — consomme transactions.payments + fraud.decisions + fraud.alerts
→ Redis (online features) + PostgreSQL (offline sync)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import redis
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [feature-store] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",  "kafka:9092")
REDIS_URL       = os.getenv("REDIS_URL",         "redis://redis:6379/0")
DB_DSN          = os.getenv("DB_DSN",            "host=postgres port=5432 dbname=fraud user=fraud password=fraud")
TOPIC_PAYMENTS  = os.getenv("TOPIC_PAYMENTS",    "transactions.payments")
TOPIC_DECISIONS = os.getenv("TOPIC_DECISIONS",   "fraud.decisions")
TOPIC_ALERTS    = os.getenv("TOPIC_ALERTS",      "fraud.alerts")
FEATURE_TTL     = int(os.getenv("FEATURE_TTL",   "86400"))


def get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)


def get_pg():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    return conn


def build_consumer(topics: list, group_id: str) -> KafkaConsumer:
    while True:
        try:
            c = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode()),
                key_deserializer=lambda k: k.decode() if k else None,
            )
            log.info("Connected to Kafka, topics=%s", topics)
            return c
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying in 3s…")
            time.sleep(3)


# ── PostgreSQL helpers ────────────────────────────────────────────────────────

def upsert_transaction(conn, tx: dict):
    if "transaction_id" not in tx or "user_id" not in tx:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO transactions
                  (id, user_id, amount, currency, merchant, country,
                   card_type, card_last4, ip_address, channel, timestamp)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
            """, (
                tx["transaction_id"],
                tx.get("user_id"),
                tx.get("amount"),
                tx.get("currency"),
                tx.get("merchant"),
                tx.get("country"),
                tx.get("card_type"),
                tx.get("card_last4"),
                tx.get("ip_address"),
                tx.get("channel", "payment"),
                tx.get("timestamp"),
            ))
        log.info("TX inserted: %s", tx["transaction_id"][:8])
    except Exception as e:
        log.error("upsert_transaction error: %s", e)


def insert_decision(conn, d: dict):
    if not d.get("transaction_id"):
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO fraud_decisions
                  (transaction_id, fraud_score, is_fraud, triggered_rules, features, decided_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                d.get("transaction_id"),
                d.get("fraud_score"),
                d.get("is_fraud"),
                d.get("triggered_rules", []),
                json.dumps(d.get("features", {})),
                d.get("decided_at"),
            ))
        if d.get("is_fraud"):
            log.info("FRAUD decision: tx=%s score=%.3f", d["transaction_id"][:8], d.get("fraud_score", 0))
    except Exception as e:
        log.error("insert_decision error: %s", e)


def insert_alert(conn, a: dict):
    if not a.get("transaction_id"):
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alerts (transaction_id, user_id, fraud_score, alert_type, message)
                VALUES (%s,%s,%s,%s,%s)
            """, (
                a.get("transaction_id"),
                a.get("user_id"),
                a.get("fraud_score"),
                a.get("alert_type"),
                a.get("message"),
            ))
        log.info("Alert stored: tx=%s type=%s", a.get("transaction_id","?")[:8], a.get("alert_type"))
    except Exception as e:
        log.error("insert_alert error: %s", e)


def update_redis_features(r, user_id: str, amount: float, country: str):
    key     = f"features:{user_id}"
    cnt_key = f"countries:{user_id}"
    pipe    = r.pipeline()
    pipe.hincrbyfloat(key, "tx_count_1h",    1)
    pipe.hincrbyfloat(key, "tx_count_24h",   1)
    pipe.hincrbyfloat(key, "amount_sum_1h",  amount)
    pipe.hincrbyfloat(key, "amount_sum_24h", amount)
    pipe.hset(key, "last_tx_at", datetime.now(timezone.utc).isoformat())
    pipe.sadd(cnt_key, country)
    pipe.expire(key, FEATURE_TTL)
    pipe.expire(cnt_key, FEATURE_TTL)
    pipe.execute()

    tx_count   = int(float(r.hget(key, "tx_count_24h") or 1))
    amount_sum = float(r.hget(key, "amount_sum_24h") or 0)
    n_cnt      = r.scard(cnt_key)
    r.hset(key, mapping={
        "avg_amount":             round(amount_sum / max(tx_count, 1), 2),
        "distinct_countries_24h": n_cnt,
    })


def upsert_user_features(conn, r, user_id: str):
    key   = f"features:{user_id}"
    data  = r.hgetall(key)
    n_cnt = r.scard(f"countries:{user_id}")
    if not data:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO user_features
                  (user_id, tx_count_1h, tx_count_24h, amount_sum_1h,
                   amount_sum_24h, avg_amount, distinct_countries_24h, last_tx_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now())
                ON CONFLICT (user_id) DO UPDATE SET
                  tx_count_1h=EXCLUDED.tx_count_1h,
                  tx_count_24h=EXCLUDED.tx_count_24h,
                  amount_sum_1h=EXCLUDED.amount_sum_1h,
                  amount_sum_24h=EXCLUDED.amount_sum_24h,
                  avg_amount=EXCLUDED.avg_amount,
                  distinct_countries_24h=EXCLUDED.distinct_countries_24h,
                  last_tx_at=EXCLUDED.last_tx_at,
                  updated_at=now()
            """, (
                user_id,
                int(float(data.get("tx_count_1h", 0))),
                int(float(data.get("tx_count_24h", 0))),
                float(data.get("amount_sum_1h", 0)),
                float(data.get("amount_sum_24h", 0)),
                float(data.get("avg_amount", 0)),
                int(n_cnt),
                data.get("last_tx_at"),
            ))
    except Exception as e:
        log.error("upsert_user_features error: %s", e)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    r    = get_redis()
    conn = get_pg()

    consumer = build_consumer(
        [TOPIC_PAYMENTS, TOPIC_DECISIONS, TOPIC_ALERTS],
        "feature-store",
    )
    log.info("Feature store ready — consuming %s, %s, %s",
             TOPIC_PAYMENTS, TOPIC_DECISIONS, TOPIC_ALERTS)

    tx_count = 0
    for msg in consumer:
        try:
            payload = msg.value
            topic   = msg.topic

            if topic == TOPIC_PAYMENTS:
                if "user_id" in payload:
                    upsert_transaction(conn, payload)
                    update_redis_features(
                        r,
                        payload["user_id"],
                        float(payload.get("amount", 0)),
                        payload.get("country", ""),
                    )
                    tx_count += 1
                    if tx_count % 50 == 0:
                        upsert_user_features(conn, r, payload["user_id"])
                        log.info("Processed %d transactions", tx_count)

            elif topic == TOPIC_DECISIONS:
                insert_decision(conn, payload)
                r.setex(f"decision:{payload.get('transaction_id','?')}", 3600,
                        json.dumps(payload))

            elif topic == TOPIC_ALERTS:
                insert_alert(conn, payload)
                r.lpush("alerts:recent", json.dumps(payload))
                r.ltrim("alerts:recent", 0, 999)

        except Exception as e:
            log.exception("Error processing message: %s", e)


if __name__ == "__main__":
    main()
