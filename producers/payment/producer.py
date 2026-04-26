"""
Payment Producer
Simulates high-volume payment transactions → Kafka topic: transactions.payments
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [payment-producer] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC             = os.getenv("TOPIC", "transactions.payments")
INTERVAL          = float(os.getenv("INTERVAL_SECONDS", "0.5"))
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "5"))

MERCHANTS  = ["Amazon","Netflix","Apple","Uber","Airbnb","Spotify","Steam",
               "PayPal","Shopify","Stripe","Revolut","N26","Wise"]
COUNTRIES  = ["FR","US","GB","DE","ES","IT","JP","CN","BR","NG","RU","UA","PL","NL"]
CURRENCIES = ["EUR","USD","GBP","JPY","CHF","PLN"]
CARD_TYPES = ["visa","mastercard","amex","discover"]
CHANNELS   = ["mobile","web","pos","api"]

# Simulate a pool of users (some will be "fraudsters")
USER_POOL  = [f"user_{i:04d}" for i in range(1, 501)]
FRAUD_POOL = [f"user_{i:04d}" for i in range(1, 20)]   # 19 known high-risk users


def make_transaction(fraudulent_hint: bool = False) -> dict:
    tx_id = str(uuid.uuid4())
    user  = random.choice(FRAUD_POOL if fraudulent_hint else USER_POOL)
    roll  = random.random()

    amount = round(random.uniform(5.0, 9_999.0), 2)

    # Inject anomalies
    if roll < 0.04:
        amount = round(random.uniform(10_000.0, 99_999.0), 2)  # high amount
    if roll < 0.03:
        # Schema violation → will go to DLQ
        return {"transaction_id": tx_id, "amount": amount, "channel": "payment",
                "timestamp": datetime.now(timezone.utc).isoformat()}

    return {
        "transaction_id": tx_id,
        "user_id":        user,
        "amount":         amount,
        "currency":       random.choice(CURRENCIES),
        "merchant":       random.choice(MERCHANTS),
        "country":        random.choice(COUNTRIES),
        "card_type":      random.choice(CARD_TYPES),
        "card_last4":     str(random.randint(1000, 9999)),
        "ip_address":     (
            f"185.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}"
            if roll < 0.06 else
            f"{random.randint(1,254)}.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}"
        ),
        "channel":        "payment",
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


def build_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode() if k else None,
                acks="all", linger_ms=10, compression_type="gzip",
            )
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying in 3s…"); time.sleep(3)


def main():
    producer = build_producer()
    log.info("Payment producer started → %s (batch=%d, interval=%.1fs)", TOPIC, BATCH_SIZE, INTERVAL)
    sent = 0
    while True:
        for _ in range(BATCH_SIZE):
            fraudulent = random.random() < 0.08
            tx = make_transaction(fraudulent)
            key = tx.get("transaction_id", str(uuid.uuid4()))
            producer.send(TOPIC, key=key, value=tx)
            sent += 1
        producer.flush()
        if sent % 100 == 0:
            log.info("Sent %d transactions total", sent)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
