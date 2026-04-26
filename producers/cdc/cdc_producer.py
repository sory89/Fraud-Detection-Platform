"""
CDC Producer
Simulates Change Data Capture events from a banking database
→ Kafka topic: transactions.cdc
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [cdc-producer] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC           = os.getenv("TOPIC", "transactions.cdc")
INTERVAL        = float(os.getenv("INTERVAL_SECONDS", "2"))

OPERATIONS = ["INSERT", "UPDATE", "DELETE"]
BANKS      = ["BNP Paribas", "Société Générale", "Crédit Agricole", "HSBC", "Barclays"]


def make_cdc_event() -> dict:
    """Wraps a transaction in a Debezium-style CDC envelope."""
    op = random.choices(OPERATIONS, weights=[0.85, 0.12, 0.03])[0]
    tx_id = str(uuid.uuid4())

    payload = {
        "transaction_id": tx_id,
        "user_id":  f"user_{random.randint(1, 500):04d}",
        "amount":   round(random.uniform(10, 5000), 2),
        "currency": random.choice(["EUR", "USD", "GBP"]),
        "merchant": random.choice(BANKS),
        "country":  random.choice(["FR", "GB", "DE", "US"]),
        "card_type": random.choice(["visa", "mastercard"]),
        "card_last4": str(random.randint(1000, 9999)),
        "ip_address": f"{random.randint(1,254)}.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}",
        "channel":  "cdc",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "schema":  "banking.transactions",
        "op":      op,           # I = insert, U = update, D = delete
        "ts_ms":   int(time.time() * 1000),
        "source":  {"db": "banking_core", "table": "transactions"},
        "before":  None if op == "INSERT" else payload,
        "after":   payload if op != "DELETE" else None,
    }


def build_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode() if k else None,
                acks="all",
            )
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying in 3s…"); time.sleep(3)


def main():
    producer = build_producer()
    log.info("CDC producer started → %s", TOPIC)
    while True:
        event = make_cdc_event()
        after = event.get("after") or {}
        key = after.get("transaction_id", str(uuid.uuid4()))
        if event["op"] != "DELETE":   # only forward inserts & updates
            producer.send(TOPIC, key=key, value=event)
            log.info("CDC %s tx=%s", event["op"], key[:8])
        producer.flush()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
