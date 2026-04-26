"""
PyFlink Fraud Detection Pipeline — Flink 1.18
KafkaSource (new API) + explicit Types.STRING() on all map() calls.
Returns "" (empty string) instead of None to avoid [B cast errors.
"""

import json
import logging
import os
from datetime import datetime, timezone

from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
    DeliveryGuarantee,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [flink] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",  "kafka:9092")
TOPIC_PAYMENTS  = os.getenv("TOPIC_PAYMENTS",   "transactions.payments")
TOPIC_CDC       = os.getenv("TOPIC_CDC",        "transactions.cdc")
TOPIC_DECISIONS = os.getenv("TOPIC_DECISIONS",  "fraud.decisions")
TOPIC_ALERTS    = os.getenv("TOPIC_ALERTS",     "fraud.alerts")
TOPIC_DLQ       = os.getenv("TOPIC_DLQ",        "fraud.dlq")

# ── In-memory feature store ───────────────────────────────────────────────────
_user_stats: dict = {}


def get_user_stats(user_id: str) -> dict:
    return _user_stats.get(user_id, {
        "tx_count_1h": 0, "amount_sum_1h": 0.0,
        "amount_sum_24h": 0.0, "tx_count_24h": 0,
        "avg_amount": 0.0, "n_countries": 0,
    })


def update_user_stats(user_id: str, amount: float, country: str):
    s = get_user_stats(user_id)
    s["tx_count_1h"]   += 1
    s["tx_count_24h"]  += 1
    s["amount_sum_1h"] += amount
    s["amount_sum_24h"]+= amount
    s["avg_amount"]     = s["amount_sum_24h"] / s["tx_count_24h"]
    s["n_countries"]    = s.get("n_countries", 0) + 1
    _user_stats[user_id] = s


def fraud_score(tx: dict, f: dict) -> tuple:
    score, rules = 0.0, []
    amt = float(tx.get("amount", 0))
    if amt > 9_000:
        score += 0.45; rules.append("HIGH_AMOUNT")
    if tx.get("country") in ["NG","CN","RU","UA"] and amt > 500:
        score += 0.30; rules.append("HIGH_RISK_COUNTRY")
    if str(tx.get("ip_address","")).startswith("185."):
        score += 0.25; rules.append("SUSPICIOUS_IP")
    if tx.get("card_type") == "amex" and amt > 5_000:
        score += 0.20; rules.append("AMEX_LARGE")
    if f.get("tx_count_1h", 0) > 10:
        score += 0.35; rules.append("VELOCITY_1H")
    avg = f.get("avg_amount", 0)
    if avg > 0 and amt > avg * 5:
        score += 0.20; rules.append("AMOUNT_SPIKE")
    if f.get("n_countries", 0) >= 3:
        score += 0.25; rules.append("MULTI_COUNTRY")
    return min(score, 1.0), rules


# ── Map functions — return str always, never None ─────────────────────────────

def normalize_payment(raw: str) -> str:
    try:
        tx = json.loads(raw)
        if "transaction_id" not in tx or "user_id" not in tx:
            return json.dumps({"__dlq__": True, "error": "MISSING_FIELDS", "raw": raw})
        return json.dumps(tx)
    except Exception as e:
        return json.dumps({"__dlq__": True, "error": str(e), "raw": raw})


def normalize_cdc(raw: str) -> str:
    try:
        event = json.loads(raw)
        tx = event.get("after")
        if not tx:
            return json.dumps({"__skip__": True})
        if "transaction_id" not in tx or "user_id" not in tx:
            return json.dumps({"__dlq__": True, "error": "MISSING_FIELDS", "raw": raw})
        return json.dumps(tx)
    except Exception as e:
        return json.dumps({"__dlq__": True, "error": str(e), "raw": raw})


def score_tx(raw: str) -> str:
    tx = json.loads(raw)
    if tx.get("__dlq__") or tx.get("__skip__"):
        return raw
    uid    = tx.get("user_id", "unknown")
    amt    = float(tx.get("amount", 0))
    country= tx.get("country", "")
    f      = get_user_stats(uid)
    update_user_stats(uid, amt, country)
    sc, rules = fraud_score(tx, f)
    return json.dumps({
        **tx,
        "fraud_score":     round(sc, 4),
        "is_fraud":        sc >= 0.5,
        "triggered_rules": rules,
        "features":        {"tx_count_1h": f["tx_count_1h"], "avg_amount": round(f["avg_amount"], 2)},
        "decided_at":      datetime.now(timezone.utc).isoformat(),
    })


def to_decision(raw: str) -> str:
    tx = json.loads(raw)
    if tx.get("__dlq__") or tx.get("__skip__") or "fraud_score" not in tx:
        return ""
    return json.dumps({
        "transaction_id":  tx.get("transaction_id"),
        "fraud_score":     tx.get("fraud_score"),
        "is_fraud":        tx.get("is_fraud"),
        "triggered_rules": tx.get("triggered_rules", []),
        "features":        tx.get("features", {}),
        "decided_at":      tx.get("decided_at"),
    })


def to_alert(raw: str) -> str:
    tx = json.loads(raw)
    if not tx.get("is_fraud"):
        return ""
    return json.dumps({
        "transaction_id": tx.get("transaction_id"),
        "user_id":        tx.get("user_id"),
        "fraud_score":    tx.get("fraud_score"),
        "alert_type":     (tx.get("triggered_rules") or ["UNKNOWN"])[0],
        "message":        f"Fraud detected score={tx.get('fraud_score', 0):.2%} rules={tx.get('triggered_rules',[])}",
        "created_at":     tx.get("decided_at"),
    })


def to_dlq(raw: str) -> str:
    tx = json.loads(raw)
    if not tx.get("__dlq__"):
        return ""
    return json.dumps({
        "topic":         "transactions",
        "payload":       tx.get("raw", ""),
        "error_type":    "PARSE_ERROR",
        "error_message": tx.get("error", ""),
    })


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def make_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def make_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    log.info("Starting Flink fraud pipeline (bootstrap=%s)", KAFKA_BOOTSTRAP)

    # Sources
    payment_src = env.from_source(
        make_source(TOPIC_PAYMENTS, "flink-payment"),
        WatermarkStrategy.no_watermarks(), "PaymentSource",
    )
    cdc_src = env.from_source(
        make_source(TOPIC_CDC, "flink-cdc"),
        WatermarkStrategy.no_watermarks(), "CDCSource",
    )

    # Normalize — Types.STRING() mandatory to avoid [B cast
    payment_norm = payment_src.map(normalize_payment, output_type=Types.STRING())
    cdc_norm     = cdc_src.map(normalize_cdc,         output_type=Types.STRING())
    unified      = payment_norm.union(cdc_norm)

    # Score
    scored = unified.map(score_tx, output_type=Types.STRING())

    # Sink: decisions (filter empty strings)
    (
        scored
        .map(to_decision, output_type=Types.STRING())
        .filter(lambda x: len(x) > 4)
        .sink_to(make_sink(TOPIC_DECISIONS))
        .name("DecisionSink")
    )

    # Sink: alerts
    (
        scored
        .map(to_alert, output_type=Types.STRING())
        .filter(lambda x: len(x) > 4)
        .sink_to(make_sink(TOPIC_ALERTS))
        .name("AlertSink")
    )

    # Sink: DLQ
    (
        unified
        .map(to_dlq, output_type=Types.STRING())
        .filter(lambda x: len(x) > 4)
        .sink_to(make_sink(TOPIC_DLQ))
        .name("DLQSink")
    )

    log.info("All sinks registered — executing pipeline…")
    env.execute("FraudDetectionPipeline")


if __name__ == "__main__":
    main()
