# 🛡️ Real-time Fraud Detection Platform (Production-grade)

<p align="center">
  <img src="https://img.shields.io/badge/Kafka-Multi--topic-black?logo=apachekafka">
  <img src="https://img.shields.io/badge/Flink-Stream%20Processing-orange?logo=apacheflink">
  <img src="https://img.shields.io/badge/Redis-Feature%20Store-red?logo=redis">
  <img src="https://img.shields.io/badge/scikit--learn-ML%20Model-blue?logo=scikitlearn">
  <img src="https://img.shields.io/badge/FastAPI-Decision%20API-green?logo=fastapi">
  <img src="https://img.shields.io/badge/Prometheus-Monitoring-orange?logo=prometheus">
  <img src="https://img.shields.io/badge/Grafana-Observability-yellow?logo=grafana">
  <img src="https://img.shields.io/badge/Docker-Compose-blue?logo=docker">
</p>

<p align="center">
  <b>End-to-end streaming fraud detection — ingestion → features → ML → decision < 50ms</b>
</p>

---

## 🧠 Architecture

```
Producers (Payment API + CDC)
        ↓
Kafka (multi-topic, 4 partitions)
   transactions.payments
   transactions.cdc
        ↓
PyFlink Streaming
   normalize → union → feature enrichment → fraud scoring → routing
        ↓                    ↓                    ↓
  fraud.decisions      fraud.alerts          fraud.dlq
        ↓
Feature Store
   Redis (online, < 1ms)  +  PostgreSQL (offline)
        ↓
ML Model (GradientBoosting, AUC ~ 0.97)
        ↓
FastAPI Decision API  →  fraud score < 50ms p99
        ↓
Prometheus + Grafana  →  full observability
```

---

## 🚀 Quick Start

```bash
git clone https://github.com/your-repo/fraud-platform.git
cd fraud-platform
docker compose up --build
```

> ⏳ First start takes ~3 min (Flink downloads JARs, ML trains 50k samples).

---

## 🌐 Services

| Service | URL | Description |
|---------|-----|-------------|
| **Fraud Decision API** | http://localhost:8000 | REST API — fraud scoring |
| **API Docs (Swagger)** | http://localhost:8000/docs | Interactive API documentation |
| **Prometheus** | http://localhost:9090 | Metrics scraping |
| **Grafana** | http://localhost:3000 | Dashboards (admin/admin) |
| **PostgreSQL** | localhost:5432 db=fraud | Offline feature store |
| **Redis** | localhost:6379 | Online feature store |
| **Kafka** | localhost:9094 | External Kafka access |

---

## 📁 Project Structure

```
fraud-platform/
├── producers/
│   ├── payment/
│   │   ├── producer.py          # Payment API simulator (5 tx/s)
│   │   └── Dockerfile
│   └── cdc/
│       ├── cdc_producer.py      # Debezium-style CDC events
│       └── Dockerfile
├── flink/
│   ├── fraud_pipeline.py        # PyFlink multi-topic streaming job
│   ├── submit.sh
│   └── Dockerfile
├── feature_store/
│   ├── feature_store.py         # Redis online + PostgreSQL offline sync
│   └── Dockerfile
├── ml/
│   ├── train.py                 # GradientBoosting training (50k samples)
│   ├── scorer.py                # ML scoring + rule-based fallback
│   └── Dockerfile
├── api/
│   ├── main.py                  # FastAPI — /score endpoint < 50ms
│   ├── requirements.txt
│   └── Dockerfile
├── monitoring/
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       └── provisioning/
│           ├── datasources/
│           └── dashboards/
├── postgres/
│   └── init/
│       └── 01_schema.sql
└── docker-compose.yml
```

---

## 🔍 Fraud Detection Rules

| Rule | Condition | Score contribution |
|------|-----------|--------------------|
| `HIGH_AMOUNT` | amount > $9,000 | +0.45 |
| `HIGH_RISK_COUNTRY` | NG/CN/RU/UA + amount > $500 | +0.30 |
| `SUSPICIOUS_IP` | IP starts with `185.*` | +0.25 |
| `AMEX_LARGE` | Amex card + amount > $5,000 | +0.20 |
| `VELOCITY_1H` | > 10 transactions in 1h | +0.35 |
| `AMOUNT_SPIKE` | amount > 5× user average | +0.20 |
| `MULTI_COUNTRY` | ≥ 3 countries in 24h | +0.25 |

> Decision threshold: **score ≥ 0.5 → fraud**

---

## ⚡ API Usage

### Score a transaction

```bash
curl -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "tx-abc-123",
    "user_id": "user_0042",
    "amount": 12500.00,
    "currency": "EUR",
    "merchant": "Amazon",
    "country": "NG",
    "card_type": "amex",
    "card_last4": "4242",
    "ip_address": "185.10.20.30"
  }'
```

**Response (< 50ms):**
```json
{
  "transaction_id": "tx-abc-123",
  "fraud_score": 0.8921,
  "is_fraud": true,
  "threshold": 0.5,
  "model_version": "gbm-v1",
  "decision_ms": 12,
  "features_used": {
    "tx_count_1h": 3,
    "amount_sum_1h": 14200.0,
    "avg_amount": 4733.33,
    "distinct_countries_24h": 2
  }
}
```

### Get cached decision
```bash
curl http://localhost:8000/decision/tx-abc-123
```

### Get user features
```bash
curl http://localhost:8000/features/user_0042
```

### Recent alerts
```bash
curl http://localhost:8000/alerts/recent?limit=20
```

---

## 🗄️ Database Schema

```sql
transactions     -- All raw transactions (from feature store sync)
fraud_decisions  -- ML decisions with score + features
user_features    -- Offline feature snapshots per user
alerts           -- Fraud alerts log
dlq_messages     -- Dead letter queue (invalid messages)
```

---

## 📊 Kafka Topics

| Topic | Description | Partitions |
|-------|-------------|------------|
| `transactions.payments` | Payment API events | 4 |
| `transactions.cdc` | CDC events from banking DB | 4 |
| `fraud.decisions` | ML fraud decisions | 4 |
| `fraud.alerts` | High-score fraud alerts | 4 |
| `fraud.dlq` | Invalid/unparseable messages | 1 |

---

## 📈 Observability

### Prometheus metrics (exposed at `/metrics`)

| Metric | Description |
|--------|-------------|
| `fraud_api_requests_total` | Total requests by endpoint + status |
| `fraud_api_latency_seconds` | Request latency histogram |
| `fraud_score_distribution` | Distribution of fraud scores |
| `fraud_decisions_total` | Decisions by fraud=true/false |

### Grafana dashboards
Navigate to http://localhost:3000 (admin/admin) and import dashboards from `monitoring/grafana/`.

---

## ⚙️ Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection |
| `DB_DSN` | `host=postgres...` | PostgreSQL DSN |
| `MODEL_PATH` | `/models/fraud_model.pkl` | ML model path |
| `FEATURE_TTL` | `86400` | Redis feature TTL (seconds) |
| `INTERVAL_SECONDS` | `0.5` | Producer interval |
| `BATCH_SIZE` | `5` | Transactions per batch |

---

## 🛠️ Tech Stack

| Component | Technology | Role |
|-----------|------------|------|
| Message broker | Apache Kafka 3.7 (KRaft) | Multi-topic streaming |
| Stream processing | Apache Flink 1.18 (PyFlink) | Normalize + score + route |
| Online feature store | Redis 7 | Sub-millisecond feature lookup |
| Offline feature store | PostgreSQL 16 | Analytics + audit |
| ML model | scikit-learn GradientBoosting | Fraud scoring |
| Decision API | FastAPI + Uvicorn (4 workers) | < 50ms p99 decisions |
| Monitoring | Prometheus + Grafana | Full observability |
| Containerization | Docker Compose | Local deployment |

---

## 📝 License

MIT
# Fraud-Detection-Platform
