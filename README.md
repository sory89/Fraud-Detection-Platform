# 🛡️ Real-Time Fraud Detection & Scoring Platform

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white">
  <img src="https://img.shields.io/badge/Apache%20Kafka-3.7-black?logo=apachekafka">
  <img src="https://img.shields.io/badge/PyFlink-1.18-orange?logo=apacheflink">
  <img src="https://img.shields.io/badge/FastAPI-0.110-green?logo=fastapi">
  <img src="https://img.shields.io/badge/Redis-7-red?logo=redis">
  <img src="https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql">
  <img src="https://img.shields.io/badge/Streamlit-1.39-red?logo=streamlit">
  <img src="https://img.shields.io/badge/Grafana-10.4-orange?logo=grafana">
  <img src="https://img.shields.io/badge/Docker-Compose-blue?logo=docker">
</p>

<p align="center">
  <b>End-to-end streaming fraud detection platform — from ingestion to decision in &lt; 50ms</b>
</p>

---

## 📐 Architecture

```
Producers
  ├── Payment API (~5 tx/s, 500 users)
  └── CDC Producer (Debezium-style)
          │
          ▼
       Kafka (KRaft, no Zookeeper)
  ┌─────────────────────────────────┐
  │  transactions.payments          │
  │  transactions.cdc               │
  └─────────────────────────────────┘
          │
          ▼
     PyFlink Streaming
  (normalize → union → enrich → score)
      │           │           │
      ▼           ▼           ▼
  fraud.      fraud.      fraud.
  decisions   alerts       dlq
      │
      ▼
   Feature Store
  ├── Redis    (online  — sub-millisecond)
  └── PostgreSQL (offline — audit & training)
          │
          ▼
   FastAPI /score  →  decision < 50ms p99
          │
          ▼
 Streamlit Dashboard  +  Grafana
```

---

## ⚙️ Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.11 |
| Message broker | Apache Kafka 3.7 (KRaft mode) |
| Stream processing | PyFlink 1.18 (Stateful Streaming) |
| Online feature store | Redis 7 (sub-millisecond) |
| Offline feature store | PostgreSQL 16 |
| ML model | Gradient Boosting (scikit-learn, AUC ~0.97) |
| Decision API | FastAPI + Uvicorn (4 workers) |
| Dashboard | Streamlit (5 tabs) |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker Compose |

---

## 🎯 Key Features

### 📡 Real-time Ingestion
- Payment API events (~5 tx/s, batch of 5 every 0.5s)
- CDC ingestion (Debezium-style envelope with `before`/`after`)
- Multi-topic Kafka architecture (4 partitions each)
- Automatic topic creation with retention policy

### ⚡ Stream Processing (PyFlink)
- Event normalization from two heterogeneous sources
- Stream union (payments + CDC → single pipeline)
- Real-time feature enrichment from in-memory state
- Rule-based + ML fraud scoring

**Routing logic:**
- `fraud.decisions` → all scored transactions
- `fraud.alerts` → transactions with score ≥ 0.5
- `fraud.dlq` → invalid or unparseable events

### 🧠 Feature Store

**🔴 Online (Redis)**
- Sub-millisecond feature access
- TTL-based expiry (24h)
- Features per user: `tx_count_1h`, `amount_sum_1h`, `avg_amount`, `distinct_countries_24h`
- Used for real-time inference in FastAPI

**🐘 Offline (PostgreSQL)**
- Full transaction history
- Fraud decisions audit log
- User feature snapshots
- Alert history + DLQ

### 🤖 Machine Learning

| Property | Value |
|----------|-------|
| Model | Gradient Boosting Classifier |
| Training samples | 50,000 synthetic transactions |
| AUC-ROC | ~0.97 |
| Threshold | 0.50 |
| Fallback | Rule-based scoring |

**Feature vector:**
- `amount` — transaction amount
- `tx_count_1h` — transactions in last hour
- `amount_sum_1h` — total amount in last hour
- `avg_amount` — user historical average
- `distinct_countries_24h` — unique countries in 24h
- `is_high_risk_country` — NG/CN/RU/UA flag
- `is_suspicious_ip` — 185.* IP range flag
- `is_amex` — Amex card flag

### 🔍 Fraud Detection Rules

| Rule | Condition | Score |
|------|-----------|-------|
| `HIGH_AMOUNT` | amount > $9,000 | +0.45 |
| `HIGH_RISK_COUNTRY` | NG/CN/RU/UA + amount > $500 | +0.30 |
| `SUSPICIOUS_IP` | IP starts with `185.*` | +0.25 |
| `AMEX_LARGE` | Amex + amount > $5,000 | +0.20 |
| `VELOCITY_1H` | > 10 transactions/hour | +0.35 |
| `AMOUNT_SPIKE` | amount > 5× user average | +0.20 |
| `MULTI_COUNTRY` | ≥ 3 countries in 24h | +0.25 |

> **Decision:** score ≥ 0.50 → `FRAUD` | score < 0.50 → `LEGIT`

### ⚡ FastAPI Decision API

**Score a transaction:**
```bash
POST /score
Content-Type: application/json

{
  "transaction_id": "tx-abc-123",
  "user_id":        "user_0042",
  "amount":         12500.00,
  "currency":       "EUR",
  "merchant":       "Amazon",
  "country":        "NG",
  "card_type":      "amex",
  "card_last4":     "4242",
  "ip_address":     "185.10.20.30"
}
```

**Response (< 50ms):**
```json
{
  "transaction_id": "tx-abc-123",
  "fraud_score":    0.8921,
  "is_fraud":       true,
  "threshold":      0.5,
  "model_version":  "gbm-v1",
  "decision_ms":    23,
  "features_used": {
    "tx_count_1h":           3,
    "amount_sum_1h":         14200.0,
    "avg_amount":            4733.33,
    "distinct_countries_24h": 2
  }
}
```

**Other endpoints:**
```bash
GET  /health                    # API + Redis health check
GET  /features/{user_id}        # Online features for a user
GET  /decision/{transaction_id} # Cached decision (Redis, 1h TTL)
GET  /alerts/recent?limit=50    # Recent fraud alerts
GET  /metrics                   # Prometheus metrics
GET  /docs                      # Swagger UI
```

---

## 📊 Dashboards

### Streamlit — http://localhost:8501
| Tab | Content |
|-----|---------|
| 📊 Overview | KPIs, transaction volume, fraud by rule/country |
| 🚨 Alerts | Alert log with filters, unacknowledged count |
| 💳 Transactions | Full table with fraud scores |
| ⚡ Score API | Live scoring form + recent decisions |
| 🔧 Pipeline | DB stats, feature store, architecture |

### Grafana — http://localhost:3000
- Transaction volume per minute (time series)
- Fraud detections per minute (time series)
- Fraud alerts by rule (bar chart)
- Top fraud countries (bar chart)
- KPI stats: total transactions, fraud rate, avg score

---

## 🚀 Run Locally

```bash
git clone https://github.com/sory89/Fraud-Detection-Platform.git
cd Fraud-Detection-Platform

# Windows (PowerShell)
$env:DOCKER_BUILDKIT=0
docker compose up --build

# Linux / macOS
docker compose up --build
```

> ⏳ First start takes ~3 minutes (Flink downloads Kafka connector JAR, ML trains 50k samples)

---

## 🌐 Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | — |
| FastAPI + Swagger | http://localhost:8000/docs | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| PostgreSQL | localhost:5432 | fraud / fraud |
| Redis | localhost:6379 | — |
| Kafka | localhost:9094 | — |

---

## 📁 Project Structure

```
fraud-platform/
├── producers/
│   ├── payment/          # Payment API simulator (5 tx/s, 500 users)
│   └── cdc/              # CDC Debezium-style producer
├── flink/
│   └── fraud_pipeline.py # PyFlink: normalize → union → score → route
├── feature_store/
│   └── feature_store.py  # Redis online + PostgreSQL offline sync
├── ml/
│   ├── train.py           # GradientBoosting training (50k samples)
│   └── scorer.py          # ML scoring + rule-based fallback
├── api/
│   └── main.py            # FastAPI /score < 50ms
├── streamlit/
│   └── app.py             # Dark dashboard, 5 tabs
├── monitoring/
│   ├── prometheus/        # Scrape config
│   └── grafana/           # Datasources + dashboards (auto-provisioned)
├── postgres/
│   └── init/              # Schema + migrations
└── docker-compose.yml     # Full stack (13 containers)
```

---

## 📈 Observability

**Prometheus metrics** (exposed at `/metrics`):

| Metric | Description |
|--------|-------------|
| `fraud_api_requests_total` | Request count by endpoint + status |
| `fraud_api_latency_seconds` | Latency histogram (p50/p95/p99) |
| `fraud_score_distribution` | Score distribution histogram |
| `fraud_decisions_total` | Decisions by `is_fraud` label |

---

## 📝 License

MIT — feel free to use, adapt, and share.

---

<p align="center">Built with ❤️ for learning and portfolio purposes</p>


<img width="940" height="473" alt="image" src="https://github.com/user-attachments/assets/48eb05d0-ceb0-418c-9d15-fbd5519a2a0a" />

<img width="949" height="448" alt="image" src="https://github.com/user-attachments/assets/cb92748a-83ca-45f7-8e8e-537588d525e9" />

