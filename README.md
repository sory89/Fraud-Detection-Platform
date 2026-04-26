# 🚀 Real-Time Fraud Detection & Scoring Platform

A **production-grade streaming platform** for fraud detection using  
**Kafka + PyFlink + Feature Store + ML + FastAPI + Observability**

---

## 📸 Architecture Overview

Payment API Producer (5 tx/s)  ──┐
                                  ├──▶  Kafka (transactions.payments + transactions.cdc)
CDC Producer (Debezium-style)  ──┘         │
                                           ▼
                                PyFlink Streaming
                                normalize → score → route
                                     │          │         │
                                decisions    alerts     DLQ
                                     │
                                Feature Store
                                Redis (online) + PostgreSQL (offline)
                                     │
                                FastAPI /score  ──▶  < 50ms
                                     │
                                Streamlit Dashboard

---

## 🧩 Architecture

```text
Producers (Payment API + CDC)
        ↓
Kafka (multi-topic, 4 partitions)
   ├── transactions.payments
   └── transactions.cdc
        ↓
PyFlink Streaming
   normalize → union → feature enrichment → fraud scoring → routing
        ↓                    ↓                    ↓
  fraud.decisions      fraud.alerts          fraud.dlq
        ↓
Feature Store
   ├── Redis (online, < 1ms)
   └── PostgreSQL (offline)
        ↓
ML Model (GradientBoosting, AUC ~ 0.97)
        ↓
FastAPI Decision API  →  fraud score < 50ms p99
        ↓
Prometheus + Grafana  →  full observability
