# 🚀 Real-Time Fraud Detection & Scoring Platform

A **production-grade streaming platform** for fraud detection using  
**Kafka + PyFlink + Feature Store + ML + FastAPI + Observability**

---

## 📸 Architecture Overview

<p align="center">
  <img src="docs/architecture.png" alt="Fraud Detection Architecture" width="900"/>
</p>

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
