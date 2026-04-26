# 🚀 Real-Time Fraud Detection & Scoring Platform

Production-grade, low-latency streaming platform for real-time fraud detection.

---

## 🧱 Architecture

```text
Producers
  ├── Payment API (~5 tx/s)
  └── CDC Producer (Debezium-style)
          │
          ▼
       Kafka
  (transactions.payments, transactions.cdc)
          │
          ▼
     PyFlink Streaming
  (normalize → enrich → score)
      │        │        │
      ▼        ▼        ▼
  decisions   alerts    DLQ
      │
      ▼
   Feature Store
  ├── Redis (online)
  └── PostgreSQL (offline)
      │
      ▼
   FastAPI (/score)
   (< 50ms latency)
      │
      ▼
 Streamlit Dashboard
