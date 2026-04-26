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

---

## ⚙️ Tech Stack

- 🐍 Python  
- ⚡ Apache Kafka  
- 🌊 PyFlink (stateful streaming)  
- 🧠 Feature Store (Redis + PostgreSQL)  
- 🤖 Machine Learning (Gradient Boosting)  
- ⚡ FastAPI (low-latency API)  
- 📊 Streamlit (dashboard)  
- 📈 Prometheus + Grafana (monitoring)  
- 🐳 Docker Compose  

---

## 🎯 Key Features

### 📡 Real-time Ingestion
- Payment API events (~5 tx/s)
- CDC ingestion (Debezium-style)
- Multi-topic Kafka architecture

---

### ⚡ Stream Processing (PyFlink)

- Event normalization  
- Stream union (payments + CDC)  
- Feature enrichment  
- Real-time fraud scoring  

#### Routing logic:
- `fraud.decisions` → all scored transactions  
- `fraud.alerts` → high-risk transactions  
- `fraud.dlq` → invalid or failed events  

---

### 🧠 Feature Store

#### 🔴 Online (Redis)
- Sub-millisecond access  
- Used for real-time inference  

#### 🐘 Offline (PostgreSQL)
- Historical features  
- Training datasets  

---

### 🤖 Machine Learning

- Model: Gradient Boosting  
- AUC: ~0.97  
- Features:
  - txn_count_1min  
  - avg_amount_5min  
  - velocity_score  
  - geo_anomaly  

---

### ⚡ FastAPI Decision API

#### Endpoint

---

## 🐳 Run Locally
```bash
git clone https://github.com/sory89/Fraud-Detection-Platform.git
cd Fraud-Detection-Platform
docker compose up -d

---

<img width="940" height="473" alt="image" src="https://github.com/user-attachments/assets/48eb05d0-ceb0-418c-9d15-fbd5519a2a0a" />

<img width="949" height="448" alt="image" src="https://github.com/user-attachments/assets/cb92748a-83ca-45f7-8e8e-537588d525e9" />

