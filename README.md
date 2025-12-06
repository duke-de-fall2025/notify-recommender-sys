# 🔔 Notify: Real-Time Personalised Notification System

[![Python Template for IDS706](https://github.com/duke-de-fall2025/notify-recommender-sys/actions/workflows/main.yml/badge.svg)](https://github.com/duke-de-fall2025/notify-recommender-sys/actions/workflows/main.yml)

**Notify** is a production-grade, ML-powered real-time notification platform delivering hyper-personalised campaigns at scale.

**Team:** Pranshul, Sejal, Kedar, Shambhavi, Supriya

---

## 🎥 Demo Video

**📹 Watch the System in Action:**

👉 **[View Demo on Google Drive](https://drive.google.com/file/d/1UuUn7b9tgiD0I8TgcvihzxDXPbwOCWvR/view?usp=sharing)**

**📊 Presentation Slides:**

👉 **[View Slides (Demo Slides PDF)](./DemoSlides.pdf)**

---

### Problem & Solution

| Challenge | Solution |
|-----------|----------|
| Generic notifications | ML-driven personalization |
| Poor timing | Intelligent scheduling |
| Rule-based targeting | Real-time embeddings + ANN search |
| No feedback loop | Closed-loop learning system |

## Architecture at a Glance

```
Frontend (Web/App) → Kafka Streaming → DynamoDB
                              ↓
                     Airflow ML Pipeline
                     (Products → Purchases → Users)
                              ↓
                  Lambda Recommendations (ANN)
                              ↓
                EventBridge Scheduling & Delivery
                              ↓
                  User Interactions (Feedback Loop)
```

---

## 📊 System Components

### 1️⃣ Event Ingestion & Streaming

| Component | Role |
|-----------|------|
| Frontend | Web/Mobile user interactions |
| Kafka | Real-time event streaming |
| Topics | `purchase_history`, `notify_clickstream` |

### 2️⃣ Data Storage & Features

| Table | Purpose |
|-------|---------|
| `notify_users` | Core user profiles |
| `notify_user_features` | Behavioural features (avg order value, spend, categories, etc.) |
| `notify_products` | Product catalogue |
| `notify_product_features` | Sales metrics (revenue, avg price, etc.) |
| `notify_campaigns` | Campaign metadata & scheduling |
| `notify_user_product_matrix` | Interaction strength matrix |
| `notify_purchase_history` | Historical transactions |

**Storage:** DynamoDB (low-latency, serverless, auto-scaling)

### 3️⃣ ML Pipeline (MWAA/Airflow)

**Dependency Chain:**
```
Products → Product Embeddings
    ↓
Purchase Orders → Purchase Embeddings
    ↓
User Data → User Embeddings
(Independent) → Campaign Embeddings
```

**Features:** Hierarchical DAGs, delta updates, embeddings persisted in DynamoDB

### 4️⃣ Recommendation Engine (Lambda)

- **Input:** User + Campaign embeddings
- **Algorithm:** Cosine similarity + ANN search O(log n)
- **Output:** Top-10 personalised campaigns per user
- **Performance:** Real-time inference at scale

### 5️⃣ Scheduling & Delivery

- **Constraints:** Max 5 notifications/user/day, time windows, priority, fatigue control
- **Plan:** 24-hour optimised schedule per user
- **Delivery:** EventBridge triggers Lambda every minute
- **Feedback:** User interactions feed back into clickstream (closed-loop)

---

## 🛠️ Technology Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Python |
| Streaming | Kafka |
| Batch Ingestion | AWS Glue |
| Database | DynamoDB |
| Orchestration | Amazon MWAA (Airflow) |
| ML | BERT Embeddings, ANN Search |
| Compute | AWS Lambda, EC2 |
| Scheduling | Amazon EventBridge |

---

## 🔄 End-to-End Flow

1. Users interact → Kafka streams events
2. Glue ingests historical data → DynamoDB
3. Airflow generates embeddings (Products → Purchases → Users)
4. Lambda computes Top-10 recommendations via ANN
5. Scheduler enforces constraints (fatigue, priority, time windows)
6. EventBridge triggers delivery every minute
7. User responses captured → Feedback loop improves future recommendations

---

## 📁 Project Structure

```
notify-recommender-sys/
├── app.py                          # Main application
├── consumer.py                     # Kafka consumer
├── test_app.py                     # Tests
├── requirements.txt                # Dependencies
├── docker-compose.yml              # Docker setup
├── dockerfile.consumer
├── dockerfile.streamlit
│
├── batch-ingestion/                # AWS Glue jobs
│   ├── glue_notify_*.py            # Data ingestion pipelines
│   └── campaigns.csv               # Sample data
│
├── embedding-orchestrator/         # Airflow DAGs
│   ├── dag.py                      # Main embedding DAG
│   └── delta_update_dag.py         # Incremental updates
│
├── notif-recommendation-engine/    # Recommendation logic
│   ├── recommender.py
│   ├── cosine_similarity.py
│   ├── notification_user_mapping.py
│   ├── schedule_notifications.py
│   ├── products_eda.ipynb
│   ├── users_eda.ipynb
│   └── *.csv                       # Sample data
│
└── output/                         # Generated artifacts
    ├── recommendations.csv
    └── dynamodb_records.json
```

---

## 🚀 Getting Started

1. **AWS Setup:** Configure DynamoDB, Glue jobs, MWAA
2. **Kafka:** Set up Kafka brokers
3. **Deploy DAGs:** Upload to MWAA
4. **Deploy Lambdas:** Recommendation & scheduling functions
5. **Run Ingestion:** Execute Glue jobs
6. **Start Streaming:** Begin event processing
7. **Monitor:** CloudWatch + MWAA UI

---

## 📋 Requirements & Specifications

### 📌 Functional & Non-Functional Requirements

For comprehensive details on system requirements, performance metrics, scalability targets, SLAs, and design principles, see:

👉 **[Functional & Non-Functional Requirements](./Functional_and_nonFunctionRequirements.md)**

This document includes:
- **Functional Requirements:** 8 core system capabilities
- **Performance SLAs:** Latency, throughput, and availability targets
- **Scalability:** User capacity, event throughput, notification volume
- **Reliability:** Fault tolerance, data consistency, disaster recovery
- **Cost & Monitoring:** Budget targets, observability metrics
- **Design Principles:** Serverless-first, event-driven, cost-optimized

### 🔌 API Contracts & Integration Guide

For detailed API specifications, request/response schemas, error handling, and integration examples, see:

👉 **[API Contracts Documentation](./API_CONTRACTS.md)**

This document provides:
- **Recommendation Engine APIs:** Get top-10 personalised campaigns, user embedding lookup
- **Scheduling APIs:** Create optimised 24-hour schedules, EventBridge integration
- **Request/Response Schemas:** Complete JSON specifications with field descriptions
- **Error Handling:** Standardised error responses with HTTP status codes and retry policies
- **SLAs & Performance:** ≤200ms P95 latency, 99.95% availability, 99.9% success rate
- **Integration Examples:** End-to-end workflow examples and error handling patterns
- **Data Models:** Campaign, user embedding, and notification schedule object schemas

All APIs are implemented as **AWS Lambda functions** and are fully serverless, event-driven, and designed for high-throughput real-time processing.

---

## 📈 Business Impact

- ✅ Hyper-personalised engagement
- ✅ Higher conversion rates
- ✅ Reduced user fatigue
- ✅ Data-driven prioritisation

---

## 🏛️ Technical Excellence

- Cloud-native & serverless
- Real-time event processing
- Scalable ML inference
- Fault-tolerant orchestration
- Closed-loop learning

---

## 📞 Contact & Support

For questions or contributions, reach out to the team or open an issue.