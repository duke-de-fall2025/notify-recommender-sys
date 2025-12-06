# Notify: Real-Time Personalised Notification and Recommendation System

[![Python Template for IDS706](https://github.com/duke-de-fall2025/notify-recommender-sys/actions/workflows/main.yml/badge.svg)](https://github.com/duke-de-fall2025/notify-recommender-sys/actions/workflows/main.yml)

## 1. Project Overview and Objective

**Notify** is a real-time personalised notification and recommendation system designed to intelligently deliver the most relevant campaigns to users at the optimal time.

### Core Objective

To deliver highly personalised, optimised notifications using:
- Real-time user events
- Machine learning driven embeddings
- Automated scheduling and delivery

**Team:** Pranshul, Sejal, Kedar, Shambhavi, Supriya

---

## 2. Problem Statement and Motivation

Traditional notification systems face critical challenges:
- Users receive generic and poorly timed notifications
- No real-time personalisation
- Rule-based targeting does not scale
- No feedback-driven learning loop

### Goal

To build a scalable, ML-driven, real-time recommendation and notification system with optimised delivery and continuous learning.

---

## 3. Frontend and Real-Time Event Ingestion

### User Interaction

Users interact via a Web or App frontend, generating real-time events such as:
- Orders
- Clicks
- Campaign views

### Streaming Layer

These events are streamed through **Kafka** into:
- `purchase_history`
- `notify_clickstream`

This provides the real-time behavioural backbone for downstream ML and scheduling.

---

## 4. Batch Ingestion and Data Backbone

Historical and master data is ingested through **AWS Glue** into **Amazon DynamoDB**, forming a stable, low-latency historical backbone.

### Target Tables

| Table | Purpose |
|-------|---------|
| `notify_purchase_history` | Historical transactions |
| `notify_users` | Core user profiles |
| `notify_user_features` | Enriched behavioural features |
| `notify_products` | Product catalogue |
| `notify_product_features` | Product performance metrics |
| `notify_campaigns` | Campaign metadata |
| `notify_user_product_matrix` | User–product interaction matrix |

### Why DynamoDB

- Low-latency at scale
- Fully serverless
- Automatic scaling
- Highly suitable for real-time inference workloads

---

## 5. Core Data Model and Feature Stores

### User Tables

#### `notify_users`
Stores authentication and identity information.

#### `notify_user_features`
Stores derived behavioural features used for:
- User profiling
- Embedding generation
- Recommendation logic
- Engagement optimisation

Includes:
- Average order value
- Total orders
- Total spend
- Top product categories
- Days since signup
- Last purchase date

### Product Tables

#### `notify_products`
Master product catalogue with:
- Category details
- Pricing
- Visual attributes
- Seasonal information

#### `notify_product_features`
Derived performance metrics:
- Total sales
- Revenue
- Average price
- Last sold date

Used for:
- Product embeddings
- Campaign targeting
- Sales analytics

### Campaign Table

#### `notify_campaigns`
Stores:
- Campaign metadata
- Priority
- Time windows
- Notification templates
- Status tracking

Defines what gets sent, when, and how.

### User–Product Behaviour Matrix

#### `notify_user_product_matrix`
Captures:
- Purchase frequency
- Last interaction date
- Repeat purchase indicators

This is the core behavioural signal driving similarity and recommendations.

---

## 6. High-Level System Architecture

The system consists of four major layers:

1. **Frontend & Ingestion**
2. **Data Storage & Feature Stores**
3. **ML Pipeline & Embedding Generation**
4. **Recommendation, Scheduling & Delivery**

Each layer is loosely coupled but operationally integrated via AWS serverless services and Airflow orchestration.

---

## 7. Machine Learning Pipeline Orchestration (Airflow)

The entire embedding lifecycle is managed using **Amazon Managed Workflows for Apache Airflow (MWAA)**.

### Hierarchical Dependency Design

```
Products → Product Embeddings
Orders + Product Embeddings → Purchase Embeddings
Users + Purchase Embeddings → User Embeddings
Campaigns → Campaign Embeddings (runs independently)
```

### Key Design Principles

- **Purchase embeddings** act as the central convergence point
- **User embeddings** are built downstream of purchase behaviour
- **Campaign embeddings** run fully in parallel

This ensures:
- Temporal consistency
- High pipeline parallelism
- Independent campaign updates without retraining the entire system

### Live System Validation

MWAA UI confirms successful execution of:
- Product embedding pipeline
- Purchase embedding pipeline
- User embedding pipeline
- Campaign embedding pipeline

The end-to-end ML system is operational on AWS (not simulated).

---

## 8. Real-Time Recommendation Engine

### Lambda: `get_recommended_campaigns`

**Inputs:**
- User embeddings
- Campaign embeddings

**Processing:**
- Cosine similarity
- Approximate Nearest Neighbour (ANN) search for fast retrieval

**Output:**
- Top-10 personalised campaigns per user

### Performance Optimisation

- Naive approach: O(n)
- Optimised ANN search: O(log n)
- Enables real-time recommendation at scale

---

## 9. Scheduling and Optimisation Layer

This layer converts recommendations into actionable, optimised delivery plans.

### Key Lambdas

- `create_notification_schedule`
- `create_eventbridge_schedule`

### Constraints Handled

- User fatigue control
- Time window enforcement
- Campaign priority
- Engagement maximisation

### Output

A 24-hour optimised notification plan:
- Hard-capped to 5 notifications per user per day

---

## 10. Fully Serverless Notification Delivery System

### Lambda: `send_notification`

1. Pulls the scheduled campaign
2. Fetches user context
3. Sends the notification
4. User response flows back into the clickstream

### EventBridge Automation

Triggers notification delivery every minute.

**Features:**
- Fully serverless
- Automated
- Hands-free
- Supports continuous, rolling execution of delivery plans

### Result

A closed-loop learning system where user interactions directly improve future recommendations.

---

## 11. Technology Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Python |
| Real-Time Streaming | Kafka |
| Batch Ingestion | AWS Glue |
| Database | DynamoDB |
| Orchestration | Amazon MWAA (Airflow) |
| Machine Learning | BERT Embeddings, ANN Search |
| Compute | AWS Lambda, EC2 |
| Scheduling | Amazon EventBridge |

This stack enables:
- Real-time intelligence
- Serverless scalability
- Fault-tolerant orchestration
- Low-latency inference

---

## 12. End-to-End System Flow Summary

1. Users interact with the frontend
2. Events stream into Kafka in real time
3. Historical and master data flows into DynamoDB via Glue
4. Airflow orchestrates:
   - Product, purchase, user, and campaign embedding pipelines
5. The recommendation Lambda computes top-10 campaigns per user
6. The scheduling layer applies:
   - Time windows
   - Fatigue constraints
   - Priority rules
7. EventBridge triggers delivery every minute
8. Lambda sends notifications
9. User reactions are captured back into the system
10. The system continuously self-improves via feedback

---

## 13. Business and Technical Impact

### Business Value

- Hyper-personalised user engagement
- Higher campaign conversion rates
- Reduced user fatigue
- Data-driven prioritisation of campaigns

### Technical Excellence

- Fully cloud-native and serverless
- Real-time event processing
- Scalable ML inference
- Fault-tolerant orchestration
- Closed-loop AI system design

---

## ✅ Final Verdict

**Notify** is a production-grade, cloud-native, ML-powered real-time notification platform that integrates:

- Streaming data
- Feature engineering
- Embedding-based recommendation
- Intelligent scheduling
- Automated delivery
- Continuous feedback learning

It demonstrates end-to-end data engineering, MLOps, real-time systems, and serverless architecture at scale.

---

## Project Structure

```
notify-recommender-sys/
├── app.py                          # Main application
├── consumer.py                     # Kafka consumer for event streaming
├── test_app.py                     # Application tests
├── requirements.txt                # Python dependencies
├── docker-compose.yml              # Docker compose for local setup
├── dockerfile.consumer             # Dockerfile for consumer service
├── dockerfile.streamlit            # Dockerfile for Streamlit frontend
│
├── batch-ingestion/                # AWS Glue batch ingestion jobs
│   ├── glue_notify_users.py
│   ├── glue_notify_products.py
│   ├── glue_notify_user_features.py
│   ├── glue_notify_product_features.py
│   ├── glue_notify_purchase_history.py
│   ├── glue_notify_campaigns_s3_to_dynamodb.py
│   ├── glue_notify_users_to_dynamodb.py
│   ├── glue_notify_products_to_dynamodb.py
│   ├── glue_notify_purchases_to_dynamodb.py
│   ├── glue_notify_user_product_matrix.py
│   └── campaigns.csv               # Sample campaign data
│
├── embedding-orchestrator/         # Airflow DAGs for ML pipeline
│   ├── dag.py                      # Main embedding orchestration DAG
│   └── delta_update_dag.py         # Incremental update DAG
│
├── notif-recommendation-engine/    # Recommendation and scheduling logic
│   ├── recommender.py              # ML recommendation engine
│   ├── cosine_similarity.py        # Embedding similarity computations
│   ├── notification_user_mapping.py # User-notification mapping
│   ├── schedule_notifications.py   # Notification scheduling logic
│   ├── products_eda.ipynb          # Product data EDA
│   ├── users_eda.ipynb             # User data EDA
│   └── *.csv                       # Sample data for testing
│
└── output/                         # Generated artifacts
    ├── recommendations.csv         # Generated recommendations
    └── dynamodb_records.json       # DynamoDB records
```

---

## Getting Started

1. **Set up AWS Resources:** Configure DynamoDB tables, Glue jobs, and MWAA environment
2. **Configure Kafka:** Set up Kafka brokers for event streaming
3. **Deploy Airflow DAGs:** Upload orchestration DAGs to MWAA
4. **Deploy Lambda Functions:** Deploy recommendation and scheduling Lambdas
5. **Run Batch Ingestion:** Execute Glue jobs to populate DynamoDB
6. **Start Event Processing:** Begin streaming user events through Kafka
7. **Monitor System:** Use CloudWatch and MWAA UI for operational insights

---

## Contact & Support

For questions or contributions, reach out to the team or open an issue in the repository.