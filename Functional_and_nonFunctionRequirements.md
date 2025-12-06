# Notify: Functional & Non-Functional Requirements

## ✅ Functional Requirements

### 1. User Interaction & Event Capture
- Capture Orders, Clicks, Campaign views via frontend
- Stream to Kafka topics: `purchase_history`, `notify_clickstream`

### 2. Batch Data Ingestion
- Ingest via AWS Glue into DynamoDB (idempotent)
- Support: Users, Products, Campaigns, Purchase history, User–product matrix

### 3. Feature Engineering
- User features: Avg order value, total spend, top categories, days since signup, etc.
- Product features: Revenue, total sales, avg price, last sold date
- Maintain user–product behaviour matrix

### 4. ML Pipeline Orchestration
- MWAA orchestrates: Product → Purchase → User embeddings (hierarchical)
- Campaign embeddings run independently
- Delta updates without full recomputation
- Persist embeddings in DynamoDB

### 5. Real-Time Recommendation
- Cosine similarity + ANN search (O(log n))
- Return Top-10 campaigns per user
- AWS Lambda for inference

### 6. Scheduling & Optimisation
- 24-hour optimised plan per user
- Enforce: Max 5 notifications/day, time windows, priority, fatigue control
- Fully serverless (Lambda + EventBridge)

### 7. Notification Delivery
- Retrieve scheduled notifications
- Fetch user context & send via Lambda
- EventBridge triggers every minute
- Log: Delivery time, Campaign ID, User ID
- Track post-delivery interactions → Clickstream feedback

### 8. Monitoring & Validation
- Airflow UI for pipeline status
- CloudWatch logs for Lambda execution
- Validate: Embedding persistence, recommendation output, delivery execution

---

## ⚡ Non-Functional Requirements

### 1. Performance Requirements

#### 1.1 Recommendation Latency
| Metric | Target | SLA |
|--------|--------|-----|
| End-to-end response time (P95) | ≤ 200 ms | 95th percentile |
| Worst-case latency (P99) | ≤ 400 ms | 99th percentile |
| Measurement Point | Lambda `get_recommended_campaigns` | |

#### 1.2 Notification Delivery Latency
| Metric | Target |
|--------|--------|
| Time from scheduled → actual delivery | ≤ 5 seconds |
| SLA: On-time delivery | 99.5% within 10 seconds |

#### 1.3 Scheduling Computation Time
| Scenario | Target |
|----------|--------|
| 1M users schedule generation | ≤ 3 minutes |
| Peak recalculation (10M users) | ≤ 7 minutes |

#### 1.4 Streaming Ingestion Latency
| Metric | Target |
|--------|--------|
| Kafka → DynamoDB persistence | ≤ 2 seconds |
| SLA: Event persistence | 99.9% within 5 seconds |

---

### 2. Scalability Requirements

#### 2.1 User Scale
| Tier | Capacity |
|------|----------|
| Initial Scale | 100,000 users |
| Designed Capacity | 10 million users |
| Max Burst Load | 2 million concurrent users |

#### 2.2 Event Throughput
| Event Type | Normal Load | Peak Load |
|------------|-------------|-----------|
| Orders | 1,000/sec | 6,000/sec |
| Clicks | 6,000/sec | 25,000/sec |
| Campaign Views | 4,000/sec | 18,000/sec |
| **Total Kafka** | **11k/sec** | **49k/sec** |

#### 2.3 Notification Volume
| Scale | Daily Volume |
|-------|--------------|
| 1M users × 5 notifications/day | 5M notifications/day |
| 10M users × 5 notifications/day | 50M notifications/day |

---

### 3. Availability SLAs

| Component | SLA | Monthly Downtime |
|-----------|-----|------------------|
| Kafka cluster | 99.95% | ~22 min |
| AWS Lambda | 99.95% | ~22 min |
| DynamoDB | 99.999% | ~2.6 sec |
| EventBridge | 99.9% | ~43 min |
| Airflow (MWAA) | 99.9% | ~43 min |
| **Overall System** | **99.95%** | **~22 min** |

---

### 4. Reliability & Fault Tolerance

#### 4.1 Delivery Guarantees
- **Guarantee Type:** At-least-once delivery
- **Duplicate Prevention:** Idempotent notification IDs with 24-hour deduplication window
- **Retry Policy:**
  - Max 3 retries for Lambda failures
  - Exponential backoff: 1s → 5s → 20s

#### 4.2 Failure Isolation
- ML pipelines isolated per embedding type
- Campaign embedding failure ≠ user embedding failure
- Recommendation engine continues independent of campaign updates

#### 4.3 Data Loss Tolerance
- **Permissible loss:** < 0.001%
- **SLA:** No more than 1 lost event per 100,000 events

---

### 5. Throughput & Capacity Calculations

#### 5.1 DynamoDB Capacity (Peak)
```
Peak writes: 50k events/sec
Average item size: 1 KB
1 WCU = 1 KB write/sec

Required WCUs: 50,000
With 30% buffer: 65,000 WCUs provisioned
```

#### 5.2 Lambda Concurrency
```
Peak recommendation requests: 150,000/min = 2,500/sec
Avg Lambda runtime: 120 ms (0.12 sec)
Concurrency = 2,500 × 0.12 = 300

Reserved concurrency: 500 (for burst safety)
```

#### 5.3 ANN Search Throughput
```
Embedding dimension: 384
Campaign embeddings per shard: 200,000
ANN query time: 3–12 ms
QPS per node: 3,000

With 4 shards:
Total capacity: 12,000+ recommendation QPS
```

---

### 6. Security Requirements

| Parameter | Requirement |
|-----------|-------------|
| Data encryption (at rest) | AES-256 |
| Data encryption (in transit) | TLS 1.2+ |
| Password hashing | bcrypt / argon2 |
| IAM role scoping | Least privilege |
| Secrets rotation | Every 90 days |
| Audit logging | 100% coverage |
| **Critical vulns in prod** | Zero |
| **Security incident response** | < 15 minutes |

---

### 7. Data Consistency & Integrity

#### 7.1 Consistency Model
- **DynamoDB base tables:** Eventual consistency
- **Recommendation tables:** Strong consistency (embedding fetch)

#### 7.2 Duplicate Control
- **Deduplication window:** 24 hours
- **Target duplicate rate:** < 0.01%

#### 7.3 Idempotency
- All batch pipelines: Fully idempotent
- All stream operations: Fully idempotent
- Safe retry without data corruption

---

### 8. Observability & Monitoring SLAs

| Metric | SLA |
|--------|-----|
| Pipeline success rate | ≥ 99.9% |
| Lambda error rate | ≤ 0.1% |
| Kafka consumer lag | ≤ 10 seconds |
| Notification failure rate | ≤ 0.3% |
| Schedule generation success | 99.95% |
| **Logs retention** | 30 days |

**Logging:** All Lambdas → CloudWatch, All DAGs → Airflow UI

---

### 9. Cost Efficiency Targets

| Component | Monthly Budget |
|-----------|-----------------|
| Lambda | ≤ $1,200 |
| DynamoDB | ≤ $1,800 |
| Kafka | ≤ $900 |
| MWAA | ≤ $700 |
| EventBridge | ≤ $250 |
| **Total** | **≤ $4,800/month** |

**Cost-per-unit SLAs:**
- Cost per 1,000 notifications ≤ $0.015
- Cost per 1,000 recommendations ≤ $0.008

---

### 10. Maintainability & Deployment SLAs

| Metric | SLA |
|--------|-----|
| Mean Time To Recovery (MTTR) | ≤ 15 minutes |
| Bug fix deployment | ≤ 2 hours |
| Pipeline modification rollout | ≤ 1 business day |
| Campaign updates (no retraining) | ≤ 5 minutes |

---

### 11. Disaster Recovery & Backup

| Parameter | Value |
|-----------|-------|
| **RPO** (Recovery Point Objective) | 5 minutes |
| **RTO** (Recovery Time Objective) | 15 minutes |
| **Backup frequency** | Every 15 minutes |
| **Cross-region replication** | Enabled for campaign & schedule tables |

---

### 12. User Experience SLAs

| UX Metric | SLA |
|-----------|-----|
| App response time | < 300 ms |
| Notification relevance CTR | ≥ 12% |
| User opt-out due to fatigue | < 1.5% |
| Failed delivery rate | < 0.5% |

---

## 📊 Summary Table

| Category | Key SLA |
|----------|---------|
| **Latency** | ≤ 200 ms P95 for recommendations |
| **Availability** | 99.95% uptime |
| **Event Throughput** | 49k/sec peak, 50k WCUs DynamoDB |
| **Daily Volume** | Up to 50M notifications |
| **Delivery** | < 10 seconds (99.5% SLA) |
| **Failure Rate** | < 0.3% |
| **MTTR** | < 15 minutes |
| **Monthly Cost** | < $5,000 |
| **Data Loss** | < 0.001% |
| **RPO / RTO** | 5 min / 15 min |

---

## 🎯 Design Principles

1. **Serverless-First:** Minimize operational overhead
2. **Event-Driven:** Real-time processing via Kafka & EventBridge
3. **Scalability:** Horizontal scaling without bottlenecks
4. **Fault Isolation:** Component failures don't cascade
5. **Observability:** Comprehensive logging & monitoring
6. **Cost-Optimized:** Predictable costs with auto-scaling
7. **Data-Driven:** ML-powered personalization
8. **Closed-Loop:** Continuous feedback & improvement
