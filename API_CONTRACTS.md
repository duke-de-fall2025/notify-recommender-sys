# 🔌 Notify API Contracts

This document defines the API contracts for the Notify recommendation and scheduling system. All APIs are implemented as AWS Lambda functions and are serverless, event-driven, and designed for high-throughput real-time processing.

---

## 📋 Table of Contents

1. [Recommendation Engine APIs](#recommendation-engine-apis)
2. [Scheduling APIs](#scheduling-apis)
3. [Data Models & Schemas](#data-models--schemas)
4. [Error Handling](#error-handling)
5. [Examples](#examples)

---

## 🎯 Recommendation Engine APIs

### 1. Get Recommended Campaigns

**Purpose:** Compute top-10 personalised campaigns for a user based on embeddings and ANN search.

#### Request

```json
{
  "user_id": "user_12345",
  "num_recommendations": 10,
  "exclude_campaign_ids": ["camp_001", "camp_002"],
  "time_window": {
    "start": "2025-12-05T08:00:00Z",
    "end": "2025-12-05T22:00:00Z"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `user_id` | string | ✅ Yes | Unique user identifier |
| `num_recommendations` | integer | ❌ No | Number of campaigns to return (default: 10, max: 20) |
| `exclude_campaign_ids` | array[string] | ❌ No | Campaign IDs to exclude from recommendations |
| `time_window` | object | ❌ No | Time window for campaign delivery (ISO 8601) |

#### Response (Success 200)

```json
{
  "status": "success",
  "user_id": "user_12345",
  "recommendations": [
    {
      "rank": 1,
      "campaign_id": "camp_005",
      "similarity_score": 0.876,
      "campaign_name": "Black Friday Sale",
      "priority": "high",
      "estimated_ctr": 0.15,
      "time_window_start": "2025-12-05T08:00:00Z",
      "time_window_end": "2025-12-05T22:00:00Z"
    },
    {
      "rank": 2,
      "campaign_id": "camp_012",
      "similarity_score": 0.812,
      "campaign_name": "New Product Launch",
      "priority": "medium",
      "estimated_ctr": 0.12,
      "time_window_start": "2025-12-05T10:00:00Z",
      "time_window_end": "2025-12-05T20:00:00Z"
    }
  ],
  "processing_time_ms": 145,
  "timestamp": "2025-12-05T12:30:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | "success" or "error" |
| `user_id` | string | The requested user ID |
| `recommendations` | array[object] | Array of recommended campaigns |
| `recommendations[].rank` | integer | Ranking from 1 to N |
| `recommendations[].campaign_id` | string | Campaign identifier |
| `recommendations[].similarity_score` | float | Cosine similarity (0.0-1.0) |
| `recommendations[].campaign_name` | string | Human-readable campaign name |
| `recommendations[].priority` | string | Campaign priority: "low", "medium", "high" |
| `recommendations[].estimated_ctr` | float | Estimated click-through rate |
| `recommendations[].time_window_start` | string | Campaign start time (ISO 8601) |
| `recommendations[].time_window_end` | string | Campaign end time (ISO 8601) |
| `processing_time_ms` | integer | API response time in milliseconds |
| `timestamp` | string | API response timestamp (ISO 8601) |

#### Response (Error 4xx/5xx)

```json
{
  "status": "error",
  "error_code": "USER_NOT_FOUND",
  "error_message": "User user_12345 not found in user embeddings table",
  "request_id": "req_xyz789",
  "timestamp": "2025-12-05T12:30:00Z"
}
```

#### SLA

| Metric | Target |
|--------|--------|
| Latency (P95) | ≤ 200 ms |
| Latency (P99) | ≤ 400 ms |
| Success Rate | ≥ 99.9% |
| Availability | 99.95% |

#### Error Codes

| Code | HTTP | Description | Retry |
|------|------|-------------|-------|
| `USER_NOT_FOUND` | 404 | User ID not found in embeddings | No |
| `INVALID_REQUEST` | 400 | Missing/invalid required fields | No |
| `CAMPAIGN_EMBEDDINGS_UNAVAILABLE` | 503 | Campaign embeddings not ready | Yes |
| `INTERNAL_ERROR` | 500 | Internal server error | Yes |
| `TIMEOUT` | 504 | Request timeout | Yes |

---

### 2. User Embedding Lookup

**Purpose:** Fetch the latest user embedding vector.

#### Request

```json
{
  "user_id": "user_12345",
  "embedding_version": "v1"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `user_id` | string | ✅ Yes | Unique user identifier |
| `embedding_version` | string | ❌ No | Embedding version (default: latest) |

#### Response (Success 200)

```json
{
  "status": "success",
  "user_id": "user_12345",
  "embedding": [0.123, -0.456, 0.789, ...],
  "embedding_dimension": 384,
  "embedding_version": "v1",
  "created_at": "2025-12-05T10:00:00Z",
  "last_updated": "2025-12-05T11:30:00Z"
}
```

#### Error Codes

| Code | HTTP | Description |
|------|------|-------------|
| `USER_NOT_FOUND` | 404 | User ID not found |
| `EMBEDDING_NOT_AVAILABLE` | 503 | Embeddings not yet computed |

---

## 📅 Scheduling APIs

### 3. Create Notification Schedule

**Purpose:** Generate a 24-hour optimised notification schedule for a user.

#### Request

```json
{
  "user_id": "user_12345",
  "recommended_campaigns": [
    {
      "campaign_id": "camp_005",
      "similarity_score": 0.876,
      "priority": "high"
    },
    {
      "campaign_id": "camp_012",
      "similarity_score": 0.812,
      "priority": "medium"
    }
  ],
  "constraints": {
    "max_notifications_per_day": 5,
    "quiet_hours": {
      "start": "22:00",
      "end": "08:00"
    },
    "user_preferences": {
      "preferred_times": ["09:00", "14:00", "19:00"],
      "blackout_dates": ["2025-12-25"]
    }
  },
  "schedule_date": "2025-12-05"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `user_id` | string | ✅ Yes | User identifier |
| `recommended_campaigns` | array[object] | ✅ Yes | Array of recommended campaigns with scores |
| `constraints.max_notifications_per_day` | integer | ✅ Yes | Max notifications allowed (≤ 5) |
| `constraints.quiet_hours` | object | ✅ Yes | Do-not-disturb hours (HH:MM format) |
| `constraints.user_preferences` | object | ❌ No | User's preferred notification times |
| `schedule_date` | string | ✅ Yes | Schedule date (YYYY-MM-DD) |

#### Response (Success 200)

```json
{
  "status": "success",
  "user_id": "user_12345",
  "schedule_id": "sched_abc123",
  "schedule_date": "2025-12-05",
  "notifications": [
    {
      "sequence": 1,
      "campaign_id": "camp_005",
      "scheduled_time": "2025-12-05T09:00:00Z",
      "priority": "high",
      "estimated_ctr": 0.15,
      "notification_status": "pending"
    },
    {
      "sequence": 2,
      "campaign_id": "camp_012",
      "scheduled_time": "2025-12-05T14:00:00Z",
      "priority": "medium",
      "estimated_ctr": 0.12,
      "notification_status": "pending"
    }
  ],
  "total_scheduled": 2,
  "optimization_score": 0.87,
  "created_at": "2025-12-05T08:00:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | "success" or "error" |
| `user_id` | string | User identifier |
| `schedule_id` | string | Unique schedule identifier |
| `schedule_date` | string | Date of the schedule |
| `notifications` | array[object] | Array of scheduled notifications |
| `notifications[].sequence` | integer | Sequence number (1-5) |
| `notifications[].campaign_id` | string | Campaign ID |
| `notifications[].scheduled_time` | string | Scheduled delivery time (ISO 8601) |
| `notifications[].priority` | string | Campaign priority |
| `notifications[].estimated_ctr` | float | Expected click-through rate |
| `notifications[].notification_status` | string | "pending", "sent", "failed", "clicked" |
| `total_scheduled` | integer | Total notifications in schedule |
| `optimization_score` | float | Schedule quality score (0.0-1.0) |
| `created_at` | string | Schedule creation timestamp |

#### Error Codes

| Code | HTTP | Description | Retry |
|------|------|-------------|-------|
| `INVALID_CONSTRAINTS` | 400 | Constraint validation failed | No |
| `MAX_NOTIFICATIONS_EXCEEDED` | 400 | More campaigns than allowed | No |
| `SCHEDULING_FAILED` | 500 | Scheduling algorithm error | Yes |

---

### 4. Create EventBridge Schedule

**Purpose:** Create an EventBridge schedule rule for notification delivery.

#### Request

```json
{
  "schedule_id": "sched_abc123",
  "user_id": "user_12345",
  "notifications": [
    {
      "sequence": 1,
      "campaign_id": "camp_005",
      "scheduled_time": "2025-12-05T09:00:00Z"
    }
  ],
  "target_lambda_arn": "arn:aws:lambda:us-east-1:123456789:function:send-notification",
  "timezone": "America/New_York"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schedule_id` | string | ✅ Yes | Schedule identifier |
| `user_id` | string | ✅ Yes | User identifier |
| `notifications` | array[object] | ✅ Yes | Notifications to schedule |
| `target_lambda_arn` | string | ✅ Yes | Target Lambda function ARN |
| `timezone` | string | ✅ Yes | User's timezone (IANA format) |

#### Response (Success 201)

```json
{
  "status": "success",
  "schedule_id": "sched_abc123",
  "eventbridge_rules_created": 2,
  "rules": [
    {
      "rule_name": "notify-user_12345-camp_005-seq1",
      "rule_arn": "arn:aws:events:us-east-1:123456789:rule/notify-user_12345-camp_005-seq1",
      "state": "ENABLED",
      "schedule_expression": "at(2025-12-05T09:00:00)"
    }
  ],
  "created_at": "2025-12-05T08:00:00Z"
}
```

#### Error Codes

| Code | HTTP | Description |
|------|------|-------------|
| `EVENTBRIDGE_CREATE_FAILED` | 500 | EventBridge rule creation failed |
| `INVALID_LAMBDA_ARN` | 400 | Invalid Lambda ARN |

---

## 📊 Data Models & Schemas

### Campaign Object

```json
{
  "campaign_id": "camp_005",
  "campaign_name": "Black Friday Sale",
  "priority": "high",
  "category": "promotion",
  "time_window_start": "2025-12-05T08:00:00Z",
  "time_window_end": "2025-12-05T22:00:00Z",
  "estimated_ctr": 0.15,
  "created_at": "2025-12-01T10:00:00Z",
  "status": "active"
}
```

### User Embedding Object

```json
{
  "user_id": "user_12345",
  "embedding": [0.123, -0.456, 0.789, ...],
  "embedding_dimension": 384,
  "model_version": "bert-base",
  "created_at": "2025-12-05T10:00:00Z",
  "last_updated": "2025-12-05T11:30:00Z"
}
```

### Notification Schedule Object

```json
{
  "schedule_id": "sched_abc123",
  "user_id": "user_12345",
  "schedule_date": "2025-12-05",
  "notifications": [
    {
      "sequence": 1,
      "campaign_id": "camp_005",
      "scheduled_time": "2025-12-05T09:00:00Z",
      "notification_status": "pending",
      "delivery_time": null,
      "click_time": null
    }
  ],
  "total_scheduled": 2,
  "optimization_score": 0.87,
  "created_at": "2025-12-05T08:00:00Z"
}
```

---

## ⚠️ Error Handling

### Standard Error Response Format

All APIs return errors in this standardised format:

```json
{
  "status": "error",
  "error_code": "ERROR_CODE",
  "error_message": "Human-readable error message",
  "error_details": {
    "field": "field_name",
    "reason": "Specific validation error"
  },
  "request_id": "req_xyz789",
  "timestamp": "2025-12-05T12:30:00Z"
}
```

### HTTP Status Codes

| Code | Meaning | Retry |
|------|---------|-------|
| 200 | Success | - |
| 201 | Created | - |
| 400 | Bad Request | No |
| 404 | Not Found | No |
| 500 | Internal Server Error | Yes (exponential backoff) |
| 503 | Service Unavailable | Yes (exponential backoff) |
| 504 | Gateway Timeout | Yes (exponential backoff) |

### Retry Policy

- **Max Retries:** 3
- **Backoff Strategy:** Exponential (1s → 5s → 20s)
- **Idempotency:** All APIs are idempotent using request IDs

---

## 💡 Examples

### Example 1: Get Recommendations & Schedule

**Step 1: Get Recommendations**

```bash
curl -X POST https://lambda.amazonaws.com/get-recommended-campaigns \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_12345",
    "num_recommendations": 5
  }'
```

**Response:**
```json
{
  "status": "success",
  "recommendations": [
    {
      "rank": 1,
      "campaign_id": "camp_005",
      "similarity_score": 0.876,
      "priority": "high"
    }
  ],
  "processing_time_ms": 145
}
```

**Step 2: Create Schedule**

```bash
curl -X POST https://lambda.amazonaws.com/create-notification-schedule \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_12345",
    "recommended_campaigns": [...],
    "constraints": {
      "max_notifications_per_day": 5
    },
    "schedule_date": "2025-12-05"
  }'
```

**Response:**
```json
{
  "status": "success",
  "schedule_id": "sched_abc123",
  "total_scheduled": 2,
  "optimization_score": 0.87
}
```

### Example 2: Error Handling

**Request with Missing User:**

```bash
curl -X POST https://lambda.amazonaws.com/get-recommended-campaigns \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "invalid_user"
  }'
```

**Response (404):**
```json
{
  "status": "error",
  "error_code": "USER_NOT_FOUND",
  "error_message": "User invalid_user not found in embeddings",
  "request_id": "req_xyz789",
  "timestamp": "2025-12-05T12:30:00Z"
}
```

---

## 🔗 Related Documentation

- [Functional & Non-Functional Requirements](../Functional_and_nonFunctionRequirements.md)
- [README](../README.md)
- [Recommender.py](./recommender.py)
- [Schedule Notifications.py](./schedule_notifications.py)

---

## 📞 Support & Questions

For questions about API contracts, please open an issue or contact the team.

**Last Updated:** December 5, 2025  
**Version:** 1.0
