import logging
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)
logging.getLogger("kafka.consumer").setLevel(logging.WARNING)
logging.getLogger("kafka.producer").setLevel(logging.WARNING)

import json
import time
import uuid
import boto3
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from botocore.exceptions import ClientError

# -----------------------------
# AWS DynamoDB Configuration
# -----------------------------
DYNAMO_TABLE = "notify_clickstream"
AWS_REGION = "us-east-1"

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DYNAMO_TABLE)

# -----------------------------
# Kafka Configuration
# -----------------------------
KAFKA_BOOTSTRAP = "kafka:9092"

# -----------------------------
# Wait for Kafka Broker
# -----------------------------
def wait_for_kafka(max_retries=30, delay=2):
    for attempt in range(max_retries):
        try:
            print(f"Connecting to Kafka (attempt {attempt + 1})...")
            consumer = KafkaConsumer(
                "clickstream",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="streamlit-consumer-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
            print("✅ Kafka broker connected")
            return consumer

        except NoBrokersAvailable:
            print("❌ Kafka not ready, retrying...")
            time.sleep(delay)

    raise Exception("❌ Failed to connect to Kafka after multiple retries")

# -----------------------------
# Verify DynamoDB Table Exists
# -----------------------------
try:
    table.load()
    print(f"✅ Using DynamoDB table: {DYNAMO_TABLE}")
except ClientError:
    raise Exception(f"❌ DynamoDB table '{DYNAMO_TABLE}' does NOT exist! Create it first.")

# -----------------------------
# Start Kafka Consumer
# -----------------------------
consumer = wait_for_kafka()

print("\n🟢 Kafka → DynamoDB consumer started")
print("📥 Waiting for messages...\n")

# -----------------------------
# Message Consumption Loop
# -----------------------------
try:
    for message in consumer:
        data = message.value

        item = {
            "id": str(uuid.uuid4()),
            "event_type": str(data.get("event")),
            "price": int(data.get("price")),
            "product_id": int(data.get("product_id")),
            "product_name": str(data.get("product_name")),
            "category": str(data.get("category")),
            "article_type": str(data.get("article_type")),
            "timestamp": str(data.get("timestamp")),
            "user_id": str(data.get("user_id", "U0020")),  
        }

        try:
            table.put_item(Item=item)
            print(f"📌 Saved to DynamoDB: {item}")

        except ClientError as e:
            print(f"❌ DynamoDB Error: {e.response['Error']['Message']}")

except KeyboardInterrupt:
    print("\n🛑 Stopping consumer (Ctrl+C detected)")

finally:
    consumer.close()
    print("👋 Consumer closed cleanly")
