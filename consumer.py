import json
import time
import psycopg2
from kafka import KafkaConsumer
from psycopg2 import OperationalError
from kafka.errors import NoBrokersAvailable

POSTGRES_CONFIG = dict(
    dbname="kafka_db",
    user="kafka_user",
    password="kafka_password",
    host="postgres",   # docker-compose service name
    port="5432",
)

KAFKA_BOOTSTRAP = "kafka:9092"


# -----------------------------------
# Wait for PostgreSQL
# -----------------------------------
def wait_for_postgres(max_retries=30, delay=2):
    for attempt in range(max_retries):
        try:
            print(f"📌 Connecting to PostgreSQL (attempt {attempt+1})...")
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            print("✅ PostgreSQL connected")
            return conn
        except OperationalError:
            print("❌ Postgres not ready, retrying...")
            time.sleep(delay)
    raise Exception("❌ Failed to connect to PostgreSQL after multiple retries")


# -----------------------------------
# Wait for Kafka Broker
# -----------------------------------
def wait_for_kafka(max_retries=30, delay=2):
    for attempt in range(max_retries):
        try:
            print(f"📌 Connecting to Kafka (attempt {attempt+1})...")
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


# -----------------------------------
# Connect with retries
# -----------------------------------
conn = wait_for_postgres()
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS clickstream (
    id SERIAL PRIMARY KEY,
    event_type TEXT,
    product_id INT,
    product_name TEXT,
    category TEXT,
    timestamp TIMESTAMP
)
""")
conn.commit()

consumer = wait_for_kafka()


print("🟢 Kafka → PostgreSQL consumer started")
print("📥 Waiting for messages...")


# -----------------------------------
# Consume messages forever
# -----------------------------------
for message in consumer:
    data = message.value
    cursor.execute(
        """
        INSERT INTO clickstream (event_type, product_id, product_name, category, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            data.get("event"),
            data.get("product_id"),
            data.get("product_name"),
            data.get("category"),
            data.get("timestamp")
        )
    )
    conn.commit()
    print("📌 Saved:", data)
