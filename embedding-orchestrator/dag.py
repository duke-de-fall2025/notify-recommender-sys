import boto3
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime
from decimal import Decimal

# ==========================================================
# CONFIG
# ==========================================================

DAG_ID = "ads_embedding_pipeline_notify_mwaa_safe"
AWS_CONN_ID = "aws_default"

PRODUCT_TABLE = "notify_products_test"        # id (N)
PURCHASE_TABLE = "notify_purchases_test"      # user_id (S)
USER_EMBED_TABLE = "notify_users_test"        # output

EMBED_DIM = 384
LAST_K = 5
USER_EMBED_DIM = EMBED_DIM * LAST_K  # 1920

# ==========================================================
# HELPERS
# ==========================================================

from decimal import Decimal

def to_decimal_vector(vec):
    return [Decimal(str(x)) for x in vec]


def aws_resource(service: str):
    hook = AwsBaseHook(aws_conn_id=AWS_CONN_ID)
    credentials = hook.get_credentials()
    region = hook.region_name or "us-east-1"

    return boto3.resource(
        service,
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token,
        region_name=region
    )

def convert_decimals(item):
    for k, v in item.items():
        if isinstance(v, Decimal):
            item[k] = float(v)
    return item

def scan_table(table_name):
    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(table_name)

    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))

    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))

    return [convert_decimals(i) for i in items]

def fake_embed(dim=EMBED_DIM):
    return [random.random() for _ in range(dim)]

# ==========================================================
# TASKS
# ==========================================================

# 1. EXTRACT + EMBED PRODUCTS (WRITE EMBEDDING BACK TO notify_products_test)
def extract_and_embed_products():
    dynamodb = aws_resource("dynamodb")
    product_table = dynamodb.Table(PRODUCT_TABLE)

    products = scan_table(PRODUCT_TABLE)

    for p in products:
        pid = int(p["id"])
        emb = fake_embed()

        product_table.update_item(
            Key={"id": pid},
            UpdateExpression="SET embedding = :e, updated_at = :u",
            ExpressionAttributeValues={
                ":e": to_decimal_vector(emb),   
                ":u": datetime.utcnow().isoformat()
            }
        )


# 2. EXTRACT PURCHASES (FOR DEBUG / FUTURE USE)
def extract_purchases():
    # This is not strictly needed by the DAG logic, but
    # you may keep it for debugging / future transformations
    _ = scan_table(PURCHASE_TABLE)

# 3. BUILD USER EMBEDDINGS = CONCAT OF LAST K PRODUCT EMBEDDINGS
def build_and_persist_user_embeddings():
    dynamodb = aws_resource("dynamodb")

    product_table = dynamodb.Table(PRODUCT_TABLE)
    user_table = dynamodb.Table(USER_EMBED_TABLE)

    purchases = scan_table(PURCHASE_TABLE)

    # Group by user
    user_map = {}
    for p in purchases:
        uid = p["user_id"]
        pid = int(p["id"])
        ts = p.get("purchase_ts", p.get("created_at", 0))
        user_map.setdefault(uid, []).append((pid, ts))

    # Process each user
    for user_id, rows in user_map.items():
        # Sort by time DESC
        rows.sort(key=lambda x: x[1], reverse=True)
        lastk = rows[:LAST_K]

        user_vector = []

        for pid, _ in lastk:
            resp = product_table.get_item(Key={"id": pid})
            item = resp.get("Item")

            if item and "embedding" in item:
                emb = item["embedding"]
            else:
                emb = [0.0] * EMBED_DIM

            user_vector.extend(emb)

        # Pad if fewer than K purchases
        while len(user_vector) < USER_EMBED_DIM:
            user_vector.extend([0.0] * EMBED_DIM)

        user_table.put_item(
            Item={
                "user_id": str(user_id),
                "embedding": to_decimal_vector(user_vector),
                "updated_at": datetime.utcnow().isoformat()
            }
        )


# ==========================================================
# DAG
# ==========================================================

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ads", "dynamodb", "mwaa-safe"]
):

    extract_and_embed_products_task = PythonOperator(
        task_id="extract_and_embed_products",
        python_callable=extract_and_embed_products
    )

    extract_purchases_task = PythonOperator(
        task_id="extract_purchases",
        python_callable=extract_purchases
    )

    build_and_persist_user_embeddings_task = PythonOperator(
        task_id="build_and_persist_user_embeddings",
        python_callable=build_and_persist_user_embeddings
    )

    extract_and_embed_products_task >> build_and_persist_user_embeddings_task
    extract_purchases_task >> build_and_persist_user_embeddings_task
