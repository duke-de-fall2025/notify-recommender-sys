import boto3
import random
import logging
from airflow import DAG
from decimal import Decimal
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Attr
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# ==========================================================
# GLOBAL LOGGING
# ==========================================================

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ==========================================================
# CONFIG
# ==========================================================

DAG_ID = "ads_embedding_pipeline_notify"
AWS_CONN_ID = "aws_default"

PRODUCT_TABLE = "notify_products"
PURCHASE_TABLE = "notify_purchase_history"
USER_SOURCE_TABLE = "notify_users"
CAMPAIGN_SOURCE_TABLE = "notify_campaigns"

USER_EMBED_TABLE = "notify_user_embeddings_v2"
PURCHASE_EMBED_TABLE = "notify_purchase_embeddings_v2"
CAMPAIGN_EMBED_TABLE = "notify_campaign_embeddings_v2"

EMBED_DIM = 384
LAST_K = 5
USER_EMBED_DIM = EMBED_DIM * LAST_K

# ==========================================================
# AWS HELPERS
# ==========================================================

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

def to_decimal_vector(vec):
    return [Decimal(str(x)) for x in vec]

def convert_decimals(item):
    for k, v in item.items():
        if isinstance(v, Decimal):
            item[k] = float(v)
    return item

# ==========================================================
# SCANS
# ==========================================================

def scan_table(table_name, limit=100):
    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(table_name)

    items, resp = [], table.scan(Limit=limit)
    items.extend(resp.get("Items", []))

    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))

    log.info(f"[DEBUG] scan_table({table_name}) → {len(items)} rows")
    if items:
        log.info(f"[DEBUG] scan_table sample → {items[0]}")

    return [convert_decimals(i) for i in items]

def fake_embed(dim=EMBED_DIM):
    return [random.random() for _ in range(dim)]

# ==========================================================
# TABLE CREATION
# ==========================================================

def ensure_embedding_table_exists(table_name, pk):
    client = aws_resource("dynamodb").meta.client

    try:
        client.describe_table(TableName=table_name)
        log.info(f"[DEBUG] Table exists → {table_name}")
        return
    except client.exceptions.ResourceNotFoundException:
        log.warning(f"[DEBUG] Creating table → {table_name}")

    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": pk, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": pk, "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST"
    )

    client.get_waiter("table_exists").wait(TableName=table_name)
    log.info(f"[DEBUG] Table created → {table_name}")

# ==========================================================
# LOADERS
# ==========================================================

def load_products(**context):
    products = scan_table(PRODUCT_TABLE)
    log.info(f"[DEBUG] load_products → count={len(products)}")
    context["ti"].xcom_push(key="delta_products", value=products)

def load_orders(**context):
    orders = scan_table(PURCHASE_TABLE)
    log.info(f"[DEBUG] load_orders → count={len(orders)}")
    context["ti"].xcom_push(key="all_orders", value=orders)

def load_users(**context):
    users = scan_table(USER_SOURCE_TABLE)
    log.info(f"[DEBUG] load_users → count={len(users)}")
    context["ti"].xcom_push(key="delta_users", value=users)

def load_campaigns(**context):
    campaigns = scan_table(CAMPAIGN_SOURCE_TABLE)
    log.info(f"[DEBUG] load_campaigns → count={len(campaigns)}")
    context["ti"].xcom_push(key="delta_campaigns", value=campaigns)

# ==========================================================
# PRODUCT EMBEDDINGS
# ==========================================================

def embed_products(**context):
    products = context["ti"].xcom_pull(task_ids="load_products", key="delta_products") or []
    log.info(f"[DEBUG] embed_products → received={len(products)}")

    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(PRODUCT_TABLE)

    for p in products[:3]:
        log.info(f"[DEBUG] embedding product_id={p.get('product_id')}")

    for p in products:
        table.update_item(
            Key={"product_id": int(p["product_id"])},
            UpdateExpression="SET embedding = :e",
            ExpressionAttributeValues={
                ":e": to_decimal_vector(fake_embed())
            }
        )

    log.info("[DEBUG] embed_products → completed")

# ==========================================================
# PURCHASE EMBEDDINGS
# ==========================================================

def build_purchase_embeddings(**context):
    orders = context["ti"].xcom_pull(task_ids="load_orders", key="all_orders") or []
    log.info(f"[DEBUG] build_purchase_embeddings → orders={len(orders)}")

    dynamodb = aws_resource("dynamodb")
    product_table = dynamodb.Table(PRODUCT_TABLE)
    purchase_vectors = {}

    for o in orders[:3]:
        log.info(f"[DEBUG] order sample={o}")

    for o in orders:
        purchase_id = str(o["purchase_id"])
        product_id = int(o["product_id"])
        user_id = str(o["user_id"])

        product = product_table.get_item(
            Key={"product_id": product_id}
        ).get("Item", {})

        if not product:
            log.error(f"[ERROR] No product found for product_id={product_id}")
            continue

        emb = product.get("embedding", [0.0] * EMBED_DIM)

        purchase_vectors[purchase_id] = {
            "embedding": emb,
            "user_id": user_id,
            "product_id": str(product_id),
            "total_amount": o.get("total_amount")
        }

    log.info(f"[DEBUG] build_purchase_embeddings → built={len(purchase_vectors)}")
    return purchase_vectors

def persist_purchase_embeddings(**context):
    ensure_embedding_table_exists(PURCHASE_EMBED_TABLE, "purchase_id")
    vectors = context["ti"].xcom_pull(task_ids="build_purchase_embeddings") or {}

    log.info(f"[DEBUG] persist_purchase_embeddings → vectors={len(vectors)}")

    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(PURCHASE_EMBED_TABLE)
    now = datetime.utcnow().isoformat()

    for pid in list(vectors.keys())[:3]:
        log.info(f"[DEBUG] writing purchase_id={pid}")

    for pid, payload in vectors.items():
        table.update_item(
            Key={"purchase_id": pid},
            UpdateExpression="SET embedding=:e, updated_at=:u",
            ExpressionAttributeValues={
                ":e": to_decimal_vector(payload["embedding"]),
                ":u": now
            }
        )

    log.info("[DEBUG] persist_purchase_embeddings → completed")

# ==========================================================
# USER EMBEDDINGS
# ==========================================================

def build_user_embeddings(**context):
    users = context["ti"].xcom_pull(task_ids="load_users", key="delta_users") or []
    orders = context["ti"].xcom_pull(task_ids="load_orders", key="all_orders") or []

    log.info(f"[DEBUG] build_user_embeddings → users={len(users)}, orders={len(orders)}")

    debug_user = "U0023"
    user_ids = [str(u["user_id"]) for u in users]
    log.info(f"[DEBUG] First 10 users: {user_ids[:10]}")

    if debug_user not in user_ids:
        log.error(f"[CRITICAL] {debug_user} not found in users")

    order_map = {}
    for o in orders:
        order_map.setdefault(str(o["user_id"]), []).append(int(o["product_id"]))

    log.info(f"[DEBUG] Orders for {debug_user}: {order_map.get(debug_user)}")

    dynamodb = aws_resource("dynamodb")
    product_table = dynamodb.Table(PRODUCT_TABLE)

    user_vectors = {}

    for u in users:
        uid = str(u["user_id"])
        vec = []

        pids = order_map.get(uid, [])[:LAST_K]

        if uid == debug_user:
            log.info(f"[DEBUG] U0023 recent_products={pids}")

        for pid in pids:
            emb = product_table.get_item(
                Key={"product_id": pid}
            ).get("Item", {}).get("embedding")

            if uid == debug_user:
                log.info(f"[DEBUG] product_id={pid} embedding_exists={emb is not None}")

            vec.extend(emb or [0.0] * EMBED_DIM)

        while len(vec) < USER_EMBED_DIM:
            vec.extend([0.0] * EMBED_DIM)

        user_vectors[uid] = vec

    log.info(f"[DEBUG] build_user_embeddings → built={len(user_vectors)}")

    if debug_user in user_vectors:
        log.info(f"[DEBUG] Final vector length for U0023={len(user_vectors[debug_user])}")

    return user_vectors

def persist_user_embeddings(**context):
    ensure_embedding_table_exists(USER_EMBED_TABLE, "user_id")
    vectors = context["ti"].xcom_pull(task_ids="build_user_embeddings") or {}

    log.info(f"[DEBUG] persist_user_embeddings → vectors={len(vectors)}")

    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(USER_EMBED_TABLE)
    now = datetime.utcnow().isoformat()

    for uid in list(vectors.keys())[:3]:
        log.info(f"[DEBUG] writing user_id={uid}")

    for uid, v in vectors.items():
        table.update_item(
            Key={"user_id": uid},
            UpdateExpression="SET embedding=:e, updated_at=:u",
            ExpressionAttributeValues={
                ":e": to_decimal_vector(v),
                ":u": now
            }
        )

    log.info("[DEBUG] persist_user_embeddings → completed")

# ==========================================================
# CAMPAIGN EMBEDDINGS
# ==========================================================

def embed_campaigns(**context):
    campaigns = context["ti"].xcom_pull(task_ids="load_campaigns", key="delta_campaigns") or []
    log.info(f"[DEBUG] embed_campaigns → campaigns={len(campaigns)}")

    campaign_vectors = {}

    for c in campaigns:
        cid = c["campaign_id"]
        campaign_vectors[cid] = {
            "embedding": fake_embed(),
            "status": c.get("status"),
            "priority": c.get("priority")
        }

    log.info(f"[DEBUG] embed_campaigns → built={len(campaign_vectors)}")
    return campaign_vectors

def persist_campaigns(**context):
    ensure_embedding_table_exists(CAMPAIGN_EMBED_TABLE, "campaign_id")
    vectors = context["ti"].xcom_pull(task_ids="embed_campaigns") or {}

    log.info(f"[DEBUG] persist_campaigns → vectors={len(vectors)}")

    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(CAMPAIGN_EMBED_TABLE)
    now = datetime.utcnow().isoformat()

    for cid in list(vectors.keys())[:3]:
        log.info(f"[DEBUG] writing campaign_id={cid}")

    for cid, payload in vectors.items():
        table.update_item(
            Key={"campaign_id": cid},
            UpdateExpression="SET embedding=:e, updated_at=:u",
            ExpressionAttributeValues={
                ":e": to_decimal_vector(payload["embedding"]),
                ":u": now
            }
        )

    log.info("[DEBUG] persist_campaigns → completed")

# ==========================================================
# DAG
# ==========================================================

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ads", "dynamodb", "mwaa"]
):

    load_products_task = PythonOperator(task_id="load_products", python_callable=load_products)
    embed_products_task = PythonOperator(task_id="embed_products", python_callable=embed_products)

    load_orders_task = PythonOperator(task_id="load_orders", python_callable=load_orders)
    load_users_task = PythonOperator(task_id="load_users", python_callable=load_users)

    build_purchase_embeddings_task = PythonOperator(
        task_id="build_purchase_embeddings",
        python_callable=build_purchase_embeddings
    )
    persist_purchase_embeddings_task = PythonOperator(
        task_id="persist_purchase_embeddings",
        python_callable=persist_purchase_embeddings
    )

    build_user_embeddings_task = PythonOperator(
        task_id="build_user_embeddings",
        python_callable=build_user_embeddings
    )
    persist_user_embeddings_task = PythonOperator(
        task_id="persist_user_embeddings",
        python_callable=persist_user_embeddings
    )

    load_campaigns_task = PythonOperator(task_id="load_campaigns", python_callable=load_campaigns)
    embed_campaigns_task = PythonOperator(task_id="embed_campaigns", python_callable=embed_campaigns)
    persist_campaigns_task = PythonOperator(task_id="persist_campaigns", python_callable=persist_campaigns)

    load_products_task >> embed_products_task
    load_orders_task >> build_purchase_embeddings_task
    embed_products_task >> build_purchase_embeddings_task
    build_purchase_embeddings_task >> persist_purchase_embeddings_task

    load_users_task >> build_user_embeddings_task
    build_purchase_embeddings_task >> build_user_embeddings_task
    build_user_embeddings_task >> persist_user_embeddings_task

    load_campaigns_task >> embed_campaigns_task >> persist_campaigns_task
