import boto3
import random
from airflow import DAG
from decimal import Decimal
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Attr
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# ==========================================================
# CONFIG
# ==========================================================

DAG_ID = "ads_embedding_pipeline_notify"
AWS_CONN_ID = "aws_default"

# SOURCE TABLES
PRODUCT_TABLE = "notify_products"
PURCHASE_TABLE = "notify_purchase_history"
USER_SOURCE_TABLE = "notify_users"
CAMPAIGN_SOURCE_TABLE = "notify_campaigns"

# EMBEDDING TABLES
USER_EMBED_TABLE = "notify_user_embeddings_v1"
PURCHASE_EMBED_TABLE = "notify_purchase_embeddings_v1"
CAMPAIGN_EMBED_TABLE = "notify_campaign_embeddings_v1"

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
# DYNAMO SCANS
# ==========================================================

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

def delta_scan(table_name, ts_field="updated_at", hours=24):
    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(table_name)

    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    items = []

    resp = table.scan(FilterExpression=Attr(ts_field).gte(cutoff))
    items.extend(resp.get("Items", []))

    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            ExclusiveStartKey=resp["LastEvaluatedKey"],
            FilterExpression=Attr(ts_field).gte(cutoff)
        )
        items.extend(resp.get("Items", []))

    return [convert_decimals(i) for i in items]

def fake_embed(dim=EMBED_DIM):
    return [random.random() for _ in range(dim)]

# ==========================================================
# GENERIC AUTO-CREATE EMBEDDING TABLE
# ==========================================================

def ensure_embedding_table_exists(table_name, pk):
    client = aws_resource("dynamodb").meta.client

    try:
        client.describe_table(TableName=table_name)
        print(f"✅ Table exists: {table_name}")
        return
    except client.exceptions.ResourceNotFoundException:
        print(f"⚠ Creating table: {table_name}")

    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": pk, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": pk, "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST"
    )

    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)

    print(f"✅ Table ready: {table_name}")

# ==========================================================
# LOADERS
# ==========================================================

def load_products(**context):
    # Product schema has no updated_at, so use full scan
    products = scan_table(PRODUCT_TABLE)
    context["ti"].xcom_push("delta_products", products)
    print(f"Products loaded: {len(products)}")

def load_orders(**context):
    orders = scan_table(PURCHASE_TABLE)
    context["ti"].xcom_push("all_orders", orders)
    print(f"Total orders loaded: {len(orders)}")

def load_users(**context):
    # Users have created_at, so use that for deltas
    users = delta_scan(USER_SOURCE_TABLE, ts_field="created_at", hours=24)
    context["ti"].xcom_push("delta_users", users)
    print(f"Delta users loaded: {len(users)}")

def load_campaigns(**context):
    # Campaigns: full scan for now
    campaigns = scan_table(CAMPAIGN_SOURCE_TABLE)
    context["ti"].xcom_push("delta_campaigns", campaigns)
    print(f"Campaigns loaded: {len(campaigns)}")

# ==========================================================
# PRODUCT EMBEDDINGS
# ==========================================================

def embed_products(**context):
    products = context["ti"].xcom_pull("load_products", "delta_products") or []
    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(PRODUCT_TABLE)

    for p in products:
        # product_id is the PK in notify_products_test
        pid = int(p["product_id"])
        table.update_item(
            Key={"product_id": pid},
            UpdateExpression="SET embedding = :e",
            ExpressionAttributeValues={":e": to_decimal_vector(fake_embed())}
        )

    print(f"Updated embeddings for {len(products)} products")

# ==========================================================
# USER EMBEDDINGS
# ==========================================================

def build_user_embeddings(**context):
    dynamodb = aws_resource("dynamodb")
    product_table = dynamodb.Table(PRODUCT_TABLE)

    users = context["ti"].xcom_pull("load_users", "delta_users") or []
    orders = context["ti"].xcom_pull("load_orders", "all_orders") or []

    # Map: user_id -> list of product_ids (Number)
    order_map = {}
    for o in orders:
        uid = o["user_id"]
        pid = int(o["product_id"])
        order_map.setdefault(uid, []).append(pid)

    user_vectors = {}

    for u in users:
        uid = u["user_id"]
        vec = []

        product_ids = order_map.get(uid, [])[:LAST_K]

        for pid in product_ids:
            resp = product_table.get_item(Key={"product_id": pid})
            item = resp.get("Item", {})
            emb = item.get("embedding", [0.0] * EMBED_DIM)
            vec.extend(emb)

        while len(vec) < USER_EMBED_DIM:
            vec.extend([0.0] * EMBED_DIM)

        user_vectors[uid] = vec

    print(f"Built embeddings for {len(user_vectors)} users")
    return user_vectors

def persist_user_embeddings(**context):
    ensure_embedding_table_exists(USER_EMBED_TABLE, "user_id")
    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(USER_EMBED_TABLE)

    vectors = context["ti"].xcom_pull("build_user_embeddings") or {}
    now = datetime.utcnow().isoformat()

    for uid, v in vectors.items():
        table.update_item(
            Key={"user_id": str(uid)},
            UpdateExpression="SET embedding=:e, updated_at=:u",
            ExpressionAttributeValues={
                ":e": to_decimal_vector(v),
                ":u": now
            }
        )

    print(f"Upserted {len(vectors)} user embeddings into {USER_EMBED_TABLE}")

# ==========================================================
# PURCHASE EMBEDDINGS
# ==========================================================

def build_purchase_embeddings(**context):
    """
    Purchase embedding is derived from the product embedding.
    Join:
        notify_purchases_test.product_id (Number)
          ->
        notify_products_test.product_id (Number)
    """

    orders = context["ti"].xcom_pull("load_orders", "all_orders") or []

    if not orders:
        print("No orders found for purchase embeddings")
        return {}

    dynamodb = aws_resource("dynamodb")
    product_table = dynamodb.Table(PRODUCT_TABLE)

    purchase_vectors = {}

    for o in orders:
        purchase_id = str(o["purchase_id"])
        product_id = int(o["product_id"])
        user_id = str(o["user_id"])

        resp = product_table.get_item(Key={"product_id": product_id})
        product = resp.get("Item", {})

        if product and "embedding" in product:
            purchase_embedding = product["embedding"]
        else:
            purchase_embedding = [0.0] * EMBED_DIM

        purchase_vectors[purchase_id] = {
            "embedding": purchase_embedding,
            "user_id": user_id,
            "product_id": str(product_id),
            "price": o.get("price"),
            "quantity": o.get("quantity"),
            "total_amount": o.get("total_amount"),
            "purchase_date": o.get("purchase_date")
        }

    print(f"Built {len(purchase_vectors)} purchase embeddings via product join")
    return purchase_vectors

def persist_purchase_embeddings(**context):
    ensure_embedding_table_exists(PURCHASE_EMBED_TABLE, "purchase_id")

    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(PURCHASE_EMBED_TABLE)

    vectors = context["ti"].xcom_pull("build_purchase_embeddings") or {}
    now = datetime.utcnow().isoformat()

    if not vectors:
        print("No purchase embeddings to persist")
        return

    for purchase_id, payload in vectors.items():
        table.update_item(
            Key={"purchase_id": purchase_id},
            UpdateExpression="""
                SET embedding = :e,
                    updated_at = :u,
                    user_id = :uid,
                    product_id = :pid,
                    price = :pr,
                    quantity = :q,
                    total_amount = :ta,
                    purchase_date = :pd
            """,
            ExpressionAttributeValues={
                ":e": to_decimal_vector(payload["embedding"]),
                ":u": now,
                ":uid": payload["user_id"],
                ":pid": payload["product_id"],
                ":pr": Decimal(str(payload.get("price", 0))),
                ":q": Decimal(str(payload.get("quantity", 0))),
                ":ta": Decimal(str(payload.get("total_amount", 0))),
                ":pd": payload.get("purchase_date")
            }
        )

    print(f"Upserted {len(vectors)} purchase embeddings into {PURCHASE_EMBED_TABLE}")

# ==========================================================
# CAMPAIGN EMBEDDINGS (FROM SOURCE TABLE)
# ==========================================================

def embed_campaigns(**context):
    campaigns = context["ti"].xcom_pull("load_campaigns", "delta_campaigns") or []

    campaign_vectors = {}

    for c in campaigns:
        cid = c["campaign_id"]

        # You can later replace fake_embed with a text model on these fields
        emb = fake_embed()

        campaign_vectors[cid] = {
            "embedding": emb,
            "status": c.get("status"),
            "priority": c.get("priority"),
            "notification_type": c.get("notification_type"),
            "start_date": c.get("start_date"),
            "end_date": c.get("end_date")
        }

    print(f"Built embeddings for {len(campaign_vectors)} campaigns")
    return campaign_vectors

def persist_campaigns(**context):
    ensure_embedding_table_exists(CAMPAIGN_EMBED_TABLE, "campaign_id")
    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(CAMPAIGN_EMBED_TABLE)

    vectors = context["ti"].xcom_pull("embed_campaigns") or {}
    now = datetime.utcnow().isoformat()

    for cid, payload in vectors.items():
        table.update_item(
            Key={"campaign_id": str(cid)},
            UpdateExpression="""
                SET embedding=:e,
                    updated_at=:u,
                    status=:s,
                    priority=:p,
                    notification_type=:n,
                    start_date=:sd,
                    end_date=:ed
            """,
            ExpressionAttributeValues={
                ":e": to_decimal_vector(payload["embedding"]),
                ":u": now,
                ":s": payload["status"],
                ":p": payload["priority"],
                ":n": payload["notification_type"],
                ":sd": payload["start_date"],
                ":ed": payload["end_date"]
            }
        )

    print(f"Upserted {len(vectors)} campaign embeddings into {CAMPAIGN_EMBED_TABLE}")

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

    load_products_task = PythonOperator(
        task_id="load_products",
        python_callable=load_products
    )
    embed_products_task = PythonOperator(
        task_id="embed_products",
        python_callable=embed_products
    )

    load_orders_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders
    )
    load_users_task = PythonOperator(
        task_id="load_users",
        python_callable=load_users
    )

    build_user_embeddings_task = PythonOperator(
        task_id="build_user_embeddings",
        python_callable=build_user_embeddings
    )
    persist_user_embeddings_task = PythonOperator(
        task_id="persist_user_embeddings",
        python_callable=persist_user_embeddings
    )

    build_purchase_embeddings_task = PythonOperator(
        task_id="build_purchase_embeddings",
        python_callable=build_purchase_embeddings
    )
    persist_purchase_embeddings_task = PythonOperator(
        task_id="persist_purchase_embeddings",
        python_callable=persist_purchase_embeddings
    )

    load_campaigns_task = PythonOperator(
        task_id="load_campaigns",
        python_callable=load_campaigns
    )
    embed_campaigns_task = PythonOperator(
        task_id="embed_campaigns",
        python_callable=embed_campaigns
    )
    persist_campaigns_task = PythonOperator(
        task_id="persist_campaigns",
        python_callable=persist_campaigns
    )

    # ===========================
    # DEPENDENCIES
    # ===========================
    
    # Products must be embedded before purchase joins
    load_products_task >> embed_products_task
    embed_products_task >> build_purchase_embeddings_task

    # Orders drive purchase embeddings
    load_orders_task >> build_purchase_embeddings_task
    build_purchase_embeddings_task >> persist_purchase_embeddings_task

    # Users depend on purchase history
    load_users_task >> build_user_embeddings_task
    build_purchase_embeddings_task >> build_user_embeddings_task
    build_user_embeddings_task >> persist_user_embeddings_task

    # Campaigns are independent
    load_campaigns_task >> embed_campaigns_task >> persist_campaigns_task
