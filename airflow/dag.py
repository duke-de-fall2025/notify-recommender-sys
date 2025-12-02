from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime
import pandas as pd
import numpy as np
from typing import List
from decimal import Decimal

# ==========================================================
# CONFIG
# ==========================================================

DAG_ID = "ads_embedding_pipeline_dynamodb_final"
AWS_CONN_ID = "aws_default"

PRODUCT_TABLE = "notify_products"
PURCHASE_TABLE = "notify_purchases_test"

USER_EMBED_TABLE = "user_embeddings"
CAMPAIGN_EMBED_TABLE = "campaign_embeddings"

EMBED_DIM = 384
USER_EMBED_DIM = EMBED_DIM * 5  # 1920

TMP = {
    "products": "/tmp/products.parquet",
    "products_embedded": "/tmp/products_embedded.parquet",
    "purchases": "/tmp/purchases.parquet",
    "users_embedded": "/tmp/users_embedded.parquet",
    "campaigns": "/tmp/campaigns.parquet",
    "campaigns_embedded": "/tmp/campaigns_embedded.parquet",
}

# ==========================================================
# HELPERS
# ==========================================================

def aws_resource(service: str):
    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type=service
    )
    return hook.get_resource_type(service)

def convert_decimals(item):
    out = {}
    for k, v in item.items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        else:
            out[k] = v
    return out

def scan_table(table_name: str) -> pd.DataFrame:
    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(table_name)

    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))

    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            ExclusiveStartKey=resp["LastEvaluatedKey"]
        )
        items.extend(resp.get("Items", []))

    cleaned = [convert_decimals(i) for i in items]
    return pd.DataFrame(cleaned)

def fake_embed(texts: List[str], dim: int = EMBED_DIM):
    return np.random.rand(len(texts), dim).tolist()

# ==========================================================
# TASK LOGIC
# ==========================================================

# --------------------------
# 1. EXTRACT PRODUCTS
# --------------------------
def extract_products_ddb():
    df = scan_table(PRODUCT_TABLE)
    df.to_parquet(TMP["products"])

# --------------------------
# 2. GENERATE PRODUCT EMBEDDINGS
# --------------------------
def generate_product_embeddings():
    df = pd.read_parquet(TMP["products"])

    df["embed_text"] = (
        df["productDisplayName"].astype(str)
        + " "
        + df["articleType"].astype(str)
        + " "
        + df["baseColour"].astype(str)
    )

    df["embedding"] = fake_embed(df["embed_text"].tolist())
    df.to_parquet(TMP["products_embedded"])

# --------------------------
# 3. EXTRACT PURCHASES
# --------------------------
def extract_purchases_ddb():
    df = scan_table(PURCHASE_TABLE)
    df.to_parquet(TMP["purchases"])

# --------------------------
# 4. BUILD USER EMBEDDINGS (LAST 5 PURCHASE CONCAT)
# --------------------------
def build_user_embeddings_last5_concat():
    purchases = pd.read_parquet(TMP["purchases"])
    products = pd.read_parquet(TMP["products_embedded"])

    merged = purchases.merge(
        products[["id", "embedding"]],
        on="id",
        how="left"
    )

    merged["purchase_ts"] = pd.to_datetime(
        merged["purchase_ts"]
    )

    merged = merged.sort_values(
        by=["user_id", "purchase_ts"],
        ascending=[True, False]
    )

    last5 = merged.groupby("user_id").head(5)

    zero_vec = [0.0] * EMBED_DIM
    user_rows = []

    for user_id, grp in last5.groupby("user_id"):
        vecs = grp["embedding"].dropna().tolist()

        while len(vecs) < 5:
            vecs.append(zero_vec)

        final_vec = []
        for v in vecs[:5]:
            final_vec.extend(v)

        user_rows.append({
            "user_id": str(user_id),
            "embedding": final_vec
        })

    pd.DataFrame(user_rows).to_parquet(
        TMP["users_embedded"]
    )

# --------------------------
# 5. PERSIST USER EMBEDDINGS → DYNAMODB
# --------------------------
def persist_user_embeddings_ddb():
    df = pd.read_parquet(TMP["users_embedded"])
    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(USER_EMBED_TABLE)

    with table.batch_writer() as batch:
        for _, r in df.iterrows():
            batch.put_item(
                Item={
                    "user_id": r["user_id"],
                    "embedding": r["embedding"],
                    "updated_at": datetime.utcnow().isoformat()
                }
            )

# --------------------------
# 6. GENERATE DUMMY CAMPAIGNS
# --------------------------
def generate_dummy_campaigns():
    df = pd.DataFrame({
        "campaign_id": [1, 2, 3],
        "campaign_name": [
            "Winter Sale",
            "Sports Bonanza",
            "Women Essentials"
        ],
        "category": [
            "Winter Wear",
            "Sports",
            "Women"
        ]
    })

    df.to_parquet(TMP["campaigns"])

# --------------------------
# 7. GENERATE CAMPAIGN EMBEDDINGS
# --------------------------
def generate_campaign_embeddings():
    df = pd.read_parquet(TMP["campaigns"])

    df["embed_text"] = (
        df["campaign_name"] + " " + df["category"]
    )

    df["embedding"] = fake_embed(df["embed_text"].tolist())
    df.to_parquet(TMP["campaigns_embedded"])

# --------------------------
# 8. PERSIST CAMPAIGN EMBEDDINGS → DYNAMODB
# --------------------------
def persist_campaign_embeddings_ddb():
    df = pd.read_parquet(TMP["campaigns_embedded"])
    dynamodb = aws_resource("dynamodb")
    table = dynamodb.Table(CAMPAIGN_EMBED_TABLE)

    with table.batch_writer() as batch:
        for _, r in df.iterrows():
            batch.put_item(
                Item={
                    "campaign_id": int(r["campaign_id"]),
                    "embedding": r["embedding"],
                    "updated_at": datetime.utcnow().isoformat()
                }
            )

# ==========================================================
# DAG DEFINITION
# ==========================================================

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ads", "dynamodb", "embeddings", "recsys"]
):

    extract_products_ddb_task = PythonOperator(
        task_id="extract_products_ddb",
        python_callable=extract_products_ddb
    )

    generate_product_embeddings_task = PythonOperator(
        task_id="generate_product_embeddings",
        python_callable=generate_product_embeddings
    )

    extract_purchases_ddb_task = PythonOperator(
        task_id="extract_purchases_ddb",
        python_callable=extract_purchases_ddb
    )

    build_user_embeddings_last5_concat_task = PythonOperator(
        task_id="build_user_embeddings_last5_concat",
        python_callable=build_user_embeddings_last5_concat
    )

    persist_user_embeddings_ddb_task = PythonOperator(
        task_id="persist_user_embeddings_ddb",
        python_callable=persist_user_embeddings_ddb
    )

    generate_dummy_campaigns_task = PythonOperator(
        task_id="generate_dummy_campaigns",
        python_callable=generate_dummy_campaigns
    )

    generate_campaign_embeddings_task = PythonOperator(
        task_id="generate_campaign_embeddings",
        python_callable=generate_campaign_embeddings
    )

    persist_campaign_embeddings_ddb_task = PythonOperator(
        task_id="persist_campaign_embeddings_ddb",
        python_callable=persist_campaign_embeddings_ddb
    )

    # ======================================================
    # DEPENDENCIES
    # ======================================================

    extract_products_ddb_task >> generate_product_embeddings_task >> build_user_embeddings_last5_concat_task
    extract_purchases_ddb_task >> build_user_embeddings_last5_concat_task >> persist_user_embeddings_ddb_task

    generate_dummy_campaigns_task >> generate_campaign_embeddings_task >> persist_campaign_embeddings_ddb_task
