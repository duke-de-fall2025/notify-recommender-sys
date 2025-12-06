import boto3
from decimal import Decimal

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pyspark.sql.functions as F

# -----------------------------
# Setup
# -----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("product_features_job")

bucket = "notify-products"
output_path = f"s3://{bucket}/feature_store/product_features/"

# DynamoDB output table
dynamodb = boto3.resource("dynamodb")
ddb_table = dynamodb.Table("notify_product_features")

# -----------------------------
# Read DynamoDB Source Tables
# -----------------------------
purchase_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="dynamodb",
    connection_options={"dynamodb.input.tableName": "notify_purchase_history"}
)

products_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="dynamodb",
    connection_options={"dynamodb.input.tableName": "notify_products"}
)

dfp = purchase_dyf.toDF()
dfp = (
    dfp
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
    .withColumn("price", F.col("price").cast("double"))
)

df_prod = products_dyf.toDF()

# -----------------------------
# Aggregate Product Features
# -----------------------------
agg = (
    dfp.groupBy("product_id")
       .agg(
           F.sum("quantity").alias("total_sales"),
           F.sum("total_amount").alias("revenue"),
           F.avg("price").alias("avg_price"),
           F.max("purchase_date").alias("last_sold_date")
       )
)

df_feat = agg.join(df_prod, "product_id", "left")

# -----------------------------
# Write to S3 (Parquet Feature Store)
# -----------------------------
df_feat.repartition(1).write.mode("overwrite").parquet(output_path)
print("PRODUCT FEATURES - written to S3")

# -----------------------------
# SAFE DynamoDB Write (Decimal Conversion)
# -----------------------------
def convert_value(v):
    if v is None:
        return None
    if isinstance(v, float):
        return Decimal(str(v))
    return v

rows = df_feat.toPandas().to_dict(orient="records")

with ddb_table.batch_writer() as batch:
    for row in rows:
        item = {k: convert_value(v) for k, v in row.items() if v is not None}
        batch.put_item(Item=item)

print("PRODUCT FEATURES - written to DynamoDB")

job.commit()
