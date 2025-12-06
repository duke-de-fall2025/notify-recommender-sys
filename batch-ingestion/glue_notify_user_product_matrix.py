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
job.init("user_product_matrix_job")

bucket = "notify-products"
output_path = f"s3://{bucket}/feature_store/user_product_matrix/"

# DynamoDB Output Table
dynamodb = boto3.resource("dynamodb")
ddb_table = dynamodb.Table("notify_user_product_matrix")

# -----------------------------
# Read Purchase History
# -----------------------------
dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="dynamodb",
    connection_options={"dynamodb.input.tableName": "notify_purchase_history"}
)

df = dyf.toDF()

df = df.withColumn("quantity", F.col("quantity").cast("int"))

# -----------------------------
# Build User-Product Matrix
# -----------------------------
matrix = (
    df.groupBy("user_id", "product_id")
      .agg(
          F.count("*").alias("times_purchased"),
          F.max("purchase_date").alias("last_purchase_date")
      )
      .withColumn("purchased_before", F.lit(1))
)

# -----------------------------
# Write to S3
# -----------------------------
matrix.repartition(1).write.mode("overwrite").parquet(output_path)
print("USER-PRODUCT MATRIX → written to S3")

# -----------------------------
# SAFE DynamoDB Write (Decimal Conversion)
# -----------------------------
def convert_value(v):
    if v is None:
        return None
    if isinstance(v, float):
        return Decimal(str(v))
    if isinstance(v, int):
        return v
    return v

rows = matrix.toPandas().to_dict(orient="records")

with ddb_table.batch_writer() as batch:
    for row in rows:
        clean = {k: convert_value(v) for k, v in row.items() if v is not None}
        batch.put_item(Item=clean)

print("USER-PRODUCT MATRIX - written to DynamoDB")

job.commit()
