import sys
import boto3
from decimal import Decimal
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F

# ---------------------------------------
# Glue Job Setup
# ---------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = "notify-products"

input_path = f"s3://{bucket}/raw/campaigns/campaigns.csv"
output_path = f"s3://{bucket}/clean/campaigns/"

print("Reading input CSV:", input_path)

# ---------------------------------------
# Read CSV
# ---------------------------------------
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

print("Preview of input data:")
df.show(5)
print("Total campaigns loaded:", df.count())

# ---------------------------------------
# Type Casting
# ---------------------------------------
df = df.withColumn("priority", F.col("priority").cast("int"))

# ---------------------------------------
# Write Cleaned Parquet to S3
# ---------------------------------------
print("Writing cleaned campaigns to S3:", output_path)
df.repartition(1).write.mode("overwrite").parquet(output_path)
print("Clean Parquet written to S3")

# ---------------------------------------
# DynamoDB Setup
# ---------------------------------------
dynamodb = boto3.resource("dynamodb")
ddb_table = dynamodb.Table("notify_campaigns")
print("Preparing to write to DynamoDB table:", ddb_table.name)

# ---------------------------------------
# Decimal-safe + Timestamp-safe conversion
# ---------------------------------------
def convert_value(v):
    if v is None:
        return None
    if isinstance(v, float):
        return Decimal(str(v))   # DynamoDB cannot store float
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        return v
    return v

# ---------------------------------------
# Convert Spark df → Pandas and fix timestamps
# ---------------------------------------
pdf = df.toPandas()

# Convert Pandas Timestamp → string
date_cols = ["start_date", "end_date", "created_at"]
for col in date_cols:
    if col in pdf.columns:
        pdf[col] = pdf[col].astype(str)

rows = pdf.to_dict(orient="records")

# ---------------------------------------
# Write to DynamoDB
# ---------------------------------------
print("Writing campaigns to DynamoDB...")

with ddb_table.batch_writer() as batch:
    for row in rows:
        clean_item = {
            k: convert_value(v)
            for k, v in row.items()
            if v is not None
        }
        batch.put_item(Item=clean_item)

print("Successfully ingested all campaigns into DynamoDB")

job.commit()
print("JOB COMPLETE!")
