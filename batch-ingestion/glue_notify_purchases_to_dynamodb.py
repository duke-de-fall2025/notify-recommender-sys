import sys
from decimal import Decimal
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------
# CONFIG
# -----------------------
bucket = "notify-products"
input_path = f"s3://{bucket}/clean/purchase_history/"
table_name = "notify_purchase_history"

ddb = boto3.resource("dynamodb")
table = ddb.Table(table_name)

# -----------------------
# READ PARQUET
# -----------------------
df = spark.read.parquet(input_path)

df.show(5)
print("Row count:", df.count())

# -----------------------
# WRITE TO DYNAMODB
# -----------------------

def convert_row(row):
    item = {}
    for col, value in row.asDict().items():
        if value is None:
            continue

        # Float → Decimal for price & quantity if needed
        if isinstance(value, float):
            item[col] = Decimal(str(value))
        else:
            item[col] = value

    return item

rows = df.collect()

for row in rows:
    table.put_item(Item=convert_row(row))

print(f"Loaded {len(rows)} purchase history records into DynamoDB table: {table_name}")

job.commit()
