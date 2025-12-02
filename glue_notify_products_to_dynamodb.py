import sys
from decimal import Decimal
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
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
input_path = f"s3://{bucket}/clean/products/"
table_name = "notify_products"

ddb = boto3.resource("dynamodb")
table = ddb.Table(table_name)

# -----------------------
# READ PARQUET
# -----------------------
df = spark.read.parquet(input_path)

# SHOW SAMPLE
df.show(5)

# -----------------------
# CONVERT FLOATS TO DECIMAL
# -----------------------

def row_to_ddb_item(row):
    item = {}
    for col, value in row.asDict().items():
        if value is None:
            continue

        if isinstance(value, float):
            item[col] = Decimal(str(value))   # convert float → Decimal
        else:
            item[col] = value

    return item

# -----------------------
# WRITE EACH ROW TO DDB
# -----------------------

rows = df.collect()

for row in rows:
    item = row_to_ddb_item(row)
    table.put_item(Item=item)

print(f"Loaded {len(rows)} items into DynamoDB table: {table_name}")

job.commit()
