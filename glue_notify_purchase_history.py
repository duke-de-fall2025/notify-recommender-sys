import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = "notify-products"

input_path  = f"s3://{bucket}/raw/purchases/purchase_history.csv"
output_path = f"s3://{bucket}/clean/purchase_history/"

# Read source CSV
df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
)

# Convert date to string format for DynamoDB (yyyy-MM-dd)
df = df.withColumn(
    "purchase_date",
    F.date_format(
        F.to_date("purchase_date", "dd-MM-yyyy"), 
        "yyyy-MM-dd"
    )
)

# Convert all columns to STRING except numbers needed as Number in DynamoDB
df = (df
    .withColumn("user_id", F.col("user_id").cast("string"))
    .withColumn("purchase_id", F.col("purchase_id").cast("string"))
    .withColumn("id", F.col("id").cast("int"))                  # DynamoDB: Number
    .withColumn("quantity", F.col("quantity").cast("int"))      # DynamoDB: Number
    .withColumn("price", F.col("price").cast("int"))            # DynamoDB: Number
    .withColumn("purchase_date", F.col("purchase_date").cast("string"))
)

# Write output as Parquet
df.write.mode("overwrite").parquet(output_path)

job.commit()
