import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# -------------------------------------------
# Spark session with SAFE S3A configuration
# -------------------------------------------
spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# -------------------------------------------
# No parameters used → hardcode bucket
# -------------------------------------------
bucket = "notify-products"

input_path = f"s3://{bucket}/raw/products/styles.csv"
output_path = f"s3://{bucket}/clean/products/"

print("DEBUG → ATTEMPTING TO READ:", input_path)

# -------------------------------------------
# Read CSV WITH header correctly
# -------------------------------------------
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

print("DEBUG → ROW COUNT:", df.count())
df.show(5)

# -------------------------------------------
# Add image URL
# -------------------------------------------
df = df.withColumn(
    "image_url",
    F.concat(
        F.lit("s3://notify-products/images/fashion/"),
        F.col("id").cast("string"),
        F.lit(".jpg")
    )
)

# -------------------------------------------
# WRITE PARQUET
# -------------------------------------------
print("DEBUG → WRITING TO:", output_path)
df.write.mode("overwrite").parquet(output_path)

print("JOB DONE SUCCESSFULLY.")
