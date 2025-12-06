import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# -------------------------
# INIT
# -------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = "notify-products"

# -------------------------
# PATHS
# -------------------------
input_path = f"s3://{bucket}/raw/users/users.csv"
output_path = f"s3://{bucket}/clean/users/"

# -------------------------
# READ CSV
# -------------------------
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")  # keeps it flexible
    .csv(input_path)
)

print("RAW ROW COUNT:", df.count())
df.show(10)

# -------------------------
# Ensure user_id exists
# -------------------------
if "user_id" not in df.columns:
    raise Exception("'user_id' column NOT FOUND in CSV. Check your source file!")

# -------------------------
# Trim all columns
# -------------------------
df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])

# -------------------------
# Select only the required columns (without type enforcement)
# -------------------------
df = df.select(
    "user_id",
    "name",
    "email",
    "password_hash",
    "created_at"
)

# -------------------------
# WRITE PARQUET (clean format)
# -------------------------
(
    df.write
      .mode("overwrite")
      .parquet(output_path)
)

print("CLEAN USERS WRITTEN TO:", output_path)

job.commit()
