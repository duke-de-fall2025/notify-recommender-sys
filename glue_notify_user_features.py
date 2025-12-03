import sys
import boto3
from decimal import Decimal
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window


# -----------------------------
# Setup
# -----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = "notify-products"
output_path = f"s3://{bucket}/feature_store/user_features/"

dynamodb = boto3.resource("dynamodb")
ddb_table = dynamodb.Table("notify_user_features")

# -----------------------------
# Read DynamoDB Source Tables
# -----------------------------
purchases_dyf = glueContext.create_dynamic_frame_from_options(
    "dynamodb",
    {"dynamodb.input.tableName": "notify_purchase_history"}
)

users_dyf = glueContext.create_dynamic_frame_from_options(
    "dynamodb",
    {"dynamodb.input.tableName": "notify_users"}
)

products_dyf = glueContext.create_dynamic_frame_from_options(
    "dynamodb",
    {"dynamodb.input.tableName": "notify_products"}
)

dfp = purchases_dyf.toDF()
dfu = users_dyf.toDF()
dfprod = products_dyf.toDF()

dfp = (
    dfp.withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("string"))
)

dfprod = dfprod.withColumn("product_id", F.col("product_id").cast("string"))

# -----------------------------
# Join purchases with products to get articleType & masterCategory
# -----------------------------
df_joined = dfp.join(dfprod.select("product_id", "articleType", "masterCategory"),
                     on="product_id", how="left")

# -----------------------------
# Compute standard aggregates
# -----------------------------
user_agg = (
    df_joined.groupBy("user_id")
       .agg(
           F.count("*").alias("total_orders"),
           F.sum("total_amount").alias("total_spent"),
           F.avg("total_amount").alias("avg_order_value"),
           F.max("purchase_date").alias("last_purchase_date")
       )
)

# -----------------------------
# Top 5 Most Frequent articleType
# -----------------------------
article_pref = (
    df_joined.groupBy("user_id", "articleType")
             .agg(F.count("*").alias("cnt"))
             .orderBy(F.col("cnt").desc())
)

window_article = (
    F.row_number().over(
        Window.partitionBy("user_id").orderBy(F.col("cnt").desc())
    )
)

top_articles = (
    article_pref.withColumn("rnk", window_article)
                .filter("rnk <= 5")
                .groupBy("user_id")
                .agg(
                    F.concat_ws(", ", F.collect_list("articleType"))
                    .alias("top_article_types")
                )
)

# -----------------------------
# Top 5 Most Frequent masterCategory
# -----------------------------
master_pref = (
    df_joined.groupBy("user_id", "masterCategory")
             .agg(F.count("*").alias("cnt"))
             .orderBy(F.col("cnt").desc())
)

window_master = (
    F.row_number().over(
        Window.partitionBy("user_id").orderBy(F.col("cnt").desc())
    )
)

top_masters = (
    master_pref.withColumn("rnk", window_master)
               .filter("rnk <= 5")
               .groupBy("user_id")
               .agg(
                   F.concat_ws(", ", F.collect_list("masterCategory"))
                   .alias("top_master_categories")
               )
)

# -----------------------------
# Merge all features
# -----------------------------
df_feat = (
    user_agg.join(dfu, "user_id", "left")
            .join(top_articles, "user_id", "left")
            .join(top_masters, "user_id", "left")
            .withColumn(
                "days_since_signup",
                F.datediff(F.current_date(), F.to_date("created_at"))
            )
)

# -----------------------------
# Write to S3
# -----------------------------
df_feat.repartition(1).write.mode("overwrite").parquet(output_path)
print("USER FEATURES - written to S3")

# -----------------------------
# Convert Spark → Pandas and fix timestamps
# -----------------------------
pdf = df_feat.toPandas()

date_cols = ["last_purchase_date", "created_at"]
for col in date_cols:
    if col in pdf.columns:
        pdf[col] = pdf[col].astype(str)

rows = pdf.to_dict(orient="records")

# -----------------------------
# Write to DynamoDB 
# -----------------------------
def convert_value(v):
    if v is None:
        return None
    if isinstance(v, float):
        return Decimal(str(v))
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        return v
    return v

with ddb_table.batch_writer() as batch:
    for row in rows:
        item = {
            k: convert_value(v)
            for k, v in row.items()
            if v is not None
        }
        batch.put_item(Item=item)

print("WROTE TO DYNAMODB SUCCESSFULLY.")

job.commit()
