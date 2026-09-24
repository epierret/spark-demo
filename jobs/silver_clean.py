import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp

def load_vault_secrets(path="/vault/secrets/minio"):
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                if line.startswith("export "):
                    key, value = line.replace("export ", "").strip().split("=", 1)
                    os.environ[key] = value.strip('"')

def create_spark_session():
    load_vault_secrets()
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")
    return (
        SparkSession.builder
        .appName("silver-clean")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.spark-medallion-demo.svc.cluster.local")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def clean_customers(spark, bronze_root, silver_root):
    df = spark.read.parquet(f"{bronze_root}/customers")
    before = df.count()
    df_clean = (
        df.dropDuplicates(["customer_id"])
          .filter(col("email") != "")
          .withColumn("signup_date", to_date(col("signup_date")))
          .withColumn("_cleaned_at", current_timestamp())
    )
    after = df_clean.count()
    df_clean.write.mode("overwrite").parquet(f"{silver_root}/customers")
    print(f"[silver] customers : {before} -> {after} lignes (doublons + emails vides retires)")

def clean_products(spark, bronze_root, silver_root):
    df = spark.read.parquet(f"{bronze_root}/products")
    df_clean = (
        df.dropDuplicates(["product_id"])
          .withColumn("price", col("price").cast("double"))
          .withColumn("_cleaned_at", current_timestamp())
    )
    df_clean.write.mode("overwrite").parquet(f"{silver_root}/products")
    print(f"[silver] products : {df_clean.count()} lignes")

def clean_orders(spark, bronze_root, silver_root):
    df = spark.read.parquet(f"{bronze_root}/orders")
    before = df.count()
    df_clean = (
        df.dropDuplicates(["order_id"])
          .withColumn("order_date", to_date(col("order_date")))
          .withColumn("quantity", col("quantity").cast("int"))
          .withColumn("_cleaned_at", current_timestamp())
    )
    after = df_clean.count()
    df_clean.write.mode("overwrite").parquet(f"{silver_root}/orders")
    print(f"[silver] orders : {before} -> {after} lignes (doublons retires)")

if __name__ == "__main__":
    spark = create_spark_session()
    bronze_root = "s3a://data-lake/bronze"
    silver_root = "s3a://data-lake/silver"

    clean_customers(spark, bronze_root, silver_root)
    clean_products(spark, bronze_root, silver_root)
    clean_orders(spark, bronze_root, silver_root)
    spark.stop()
