import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

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
        .appName("bronze-ingest")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.spark-medallion-demo.svc.cluster.local")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def ingest(spark, source_path: str, table_name: str, bronze_root: str):
    df = spark.read.option("header", "true").csv(source_path)
    df_enriched = (
        df.withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", input_file_name())
          .withColumn("_source_system", lit("ecommerce-csv-export"))
    )
    output_path = f"{bronze_root}/{table_name}"
    df_enriched.write.mode("overwrite").parquet(output_path)
    print(f"[bronze] {table_name} : {df_enriched.count()} lignes ingérées vers {output_path}")

if __name__ == "__main__":
    spark = create_spark_session()
    raw_root = "s3a://data-lake/raw"
    bronze_root = "s3a://data-lake/bronze"

    ingest(spark, f"{raw_root}/customers.csv", "customers", bronze_root)
    ingest(spark, f"{raw_root}/products.csv", "products", bronze_root)
    ingest(spark, f"{raw_root}/orders.csv", "orders", bronze_root)
    spark.stop()
