import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, round as spark_round, current_timestamp

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
        .appName("gold-aggregate")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.spark-medallion-demo.svc.cluster.local")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

if __name__ == "__main__":
    spark = create_spark_session()
    silver_root = "s3a://data-lake/silver"
    gold_root = "s3a://data-lake/gold"

    orders = spark.read.parquet(f"{silver_root}/orders")
    products = spark.read.parquet(f"{silver_root}/products")
    customers = spark.read.parquet(f"{silver_root}/customers")

    # Jointure orders + products pour avoir le prix de chaque ligne de commande
    orders_enriched = orders.join(products, "product_id").withColumn(
        "line_total", col("quantity") * col("price")
    )

    # 1. CA quotidien
    daily_revenue = (
        orders_enriched.groupBy("order_date")
        .agg(spark_round(spark_sum("line_total"), 2).alias("revenue"))
        .orderBy("order_date")
        .withColumn("_computed_at", current_timestamp())
    )
    daily_revenue.write.mode("overwrite").parquet(f"{gold_root}/daily_revenue")
    print(f"[gold] daily_revenue : {daily_revenue.count()} jours calcules")

    # 2. Top produits par quantite vendue
    top_products = (
        orders_enriched.groupBy("name", "category")
        .agg(spark_sum("quantity").alias("total_quantity"), spark_round(spark_sum("line_total"), 2).alias("total_revenue"))
        .orderBy(col("total_quantity").desc())
        .withColumn("_computed_at", current_timestamp())
    )
    top_products.write.mode("overwrite").parquet(f"{gold_root}/top_products")
    print(f"[gold] top_products : {top_products.count()} produits classes")

    # 3. Panier moyen par client
    avg_basket = (
        orders_enriched.groupBy("customer_id")
        .agg(count("order_id").alias("nb_orders"), spark_round(avg("line_total"), 2).alias("avg_basket"))
        .withColumn("_computed_at", current_timestamp())
    )
    avg_basket.write.mode("overwrite").parquet(f"{gold_root}/avg_basket_per_customer")
    print(f"[gold] avg_basket_per_customer : {avg_basket.count()} clients")

    spark.stop()
