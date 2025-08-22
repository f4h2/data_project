import logging
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, LongType

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("ClickstreamETL") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1034,org.postgresql:postgresql:42.3.1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

logger.info("Spark session initialized")

# Định nghĩa schema cho raw data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Đọc raw data từ MinIO
raw_data_path = "s3a://raw-data/dt=*/*.json"
try:
    df_raw = spark.read.schema(schema).json(raw_data_path)
    logger.info(f"Read {df_raw.count()} records from MinIO")
except Exception as e:
    logger.error(f"Error reading from MinIO: {e}")
    spark.stop()
    exit(1)

# Clean: Drop nulls, standardize formats
df_clean = df_raw.na.drop(subset=["user_id", "product_id", "action", "timestamp"]) \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")) \
    .withColumn("product_id", col("product_id").cast(IntegerType())) \
    .withColumn("action", col("action").cast(StringType()))
logger.info(f"Cleaned data, {df_clean.count()} records remain")

# Transform: Aggregate clicks per product
df_aggregated = df_clean.groupBy("product_id").agg(count("action").alias("click_count").cast(LongType()))
logger.info("Aggregated clicks per product")

# Enrich: Join với dữ liệu sản phẩm từ CSV
products_path = "products.csv"
try:
    df_products = spark.read.csv(products_path, header=True, inferSchema=True) \
        .withColumn("product_id", col("product_id").cast(IntegerType())) \
        .withColumn("price", col("price").cast(DoubleType()))
    logger.info(f"Read {df_products.count()} products from CSV")
except Exception as e:
    logger.error(f"Error reading products.csv: {e}")
    spark.stop()
    exit(1)

df_enriched = df_aggregated.join(df_products, on="product_id", how="inner")
logger.info("Enriched data with product info")

# Load vào PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/warehouse"
properties = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

try:
    df_enriched.write.jdbc(url=jdbc_url, table="aggregated_clicks", mode="overwrite", properties=properties)
    logger.info("Data loaded to PostgreSQL table aggregated_clicks")
except Exception as e:
    logger.error(f"Error writing to PostgreSQL: {e}")
    spark.stop()
    exit(1)

# Test: Show sample data
df_enriched.show(10)

# Verify trong PostgreSQL
df_verify = spark.read.jdbc(url=jdbc_url, table="aggregated_clicks", properties=properties)
logger.info(f"Verification: Loaded {df_verify.count()} records from PostgreSQL")
df_verify.show(10)

# Stop Spark Session
spark.stop()
