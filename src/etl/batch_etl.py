# batch_etl.py
import logging
import findspark
import yaml
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, to_timestamp, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, LongType
from datetime import datetime

# Cấu hình logging nâng cao
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class ETLConfig:
    """Quản lý cấu hình ETL từ file YAML"""

    def __init__(self, config_path: str = "../config/etl_config.yaml"):
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Không thể đọc file cấu hình {config_path}: {e}")
            raise

    def get_spark_config(self) -> Dict[str, str]:
        return self.config.get('spark', {})

    def get_schema(self) -> StructType:
        return StructType([
            StructField("user_id", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

    def get_paths(self) -> Dict[str, str]:
        return self.config.get('paths', {})

    def get_db_properties(self) -> Dict[str, str]:
        return self.config.get('database', {})


class ETLPipeline:
    """Pipeline ETL chính với các bước Extract, Transform, Load"""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.spark = self._init_spark_session()
        self.schema = config.get_schema()

    def _init_spark_session(self) -> SparkSession:
        """Khởi tạo Spark Session với cấu hình động"""
        try:
            spark_conf = self.config.get_spark_config()
            builder = SparkSession.builder.appName("AdvancedClickstreamETL")

            for key, value in spark_conf.items():
                builder = builder.config(key, value)

            spark = builder.getOrCreate()
            logger.info("Khởi tạo Spark session thành công")
            return spark
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Spark session: {e}")
            raise

    def extract(self) -> Optional[DataFrame]:
        """Extract: Đọc dữ liệu từ MinIO"""
        try:
            paths = self.config.get_paths()
            df_raw = self.spark.read.schema(self.schema).json(paths['raw_data'])
            logger.info(f"Đọc thành công {df_raw.count()} bản ghi từ MinIO")
            return df_raw
        except Exception as e:
            logger.error(f"Lỗi khi đọc từ MinIO: {e}")
            return None

    def transform(self, df_raw: DataFrame) -> Optional[DataFrame]:
        """Transform: Làm sạch và biến đổi dữ liệu"""
        try:
            # Làm sạch dữ liệu
            df_clean = (df_raw.na.drop(subset=["user_id", "product_id", "action", "timestamp"])
                        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
                        .withColumn("product_id", col("product_id").cast(IntegerType()))
                        .withColumn("action", col("action").cast(StringType()))
                        .withColumn("etl_timestamp", current_timestamp())
                        .withColumn("source", lit("minio")))

            logger.info(f"Dữ liệu sau khi làm sạch: {df_clean.count()} bản ghi")

            # Tính toán chỉ số bổ sung
            df_aggregated = (df_clean.groupBy("product_id")
            .agg(
                count("action").cast(LongType()).alias("click_count"),
                count("user_id").cast(LongType()).alias("unique_users")
            )
            )

            # Join với dữ liệu sản phẩm
            df_products = self._load_product_data()
            if df_products is None:
                return None

            df_enriched = df_aggregated.join(df_products, on="product_id", how="inner")
            logger.info("Hoàn thành enrich dữ liệu với thông tin sản phẩm")

            # Thêm cột tính toán doanh thu tiềm năng
            df_enriched = df_enriched.withColumn(
                "potential_revenue",
                col("click_count") * col("price") * lit(
                    self.config.config.get('business_rules', {}).get('conversion_rate', 0.1))
            )

            return df_enriched
        except Exception as e:
            logger.error(f"Lỗi trong quá trình transform: {e}")
            return None

    def _load_product_data(self) -> Optional[DataFrame]:
        """Đọc dữ liệu sản phẩm từ CSV"""
        try:
            paths = self.config.get_paths()
            df_products = (self.spark.read.csv(paths['products'], header=True, inferSchema=True)
                           .withColumn("product_id", col("product_id").cast(IntegerType()))
                           .withColumn("price", col("price").cast(DoubleType())))
            logger.info(f"Đọc thành công {df_products.count()} sản phẩm từ CSV")
            return df_products
        except Exception as e:
            logger.error(f"Lỗi khi đọc products.csv: {e}")
            return None

    def load(self, df: DataFrame) -> bool:
        """Load: Ghi dữ liệu vào PostgreSQL"""
        """Spark dùng JDBC để kết nối PostgreSQL vì JDBC là chuẩn Java, hỗ trợ phân tán, tối ưu cho dataframe distributed processing, trong khi thư viện Python thuần (psycopg2) không phân tán."""
        try:
            db_props = self.config.get_db_properties()
            df.write.jdbc(
                url=db_props['url'],
                table="aggregated_clicks",
                mode="overwrite",
                properties={
                    "user": db_props['user'],
                    "password": db_props['password'],
                    "driver": db_props['driver']
                }
            )
            logger.info("Dữ liệu đã được ghi vào PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi ghi vào PostgreSQL: {e}")
            return False

    def verify(self) -> None:
        """Xác minh dữ liệu đã load"""
        try:
            db_props = self.config.get_db_properties()
            df_verify = self.spark.read.jdbc(
                url=db_props['url'],
                table="aggregated_clicks",
                properties={
                    "user": db_props['user'],
                    "password": db_props['password'],
                    "driver": db_props['driver']
                }
            )
            logger.info(f"Xác minh: Đã load {df_verify.count()} bản ghi từ PostgreSQL")
            df_verify.show(10)
        except Exception as e:
            logger.error(f"Lỗi khi xác minh dữ liệu: {e}")

    def run(self) -> bool:
        """Chạy toàn bộ pipeline ETL"""
        try:
            df_raw = self.extract()
            if df_raw is None:
                return False

            df_transformed = self.transform(df_raw)
            if df_transformed is None:
                return False

            success = self.load(df_transformed)
            if success:
                self.verify()
            return success
        except Exception as e:
            logger.error(f"Lỗi trong pipeline: {e}")
            return False
        finally:
            self.spark.stop()
            logger.info("Đã dừng Spark session")


if __name__ == "__main__":
    config = ETLConfig()
    pipeline = ETLPipeline(config)
    success = pipeline.run()
    if not success:
        logger.error("Pipeline thất bại")
        exit(1)