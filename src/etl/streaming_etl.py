import logging
import yaml
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, window, current_timestamp, lit, from_json, to_timestamp, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, LongType

# Cấu hình logging nâng cao
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('streaming_etl.log')
    ]
)
logger = logging.getLogger(__name__)


class StreamingETLConfig:
    """Quản lý cấu hình cho streaming ETL"""

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
            StructField("timestamp", StringType(), True),
            StructField("session_id", StringType(), True)
        ])

    def get_kafka_config(self) -> Dict[str, str]:
        return self.config.get('kafka', {})

    def get_db_properties(self) -> Dict[str, str]:
        return self.config.get('database', {})


class StreamingETLPipeline:
    """Pipeline Spark Structured Streaming để xử lý clickstream"""

    def __init__(self, config: StreamingETLConfig, top_n: int = 5):
        self.config = config
        self.top_n = top_n
        self.spark = self._init_spark_session()
        self.schema = config.get_schema()

    def _init_spark_session(self) -> SparkSession:
        """Khởi tạo Spark Session cho streaming"""
        try:
            spark_conf = self.config.get_spark_config()
            builder = SparkSession.builder.appName("ClickstreamStreamingETL")

            for key, value in spark_conf.items():
                builder = builder.config(key, value)

            spark = builder.getOrCreate()
            logger.info("Khởi tạo Spark session thành công")
            return spark
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Spark session: {e}")
            raise

    def read_stream(self) -> DataFrame:
        """Đọc stream từ Kafka"""
        try:
            kafka_config = self.config.get_kafka_config()
            df_stream = (self.spark.readStream
                         .format("kafka")
                         .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
                         .option("subscribe", kafka_config['topic'])
                         .option("startingOffsets", "latest")
                         .load())

            df_stream = (df_stream.selectExpr("CAST(value AS STRING) as json")
                         .select(from_json(col("json"), self.schema).alias("data"))
                         .select("data.*"))

            logger.info("Đọc stream từ Kafka thành công")
            return df_stream
        except Exception as e:
            logger.error(f"Lỗi khi đọc stream từ Kafka: {e}")
            raise


    def transform_stream(self, df_stream: DataFrame) -> DataFrame:
        """Biến đổi stream: window 5 phút, group by product_id, lấy top N"""
        try:
            df_clean = (df_stream.na.drop(subset=["user_id", "product_id", "action", "timestamp"])
                        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
                        .withColumn("product_id", col("product_id").cast(IntegerType()))
                        .withColumn("action", col("action").cast(StringType()))
                        .withColumn("etl_timestamp", current_timestamp()))

            # Window aggregation: 5 phút
            df_aggregated = (df_clean
            .withWatermark("timestamp", "10 minutes")
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("product_id")
            )
            .agg(
                count("action").cast(LongType()).alias("click_count"),
                count("user_id").cast(LongType()).alias("unique_users")
            ))

            # Tách window struct thành 2 cột riêng
            df_fixed = (df_aggregated
                        .withColumn("window_start", col("window.start"))
                        .withColumn("window_end", col("window.end"))
                        .drop("window"))

            # Lấy top N sản phẩm hot
            df_top_n = (df_fixed
                        .orderBy(desc("click_count"))
                        .limit(self.top_n))

            logger.info(f"Hoàn thành biến đổi stream, lấy top {self.top_n} sản phẩm")
            return df_top_n
        except Exception as e:
            logger.error(f"Lỗi trong quá trình biến đổi stream: {e}")
            raise


    def write_stream(self, df_transformed: DataFrame) -> None:
        """Sink kết quả vào console và PostgreSQL"""
        try:
            db_props = self.config.get_db_properties()

            # Sink vào console
            console_query = (df_transformed.writeStream
                             .outputMode("complete")
                             .format("console")
                             .option("truncate", False)
                             .start())

            # Sink vào PostgreSQL
            def write_to_postgres(df: DataFrame, batch_id: int) -> None:
                try:
                    df.write.jdbc(
                        url=db_props['url'],
                        table="hot_products_stream",
                        mode="overwrite",
                        properties={
                            "user": db_props['user'],
                            "password": db_props['password'],
                            "driver": db_props['driver']
                        }
                    )
                    logger.info(f"Batch {batch_id}: Ghi thành công vào PostgreSQL")
                except Exception as e:
                    logger.error(f"Lỗi khi ghi batch {batch_id} vào PostgreSQL: {e}")

            postgres_query = (df_transformed.writeStream
                              .outputMode("complete")
                              .foreachBatch(write_to_postgres)
                              .start())

            # Chờ stream hoàn thành
            console_query.awaitTermination()
            postgres_query.awaitTermination()
        except Exception as e:
            logger.error(f"Lỗi khi sink stream: {e}")
            raise

    def run(self) -> None:
        """Chạy pipeline streaming"""
        try:
            df_stream = self.read_stream()
            df_transformed = self.transform_stream(df_stream)
            self.write_stream(df_transformed)
        except Exception as e:
            logger.error(f"Lỗi trong pipeline streaming: {e}")
            self.spark.stop()
            raise


if __name__ == "__main__":
    config = StreamingETLConfig()
    pipeline = StreamingETLPipeline(config, top_n=5)
    pipeline.run()