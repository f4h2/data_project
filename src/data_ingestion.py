# data_ingestion.py
import io
import json
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from minio import Minio
from faker import Faker
import pandas as pd
import os
from config import ConfigManager


class DataIngestion:
    """Quản lý quá trình thu thập và truyền dữ liệu"""

    def __init__(self, config: ConfigManager):
        self.config = config
        self.fake = Faker()
        self.minio_client = self._init_minio_client()
        self._ensure_bucket()

    def _init_minio_client(self) -> Minio:
        """Khởi tạo MinIO client"""
        minio_config = self.config.get_minio_config()
        return Minio(
            minio_config['endpoint'],
            access_key=minio_config['access_key'],
            secret_key=minio_config['secret_key'],
            secure=False
        )

    def _ensure_bucket(self) -> None:
        """Tạo bucket nếu chưa tồn tại"""
        minio_config = self.config.get_minio_config()
        if not self.minio_client.bucket_exists(minio_config['bucket']):
            self.minio_client.make_bucket(minio_config['bucket'])

    def produce_clickstream(self, num_events: int = 100) -> None:
        """Tạo và gửi sự kiện clickstream tới Kafka"""
        kafka_config = self.config.get_kafka_config()
        producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=60000,
            retry_backoff_ms=500,
            max_block_ms=60000
        )

        for _ in range(num_events):
            event = {
                'user_id': self.fake.uuid4(),
                'product_id': self.fake.random_int(min=1, max=1000),
                'action': self.fake.random_element(elements=('click', 'search', 'view')),
                'timestamp': datetime.utcnow().isoformat(),
                'session_id': self.fake.uuid4()
            }
            producer.send(kafka_config['topic'], event)
            print(f"Đã gửi: {event}")
            time.sleep(0.1)
        producer.flush()

    def consume_clickstream(self) -> None:
        """Đọc từ Kafka và lưu vào MinIO"""
        kafka_config = self.config.get_kafka_config()
        minio_config = self.config.get_minio_config()
        consumer = KafkaConsumer(
            kafka_config['topic'],
            bootstrap_servers=kafka_config['bootstrap_servers'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            request_timeout_ms=60000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )

        for message in consumer:
            event = message.value
            timestamp = datetime.fromisoformat(event['timestamp'])
            partition_path = f"dt={timestamp.strftime('%Y-%m-%d-%H')}/event_{message.offset}.json"

            event_json = json.dumps(event).encode('utf-8')
            self.minio_client.put_object(
                minio_config['bucket'],
                partition_path,
                data=io.BytesIO(event_json),
                length=len(event_json),
                content_type='application/json'
            )
            print(f"Đã lưu vào MinIO: {partition_path}")

    def batch_ingestion(self, num_records: int = 100) -> None:
        """Tạo và xử lý dữ liệu batch"""
        minio_config = self.config.get_minio_config()
        data = [{
            'user_id': self.fake.uuid4(),
            'product_id': self.fake.random_int(min=1, max=1000),
            'action': self.fake.random_element(elements=('click', 'search', 'view')),
            'timestamp': self.fake.date_time_this_year().isoformat(),
            'session_id': self.fake.uuid4()
        } for _ in range(num_records)]

        df = pd.DataFrame(data)
        csv_path = f"fake_clickstream_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(csv_path, index=False)

        # Gửi tới Kafka
        self.produce_clickstream(num_records)

        # Lưu CSV vào MinIO
        with open(csv_path, 'rb') as file:
            self.minio_client.put_object(
                minio_config['bucket'],
                f"batch/{csv_path}",
                data=file,
                length=os.path.getsize(csv_path),
                content_type='text/csv'
            )
        print(f"Đã lưu CSV vào MinIO: batch/{csv_path}")


if __name__ == "__main__":
    config = ConfigManager()
    ingestion = DataIngestion(config)
    ingestion.produce_clickstream()
    ingestion.consume_clickstream()