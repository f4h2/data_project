import io
import json
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from minio import Minio
from faker import Faker
import pandas as pd
import os
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET
)

# Initialize Faker for generating fake data
fake = Faker()

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Create bucket if not exists
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

# Producer: Simulate clickstream events and send to Kafka
def produce_clickstream():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=60000,
        retry_backoff_ms=500,
        max_block_ms=60000
    )

    for _ in range(100):  # Simulate 100 events
        event = {
            'user_id': fake.uuid4(),
            'product_id': fake.random_int(min=1, max=1000),
            'action': fake.random_element(elements=('click', 'search')),
            'timestamp': datetime.utcnow().isoformat()
        }
        producer.send(KAFKA_TOPIC, event)
        print(f"Produced: {event}")
        time.sleep(0.1)  # Simulate real-time events
    producer.flush()

# Consumer: Read from Kafka and store to MinIO
def consume_clickstream():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
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

        # Save to MinIO
        event_json = json.dumps(event).encode('utf-8')
        minio_client.put_object(
            MINIO_BUCKET,
            partition_path,
            data=io.BytesIO(event_json),
            length=len(event_json),
            content_type='application/json'
        )
        print(f"Stored to MinIO: {partition_path}")

# Batch Ingestion: Generate fake CSV and push to Kafka or MinIO
def batch_ingestion():
    # Generate fake CSV
    data = [{
        'user_id': fake.uuid4(),
        'product_id': fake.random_int(min=1, max=1000),
        'action': fake.random_element(elements=('click', 'search')),
        'timestamp': fake.date_time_this_year().isoformat()
    } for _ in range(100)]

    df = pd.DataFrame(data)
    csv_path = 'fake_clickstream.csv'
    df.to_csv(csv_path, index=False)

    # Push to Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=60000,
        retry_backoff_ms=500,
        max_block_ms=60000
    )

    for _, row in df.iterrows():
        event = row.to_dict()
        producer.send(KAFKA_TOPIC, event)
        print(f"Batch produced: {event}")
    producer.flush()

    # Optionally, store CSV to MinIO
    with open(csv_path, 'rb') as file:
        minio_client.put_object(
            MINIO_BUCKET,
            f"batch/csv_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv",
            data=file,
            length=os.path.getsize(csv_path),
            content_type='text/csv'
        )
    print(f"Stored CSV to MinIO")


def ensure_topic():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='clickstream-admin'
    )
    try:
        admin_client.create_topics([
            NewTopic(
                name=KAFKA_TOPIC,
                num_partitions=3,
                replication_factor=1
            )
        ])
        print(f"Created topic {KAFKA_TOPIC}")
    except Exception as e:
        print(f"Topic exists or error: {e}")

if __name__ == "__main__":
    ensure_topic()
    # Run producer
    # produce_clickstream()

    # Run consumer in a separate terminal/process
    consume_clickstream()

    # Run batch ingestion
    # batch_ingestion()