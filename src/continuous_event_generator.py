import json
import time
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
from config import ConfigManager

class EventGenerator:
    """Tạo sự kiện clickstream liên tục"""
    def __init__(self, config: ConfigManager):
        self.config = config
        self.fake = Faker()
        self.producer = self._init_producer()

    def _init_producer(self) -> KafkaProducer:
        """Khởi tạo Kafka Producer"""
        kafka_config = self.config.get_kafka_config()
        return KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=60000,
            retry_backoff_ms=500,
            max_block_ms=60000
        )

    def generate_events(self, interval: float = 0.1) -> None:
        """Tạo và gửi sự kiện liên tục"""
        try:
            kafka_config = self.config.get_kafka_config()
            while True:
                event = {
                    'user_id': self.fake.uuid4(),
                    'product_id': self.fake.random_int(min=1, max=1000),
                    'action': self.fake.random_element(elements=('click', 'search', 'view')),
                    'timestamp': datetime.utcnow().isoformat(),
                    'session_id': self.fake.uuid4()
                }
                self.producer.send(kafka_config['topic'], event)
                print(f"Đã gửi: {event}")
                self.producer.flush()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("Dừng tạo sự kiện")
            self.producer.close()
        except Exception as e:
            print(f"Lỗi khi tạo sự kiện: {e}")
            self.producer.close()

if __name__ == "__main__":
    config = ConfigManager()
    generator = EventGenerator(config)
    generator.generate_events()