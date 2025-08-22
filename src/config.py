# config.py
import yaml
from typing import Dict, Any


class ConfigManager:
    """Quản lý cấu hình cho toàn bộ hệ thống"""
    def __init__(self, config_path: str = "./config/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

    def get_kafka_config(self) -> Dict[str, Any]:
        return self.config.get('kafka', {})

    def get_minio_config(self) -> Dict[str, Any]:
        return self.config.get('minio', {})