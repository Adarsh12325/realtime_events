"""
Configuration module for the Event Producer Service.
Loads configuration parameters from environment variables with safe production-ready defaults.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProducerConfig:
    """Immutable configuration container for the Kafka Producer service."""
    KAFKA_BROKER_URL: str = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "raw_events")
    HOST: str = os.getenv("PRODUCER_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PRODUCER_PORT", "5000"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    KAFKA_RETRIES: int = int(os.getenv("KAFKA_RETRIES", "5"))
    KAFKA_RETRY_BACKOFF_MS: int = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "2000"))
    REQUEST_TIMEOUT_SECONDS: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10.0"))


def load_config() -> ProducerConfig:
    """Load and return current configuration instance."""
    return ProducerConfig()
