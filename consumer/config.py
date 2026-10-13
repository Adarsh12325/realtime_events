"""
Configuration module for the Event Consumer Service.
Loads consumer parameters from environment variables with safe production-ready defaults.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumerConfig:
    """Immutable configuration container for the Kafka Consumer service."""
    KAFKA_BROKER_URL: str = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "raw_events")
    CONSUMER_GROUP_ID: str = os.getenv("CONSUMER_GROUP_ID", "event_processing_group")
    AUTO_OFFSET_RESET: str = os.getenv("AUTO_OFFSET_RESET", "earliest")
    ENABLE_AUTO_COMMIT: bool = os.getenv("ENABLE_AUTO_COMMIT", "true").lower() in ("true", "1", "yes")
    OUTPUT_LOG_PATH: str = os.getenv("OUTPUT_LOG_PATH", "processed_events.log")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    KAFKA_RETRIES: int = int(os.getenv("KAFKA_RETRIES", "10"))
    KAFKA_RETRY_BACKOFF_MS: int = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "2000"))
    POLL_TIMEOUT_MS: int = int(os.getenv("POLL_TIMEOUT_MS", "1000"))


def load_config() -> ConsumerConfig:
    """Load and return current consumer configuration."""
    return ConsumerConfig()
