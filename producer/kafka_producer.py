"""
Kafka Producer client wrapper module.
Handles resilient connection initialization, message serialization, and publishing with delivery confirmation.
"""

import json
import logging
import time
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger("event_producer.kafka")


class ResilientKafkaProducer:
    """
    Wrapper around kafka-python's KafkaProducer providing retry logic,
    health tracking, and clean resource teardown.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        retries: int = 5,
        retry_backoff_ms: int = 2000,
        custom_producer: Optional[KafkaProducer] = None,
        auto_connect: bool = True,
    ):
        """
        Initializes the Kafka producer with connection retries.

        Args:
            bootstrap_servers: Comma-separated list of broker host:port strings.
            retries: Number of connection attempts before failing.
            retry_backoff_ms: Milliseconds to wait between connection attempts.
            custom_producer: Optional pre-configured producer instance (e.g. for unit testing).
            auto_connect: Whether to immediately initiate broker handshake on instantiation.
        """
        self.bootstrap_servers = bootstrap_servers
        self.retries = retries
        self.retry_backoff_ms = retry_backoff_ms
        self._producer: Optional[KafkaProducer] = custom_producer

        if self._producer is None and auto_connect:
            self.connect()

    def connect(self) -> bool:
        """Attempts connection to the Kafka cluster with backoff."""
        if self._producer is not None:
            return True

        delay = self.retry_backoff_ms / 1000.0
        for attempt in range(1, self.retries + 1):
            try:
                logger.info(
                    f"Attempting to connect to Kafka at {self.bootstrap_servers} (Attempt {attempt}/{self.retries})..."
                )
                self._producer = KafkaProducer(
                    bootstrap_servers=[s.strip() for s in self.bootstrap_servers.split(",")],
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                    acks="all",
                    retries=3,
                    max_block_ms=10000,
                )
                logger.info(f"Successfully connected to Kafka brokers at {self.bootstrap_servers}")
                return True
            except Exception as exc:
                logger.warning(
                    f"Connection attempt {attempt} failed: {exc}. Retrying in {delay:.1f}s..."
                )
                if attempt < self.retries:
                    time.sleep(delay)
                    delay = min(delay * 1.5, 10.0)
                else:
                    logger.error("Exhausted all retries connecting to Kafka.")
                    self._producer = None
                    return False
        return False

    def is_healthy(self) -> bool:
        """Checks if the underlying Kafka producer is instantiated and active."""
        if self._producer is None:
            return False
        try:
            return self._producer.bootstrap_connected()
        except Exception:
            return False

    def publish_event(
        self, topic: str, event_data: Dict[str, Any], timeout: float = 10.0
    ) -> Dict[str, Any]:
        """
        Publishes a validated event to the designated Kafka topic synchronously.

        Args:
            topic: Target Kafka topic name.
            event_data: The validated dictionary payload.
            timeout: Maximum seconds to block waiting for broker ACK.

        Returns:
            Dictionary containing published record metadata (topic, partition, offset).

        Raises:
            RuntimeError: If producer is not connected.
            KafkaError: If broker fails to accept the record within timeout.
        """
        if self._producer is None:
            if not self.connect():
                raise RuntimeError(
                    f"Kafka producer is not initialized or unreachable at {self.bootstrap_servers}"
                )

        event_id = event_data.get("event_id", "")
        key = str(event_id)

        try:
            future = self._producer.send(topic=topic, key=key, value=event_data)
            record_metadata = future.get(timeout=timeout)
            logger.info(
                f"Published event_id='{event_id}' -> topic='{record_metadata.topic}', "
                f"partition={record_metadata.partition}, offset={record_metadata.offset}"
            )
            return {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
                "timestamp": getattr(record_metadata, "timestamp", None),
            }
        except KafkaError as kerr:
            logger.error(f"Kafka error while publishing event_id='{event_id}': {kerr}")
            raise
        except Exception as exc:
            logger.error(f"Unexpected error publishing event_id='{event_id}': {exc}")
            raise

    def close(self, timeout: float = 5.0) -> None:
        """Flushes buffered messages and closes the underlying producer cleanly."""
        if self._producer is not None:
            logger.info("Flushing and shutting down Kafka producer...")
            try:
                self._producer.flush(timeout=timeout)
                self._producer.close(timeout=timeout)
                logger.info("Kafka producer successfully closed.")
            except Exception as exc:
                logger.warning(f"Error during Kafka producer shutdown: {exc}")
            finally:
                self._producer = None
