"""
Kafka Consumer Service Application.
Subscribes to the raw_events topic, validates and transforms incoming messages,
commits offsets, and provides graceful shutdown handling.
"""

import json
import logging
import signal
import sys
import time
from typing import Optional, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from consumer.config import ConsumerConfig, load_config
from consumer.processor import process_event, write_to_sink

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("event_consumer.app")


def safe_json_deserializer(raw_bytes: Optional[bytes]) -> Any:
    """
    Safely decodes and deserializes raw bytes into Python objects.
    Returns None if decoding or JSON parsing fails.
    """
    if raw_bytes is None:
        return None
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:
        logger.error(f"Failed to deserialize message bytes: {exc}. Raw bytes: {raw_bytes!r}")
        return None


class EventConsumerService:
    """
    Manages Kafka consumer lifecycle, message consumption loop,
    graceful shutdown, and offset commits.
    """

    def __init__(
        self,
        config: Optional[ConsumerConfig] = None,
        custom_consumer: Optional[KafkaConsumer] = None,
    ):
        """
        Initializes the consumer service.

        Args:
            config: Optional ConsumerConfig instance.
            custom_consumer: Optional pre-configured KafkaConsumer (for testing).
        """
        self.config = config or load_config()
        self.running = False
        self._consumer: Optional[KafkaConsumer] = custom_consumer

        # Update log level
        logging.getLogger().setLevel(
            getattr(logging, self.config.LOG_LEVEL.upper(), logging.INFO)
        )

        # Register OS signal handlers for graceful shutdown
        self._register_signals()

    def _register_signals(self) -> None:
        """Hooks SIGTERM and SIGINT to trigger clean shutdown."""
        def _signal_handler(signum, frame):
            sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
            logger.info(f"Received shutdown signal {sig_name}. Stopping consumer loop...")
            self.stop()

        try:
            signal.signal(signal.SIGTERM, _signal_handler)
            signal.signal(signal.SIGINT, _signal_handler)
        except (ValueError, AttributeError):
            pass

    def stop(self) -> None:
        """Signals the consumer loop to exit."""
        self.running = False

    def connect(self) -> bool:
        """
        Establishes connection to the Kafka broker with retry backoff.

        Returns:
            bool: True if connection was successful, False otherwise.
        """
        if self._consumer is not None:
            return True

        delay = self.config.KAFKA_RETRY_BACKOFF_MS / 1000.0
        for attempt in range(1, self.config.KAFKA_RETRIES + 1):
            try:
                logger.info(
                    f"Connecting Kafka Consumer to {self.config.KAFKA_BROKER_URL} "
                    f"[Topic: {self.config.KAFKA_TOPIC}, Group: {self.config.CONSUMER_GROUP_ID}] "
                    f"(Attempt {attempt}/{self.config.KAFKA_RETRIES})..."
                )
                self._consumer = KafkaConsumer(
                    self.config.KAFKA_TOPIC,
                    bootstrap_servers=[
                        s.strip() for s in self.config.KAFKA_BROKER_URL.split(",")
                    ],
                    auto_offset_reset=self.config.AUTO_OFFSET_RESET,
                    enable_auto_commit=self.config.ENABLE_AUTO_COMMIT,
                    group_id=self.config.CONSUMER_GROUP_ID,
                    value_deserializer=safe_json_deserializer,
                    consumer_timeout_ms=self.config.POLL_TIMEOUT_MS,
                )
                logger.info("Kafka Consumer connected successfully.")
                return True
            except Exception as exc:
                logger.warning(f"Connection attempt {attempt} failed: {exc}. Retrying in {delay:.1f}s...")
                if attempt < self.config.KAFKA_RETRIES:
                    time.sleep(delay)
                    delay = min(delay * 1.5, 10.0)
                else:
                    logger.error("Exhausted all retries connecting consumer to Kafka.")
                    return False
        return False

    def run(self, max_messages: Optional[int] = None) -> int:
        """
        Executes the main consumption loop.

        Args:
            max_messages: Optional limit on messages to process (useful for integration testing).

        Returns:
            int: Total count of messages processed.
        """
        if self._consumer is None:
            if not self.connect():
                logger.error("Unable to start consumer: broker connection failed.")
                return 0

        self.running = True
        processed_count = 0
        logger.info(
            f"Consumer active and listening for events on topic '{self.config.KAFKA_TOPIC}'..."
        )

        try:
            while self.running:
                # Poll message batches or iterate
                try:
                    for message in self._consumer:
                        if not self.running:
                            break

                        logger.info(
                            f"Received message from topic={message.topic}, "
                            f"partition={message.partition}, offset={message.offset}"
                        )

                        event_data = message.value
                        if event_data is not None:
                            transformed = process_event(event_data)
                            write_to_sink(transformed, self.config.OUTPUT_LOG_PATH)
                            processed_count += 1
                        else:
                            logger.error(
                                f"Skipping un-deserializable or empty message at "
                                f"partition={message.partition}, offset={message.offset}"
                            )

                        if max_messages and processed_count >= max_messages:
                            logger.info(f"Reached target message limit ({max_messages}). Exiting loop.")
                            self.stop()
                            break

                except StopIteration:
                    # Occurs when consumer_timeout_ms expires with no new messages
                    continue
                except KafkaError as kerr:
                    logger.error(f"Kafka consumer error: {kerr}")
                    time.sleep(1.0)
                except Exception as exc:
                    logger.error(f"Unexpected error in consumption loop: {exc}", exc_info=True)
                    time.sleep(1.0)

        finally:
            self.cleanup()

        return processed_count

    def cleanup(self) -> None:
        """Safely commits offsets and closes consumer resources."""
        if self._consumer is not None:
            logger.info("Committing offsets and closing Kafka Consumer...")
            try:
                if not self.config.ENABLE_AUTO_COMMIT:
                    self._consumer.commit()
                self._consumer.close()
                logger.info("Kafka Consumer cleanly shut down.")
            except Exception as exc:
                logger.warning(f"Error during consumer shutdown: {exc}")
            finally:
                self._consumer = None


def run_consumer() -> None:
    """Entry point function executing the consumer service."""
    service = EventConsumerService()
    service.run()


if __name__ == "__main__":
    run_consumer()
