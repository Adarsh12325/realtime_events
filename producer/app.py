"""
Event Producer Service API.
Exposes REST endpoints to receive, validate, and publish real-time telemetry events to Apache Kafka.
"""

import logging
import signal
import sys
from typing import Optional
from flask import Flask, request, jsonify

from producer.config import ProducerConfig, load_config
from producer.schemas import validate_event, EVENT_SCHEMA
from producer.kafka_producer import ResilientKafkaProducer

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("event_producer.app")


def create_app(
    producer_instance: Optional[ResilientKafkaProducer] = None,
    config: Optional[ProducerConfig] = None,
    auto_connect: bool = True,
) -> Flask:
    """
    Application factory for the Event Producer API.

    Args:
        producer_instance: Optional pre-configured ResilientKafkaProducer (for testing).
        config: Optional ProducerConfig instance.
        auto_connect: Whether to initiate Kafka connection immediately on app creation.

    Returns:
        Configured Flask application instance.
    """
    app = Flask(__name__)
    cfg = config or load_config()
    app.config["APP_CONFIG"] = cfg

    # Set logger level based on configuration
    logging.getLogger().setLevel(getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO))

    # Initialize Kafka producer wrapper
    if producer_instance is not None:
        producer = producer_instance
    else:
        producer = ResilientKafkaProducer(
            bootstrap_servers=cfg.KAFKA_BROKER_URL,
            retries=cfg.KAFKA_RETRIES,
            retry_backoff_ms=cfg.KAFKA_RETRY_BACKOFF_MS,
            auto_connect=auto_connect,
        )

    app.config["KAFKA_PRODUCER"] = producer

    # Register graceful shutdown hooks
    def _graceful_shutdown(signum, frame):
        sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        logger.info(f"Received shutdown signal {sig_name}. Cleaning up resources...")
        prod = app.config.get("KAFKA_PRODUCER")
        if prod:
            prod.close()
        sys.exit(0)

    try:
        signal.signal(signal.SIGTERM, _graceful_shutdown)
        signal.signal(signal.SIGINT, _graceful_shutdown)
    except (ValueError, AttributeError):
        # In non-main thread execution, signal handlers cannot be registered
        pass

    @app.route("/", methods=["GET"])
    def root():
        """Root discovery endpoint providing service metadata."""
        return jsonify({
            "service": "event-producer-api",
            "version": "1.0.0",
            "status": "online",
            "endpoints": {
                "health": "/health",
                "ingest": "/ingest [POST]",
                "schema": "/schema [GET]"
            }
        }), 200

    @app.route("/health", methods=["GET"])
    def health_check():
        """
        Healthcheck probe endpoint verifying service liveness and Kafka connectivity.
        """
        prod: ResilientKafkaProducer = app.config.get("KAFKA_PRODUCER")
        is_kafka_up = prod.is_healthy() if prod else False

        status_code = 200 if is_kafka_up else 503
        return jsonify({
            "status": "healthy" if is_kafka_up else "degraded",
            "service": "event_producer",
            "kafka_connected": is_kafka_up,
            "target_topic": cfg.KAFKA_TOPIC,
            "broker_url": cfg.KAFKA_BROKER_URL
        }), status_code

    @app.route("/schema", methods=["GET"])
    def get_schema():
        """Returns the canonical JSON schema for ingested events."""
        return jsonify(EVENT_SCHEMA), 200

    @app.route("/ingest", methods=["POST"])
    def ingest_event():
        """
        Ingests a single telemetry event.
        Validates the JSON payload against canonical schema and publishes to Kafka.
        """
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400

        event_data = request.get_json(silent=True)
        if event_data is None:
            return jsonify({"error": "Request body could not be parsed as valid JSON"}), 400

        # Schema validation
        is_valid, validation_error = validate_event(event_data)
        if not is_valid:
            logger.warning(f"Payload validation failed: {validation_error}")
            return jsonify({
                "error": "Invalid event schema",
                "details": validation_error
            }), 400

        # Producer connection check
        prod: ResilientKafkaProducer = app.config.get("KAFKA_PRODUCER")
        if not prod or not prod.is_healthy():
            logger.error("Kafka producer is not initialized or broker is unreachable")
            return jsonify({"error": "Kafka producer not initialized"}), 500

        # Synchronous publish
        try:
            metadata = prod.publish_event(
                topic=cfg.KAFKA_TOPIC,
                event_data=event_data,
                timeout=cfg.REQUEST_TIMEOUT_SECONDS,
            )
            event_id = event_data.get("event_id")
            logger.info(
                f"Successfully ingested event '{event_id}' (partition={metadata['partition']}, offset={metadata['offset']})"
            )
            return jsonify({
                "message": "Event ingested successfully",
                "event_id": event_id,
                "topic": metadata["topic"],
                "partition": metadata["partition"],
                "offset": metadata["offset"]
            }), 200

        except Exception as exc:
            logger.error(f"Failed to publish event to Kafka: {exc}", exc_info=True)
            return jsonify({
                "error": "Failed to publish event to Kafka",
                "details": str(exc)
            }), 500

    return app


# Lazy app instance for WSGI / Docker servers (connects when service starts)
app = create_app(auto_connect=False)

if __name__ == "__main__":
    config = load_config()
    logger.info(f"Connecting to Kafka brokers at {config.KAFKA_BROKER_URL}...")
    producer = app.config.get("KAFKA_PRODUCER")
    if producer and not producer.is_healthy():
        producer.connect()

    logger.info(f"Starting Event Producer API on {config.HOST}:{config.PORT}...")
    app.run(host=config.HOST, port=config.PORT, debug=False)
