"""
Shared Pytest fixtures for the Event Ingestion Pipeline test suite.
"""

import sys
import os
from unittest.mock import MagicMock
import pytest

# Ensure root realtime_events directory is on Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from producer.app import create_app
from producer.config import ProducerConfig
from producer.kafka_producer import ResilientKafkaProducer


@pytest.fixture
def valid_event():
    """Provides a canonical valid telemetry event dictionary."""
    return {
        "event_id": "evt-sensor-7890",
        "timestamp": "2026-10-13T14:30:00Z",
        "sensor_data": 23.85
    }


@pytest.fixture
def invalid_event_missing_field():
    """Provides an invalid event missing required 'sensor_data' field."""
    return {
        "event_id": "evt-sensor-missing",
        "timestamp": "2026-10-13T14:30:00Z"
    }


@pytest.fixture
def invalid_event_wrong_types():
    """Provides an invalid event with non-numeric sensor_data and ill-formatted timestamp."""
    return {
        "event_id": "evt-sensor-wrong",
        "timestamp": "not-an-iso-timestamp",
        "sensor_data": "high_temperature"
    }


@pytest.fixture
def invalid_event_extra_properties():
    """Provides an event with unrecognized additional properties."""
    return {
        "event_id": "evt-extra-props",
        "timestamp": "2026-10-13T14:30:00Z",
        "sensor_data": 15.0,
        "unexpected_field": "disallowed"
    }


@pytest.fixture
def mock_resilient_producer():
    """Provides a mocked ResilientKafkaProducer instance."""
    mock_prod = MagicMock(spec=ResilientKafkaProducer)
    mock_prod.is_healthy.return_value = True
    mock_prod.publish_event.return_value = {
        "topic": "raw_events",
        "partition": 0,
        "offset": 42,
        "timestamp": 1728820000000
    }
    return mock_prod


@pytest.fixture
def app_with_mock_producer(mock_resilient_producer):
    """Provides a Flask application initialized with a mocked Kafka producer."""
    cfg = ProducerConfig(
        KAFKA_BROKER_URL="localhost:9092",
        KAFKA_TOPIC="raw_events",
        LOG_LEVEL="INFO"
    )
    test_app = create_app(producer_instance=mock_resilient_producer, config=cfg)
    test_app.config["TESTING"] = True
    return test_app


@pytest.fixture
def client(app_with_mock_producer):
    """Provides a Flask test client."""
    return app_with_mock_producer.test_client()
