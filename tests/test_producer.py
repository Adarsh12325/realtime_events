"""
Unit tests for the Event Producer API service and schema validation.
"""

import json
from unittest.mock import MagicMock
from kafka.errors import KafkaTimeoutError

from producer.schemas import validate_event, EVENT_SCHEMA
from producer.config import ProducerConfig
from producer.app import create_app


class TestProducerSchemaValidation:
    """Tests the canonical JSON schema and validation function directly."""

    def test_valid_payload_passes_validation(self, valid_event):
        is_valid, error = validate_event(valid_event)
        assert is_valid is True
        assert error is None

    def test_missing_required_property_fails(self, invalid_event_missing_field):
        is_valid, error = validate_event(invalid_event_missing_field)
        assert is_valid is False
        assert "sensor_data" in error

    def test_invalid_data_types_fail(self, invalid_event_wrong_types):
        is_valid, error = validate_event(invalid_event_wrong_types)
        assert is_valid is False
        assert error is not None

    def test_disallowed_additional_properties_fail(self, invalid_event_extra_properties):
        is_valid, error = validate_event(invalid_event_extra_properties)
        assert is_valid is False
        assert "unexpected_field" in error or "additional" in error.lower()

    def test_non_dict_payload_fails(self):
        is_valid, error = validate_event(["not", "a", "dictionary"])
        assert is_valid is False
        assert "JSON object" in error


class TestProducerEndpoints:
    """Tests the REST API routes exposed by the Event Producer service."""

    def test_root_endpoint(self, client):
        response = client.get("/")
        assert response.status_code == 200
        data = response.get_json()
        assert data["service"] == "event-producer-api"
        assert "endpoints" in data

    def test_health_endpoint_healthy(self, client, mock_resilient_producer):
        mock_resilient_producer.is_healthy.return_value = True
        response = client.get("/health")
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "healthy"
        assert data["kafka_connected"] is True

    def test_health_endpoint_degraded(self, client, mock_resilient_producer):
        mock_resilient_producer.is_healthy.return_value = False
        response = client.get("/health")
        assert response.status_code == 503
        data = response.get_json()
        assert data["status"] == "degraded"
        assert data["kafka_connected"] is False

    def test_schema_endpoint(self, client):
        response = client.get("/schema")
        assert response.status_code == 200
        data = response.get_json()
        assert data["title"] == "SensorEvent"
        assert "properties" in data

    def test_ingest_valid_event_success(self, client, valid_event, mock_resilient_producer):
        response = client.post(
            "/ingest",
            data=json.dumps(valid_event),
            content_type="application/json"
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["message"] == "Event ingested successfully"
        assert data["event_id"] == valid_event["event_id"]
        assert data["partition"] == 0
        assert data["offset"] == 42
        mock_resilient_producer.publish_event.assert_called_once()

    def test_ingest_non_json_request_rejected(self, client):
        response = client.post(
            "/ingest",
            data="plain text payload",
            content_type="text/plain"
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "Request must be JSON" in data["error"]

    def test_ingest_missing_required_field_rejected(self, client, invalid_event_missing_field):
        response = client.post(
            "/ingest",
            data=json.dumps(invalid_event_missing_field),
            content_type="application/json"
        )
        assert response.status_code == 400
        data = response.get_json()
        assert data["error"] == "Invalid event schema"
        assert "sensor_data" in data["details"]

    def test_ingest_invalid_types_rejected(self, client, invalid_event_wrong_types):
        response = client.post(
            "/ingest",
            data=json.dumps(invalid_event_wrong_types),
            content_type="application/json"
        )
        assert response.status_code == 400
        data = response.get_json()
        assert data["error"] == "Invalid event schema"

    def test_ingest_uninitialized_producer_returns_500(self, valid_event):
        broken_prod = MagicMock()
        broken_prod.is_healthy.return_value = False
        app = create_app(producer_instance=broken_prod)
        test_client = app.test_client()

        response = test_client.post(
            "/ingest",
            data=json.dumps(valid_event),
            content_type="application/json"
        )
        assert response.status_code == 500
        data = response.get_json()
        assert "Kafka producer not initialized" in data["error"]

    def test_ingest_kafka_publish_exception_returns_500(self, client, valid_event, mock_resilient_producer):
        mock_resilient_producer.publish_event.side_effect = KafkaTimeoutError("Broker timed out")
        response = client.post(
            "/ingest",
            data=json.dumps(valid_event),
            content_type="application/json"
        )
        assert response.status_code == 500
        data = response.get_json()
        assert "Failed to publish event to Kafka" in data["error"]
        assert "Broker timed out" in data["details"]
