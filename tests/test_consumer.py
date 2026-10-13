"""
Unit tests for the Event Consumer processing logic, schema validation, and Kafka interactions.
"""

import json
import os
from unittest.mock import MagicMock
from collections import namedtuple

from consumer.processor import process_event, write_to_sink
from consumer.app import safe_json_deserializer, EventConsumerService
from consumer.config import ConsumerConfig

# Mock Kafka ConsumerRecord object
MockRecord = namedtuple("MockRecord", ["topic", "partition", "offset", "value"])


class TestConsumerProcessor:
    """Tests the message processing and transformation logic."""

    def test_process_valid_event_success(self, valid_event):
        result = process_event(dict(valid_event))
        assert result["status"] == "SUCCESS"
        assert "processed_at" in result
        assert result["event_id"] == valid_event["event_id"]
        assert result["sensor_data"] == valid_event["sensor_data"]

    def test_process_invalid_event_validation_failed(self, invalid_event_missing_field):
        result = process_event(dict(invalid_event_missing_field))
        assert result["status"] == "FAILED_VALIDATION"
        assert "error_details" in result
        assert "sensor_data" in result["error_details"]

    def test_process_non_dict_event_failed(self):
        result = process_event("not-a-dict")
        assert result["status"] == "FAILED_PROCESSING"
        assert "error_details" in result


class TestConsumerDeserializer:
    """Tests byte deserialization handling."""

    def test_deserialize_valid_json_bytes(self):
        payload = {"event_id": "test-123", "sensor_data": 99.9}
        raw_bytes = json.dumps(payload).encode("utf-8")
        result = safe_json_deserializer(raw_bytes)
        assert result == payload

    def test_deserialize_corrupted_bytes_returns_none(self):
        corrupted = b"\x80\x01\x02not-valid-json"
        result = safe_json_deserializer(corrupted)
        assert result is None

    def test_deserialize_none_returns_none(self):
        assert safe_json_deserializer(None) is None


class TestConsumerSinkWriter:
    """Tests writing processed events to file sink."""

    def test_write_to_sink_file(self, tmp_path):
        sink_file = tmp_path / "processed_test.log"
        sample_event = {
            "event_id": "sink-1",
            "timestamp": "2026-10-13T12:00:00Z",
            "sensor_data": 12.3,
            "status": "SUCCESS",
            "processed_at": "2026-10-13T12:00:01Z"
        }
        success = write_to_sink(sample_event, str(sink_file))
        assert success is True
        assert os.path.exists(sink_file)

        with open(sink_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 1
        recorded = json.loads(lines[0])
        assert recorded["event_id"] == "sink-1"


class TestConsumerServiceLoop:
    """Tests the consumer message polling loop with mocked KafkaConsumer."""

    def test_consumer_processes_stream_and_stops(self, tmp_path, valid_event):
        sink_file = str(tmp_path / "stream_events.log")
        cfg = ConsumerConfig(
            KAFKA_BROKER_URL="localhost:9092",
            KAFKA_TOPIC="raw_events",
            OUTPUT_LOG_PATH=sink_file,
            ENABLE_AUTO_COMMIT=False
        )

        mock_records = [
            MockRecord(topic="raw_events", partition=0, offset=1, value=dict(valid_event)),
            MockRecord(topic="raw_events", partition=0, offset=2, value={"event_id": "bad-2"})
        ]

        mock_kafka_consumer = MagicMock()
        mock_kafka_consumer.__iter__.return_value = iter(mock_records)

        service = EventConsumerService(config=cfg, custom_consumer=mock_kafka_consumer)
        processed_total = service.run(max_messages=2)

        assert processed_total == 2
        mock_kafka_consumer.commit.assert_called_once()
        mock_kafka_consumer.close.assert_called_once()
        assert os.path.exists(sink_file)

    def test_stop_signal_breaks_loop(self):
        service = EventConsumerService()
        service.running = True
        service.stop()
        assert service.running is False
