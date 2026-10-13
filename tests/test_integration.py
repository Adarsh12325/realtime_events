"""
End-to-end integration tests for the Event Ingestion Pipeline.
Validates the complete pipeline flow: API Ingest -> Kafka Broker Queue -> Consumer Processing -> Enriched Log Sink.
"""

import json
import os
import queue
import time
from unittest.mock import MagicMock
from collections import namedtuple
import pytest

from producer.app import create_app
from producer.config import ProducerConfig
from producer.kafka_producer import ResilientKafkaProducer
from consumer.app import EventConsumerService
from consumer.config import ConsumerConfig

MockRecord = namedtuple("MockRecord", ["topic", "partition", "offset", "value"])


class InMemoryKafkaBroker:
    """
    Lightweight in-memory message broker queue simulating Kafka topic partitions
    for rapid, deterministic integration testing without external daemons.
    """

    def __init__(self, topic: str = "raw_events"):
        self.topic = topic
        self._queue = queue.Queue()
        self._offset_counter = 0

    def send(self, topic: str, key: str, value: dict):
        self._offset_counter += 1
        record = MockRecord(
            topic=topic,
            partition=0,
            offset=self._offset_counter,
            value=value
        )
        self._queue.put(record)

        # Mock future returning metadata
        future = MagicMock()
        future.get.return_value = MagicMock(
            topic=topic,
            partition=0,
            offset=self._offset_counter,
            timestamp=int(time.time() * 1000)
        )
        return future

    def __iter__(self):
        while not self._queue.empty():
            yield self._queue.get_nowait()


class TestEndToEndPipelineIntegration:
    """
    Integration test suite demonstrating event journey across Producer and Consumer components.
    """

    def test_end_to_end_pipeline_flow(self, tmp_path, valid_event):
        """
        Tests complete event lifecycle:
        1. Client POSTs valid telemetry payload to Producer /ingest endpoint.
        2. Producer validates against JSON Schema and queues message.
        3. Consumer subscribes to topic, polls message, applies transformation.
        4. Transformed event is verified in the output persistence sink.
        """
        sink_file = str(tmp_path / "integration_sink.log")
        broker = InMemoryKafkaBroker(topic="raw_events")

        # 1. Setup Producer with Broker
        mock_raw_producer = MagicMock()
        mock_raw_producer.send.side_effect = broker.send
        mock_raw_producer.bootstrap_connected.return_value = True

        producer_wrapper = ResilientKafkaProducer(
            bootstrap_servers="localhost:9092",
            custom_producer=mock_raw_producer
        )

        prod_cfg = ProducerConfig(KAFKA_TOPIC="raw_events")
        producer_app = create_app(producer_instance=producer_wrapper, config=prod_cfg)
        client = producer_app.test_client()

        # 2. Ingest valid telemetry event via API
        response = client.post(
            "/ingest",
            data=json.dumps(valid_event),
            content_type="application/json"
        )
        assert response.status_code == 200
        res_data = response.get_json()
        assert res_data["message"] == "Event ingested successfully"
        assert res_data["event_id"] == valid_event["event_id"]

        # 3. Setup Consumer listening on the same simulated broker
        cons_cfg = ConsumerConfig(
            KAFKA_TOPIC="raw_events",
            OUTPUT_LOG_PATH=sink_file,
            ENABLE_AUTO_COMMIT=False
        )

        mock_raw_consumer = MagicMock()
        mock_raw_consumer.__iter__.return_value = iter(broker)

        consumer_service = EventConsumerService(
            config=cons_cfg,
            custom_consumer=mock_raw_consumer
        )

        # 4. Run consumer processing
        processed_count = consumer_service.run(max_messages=1)
        assert processed_count == 1

        # 5. Verify the persistent output sink
        assert os.path.exists(sink_file)
        with open(sink_file, "r", encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 1
        enriched_event = json.loads(lines[0])

        assert enriched_event["event_id"] == valid_event["event_id"]
        assert enriched_event["sensor_data"] == valid_event["sensor_data"]
        assert enriched_event["status"] == "SUCCESS"
        assert "processed_at" in enriched_event


@pytest.mark.integration
def test_live_docker_pipeline_if_available():
    """
    Live integration test against a running Docker Compose cluster.
    Automatically skipped if Docker services are not currently active.
    """
    import urllib.request

    live_api_url = os.getenv("LIVE_PRODUCER_URL", "http://localhost:5000")
    try:
        health_req = urllib.request.urlopen(f"{live_api_url}/health", timeout=2)
        if health_req.getcode() != 200:
            pytest.skip("Live Docker Producer service is not healthy.")
    except Exception:
        pytest.skip("Docker Compose cluster not detected on localhost:5000 (normal during unit test pass).")

    # If cluster is live, send payload and verify
    payload = {
        "event_id": f"live-test-{int(time.time())}",
        "timestamp": "2026-10-13T12:00:00Z",
        "sensor_data": 45.2
    }
    req = urllib.request.Request(
        f"{live_api_url}/ingest",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.getcode() == 200
        body = json.loads(resp.read().decode("utf-8"))
        assert body["event_id"] == payload["event_id"]
