"""
Event processor and business transformation logic for the Event Consumer.
Handles validation, enrichment, metadata tagging, and output persistence.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from jsonschema import validate, ValidationError

from consumer.schemas import EVENT_SCHEMA, _FORMAT_CHECKER

logger = logging.getLogger("event_consumer.processor")


def process_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates, enriches, and transforms a single deserialized event payload.

    Transformation Steps:
    1. Schema validation against canonical EVENT_SCHEMA.
    2. Enrichment: appends 'processed_at' UTC timestamp.
    3. Status tagging: assigns 'SUCCESS', 'FAILED_VALIDATION', or 'FAILED_PROCESSING'.

    Args:
        event_data: The incoming dictionary representation of the event.

    Returns:
        The mutated/enriched event dictionary with processing metadata.
    """
    if not isinstance(event_data, dict):
        logger.error(f"Cannot process non-dictionary event: {type(event_data)}")
        return {
            "raw_payload": str(event_data),
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "status": "FAILED_PROCESSING",
            "error_details": "Payload is not a valid dictionary"
        }

    event_id = event_data.get("event_id", "UNKNOWN_ID")

    try:
        # 1. Validate against canonical schema
        validate(instance=event_data, schema=EVENT_SCHEMA, format_checker=_FORMAT_CHECKER)

        # 2. Apply business transformations
        event_data["processed_at"] = datetime.now(timezone.utc).isoformat()
        event_data["status"] = "SUCCESS"
        logger.info(
            f"Successfully processed event_id='{event_id}' (sensor_data={event_data.get('sensor_data')})"
        )
        return event_data

    except ValidationError as val_err:
        logger.warning(f"Event validation failed for event_id='{event_id}': {val_err.message}")
        event_data["processed_at"] = datetime.now(timezone.utc).isoformat()
        event_data["status"] = "FAILED_VALIDATION"
        event_data["error_details"] = str(val_err.message)
        return event_data

    except Exception as exc:
        logger.error(f"Error processing event_id='{event_id}': {exc}", exc_info=True)
        event_data["processed_at"] = datetime.now(timezone.utc).isoformat()
        event_data["status"] = "FAILED_PROCESSING"
        event_data["error_details"] = str(exc)
        return event_data


def write_to_sink(processed_event: Dict[str, Any], sink_path: Optional[str] = None) -> bool:
    """
    Persists processed event record to a local JSON Lines sink file.

    Args:
        processed_event: Enriched event dictionary.
        sink_path: Optional file path. If None, operation is a no-op.

    Returns:
        bool: True if written successfully or not configured, False on I/O error.
    """
    if not sink_path:
        return True

    try:
        # Ensure parent directory exists if path contains directories
        dir_name = os.path.dirname(sink_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        with open(sink_path, "a", encoding="utf-8") as sink_file:
            sink_file.write(json.dumps(processed_event, ensure_ascii=False) + "\n")
        return True
    except Exception as exc:
        logger.warning(f"Failed to append event to sink file '{sink_path}': {exc}")
        return False
