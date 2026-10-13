"""
Event schema definition and validation module.
Defines the canonical JSON schema for real-time sensor events and validation utilities.
"""

from typing import Tuple, Optional, Any, Dict
import jsonschema
from jsonschema import validate, ValidationError

# Canonical JSON Schema for ingested telemetry events
EVENT_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "SensorEvent",
    "description": "Schema representing a single real-time telemetry or sensor event",
    "type": "object",
    "properties": {
        "event_id": {
            "type": "string",
            "minLength": 1,
            "description": "Unique identifier for the event (e.g., UUID or structured ID)"
        },
        "timestamp": {
            "type": "string",
            "format": "date-time",
            "description": "ISO 8601 formatted UTC timestamp string (e.g., 2026-10-13T10:30:00Z)"
        },
        "sensor_data": {
            "type": "number",
            "description": "Numeric sensor telemetry measurement"
        }
    },
    "required": ["event_id", "timestamp", "sensor_data"],
    "additionalProperties": False
}

# Pre-compiled format checker for RFC 3339 / ISO 8601 date-time formats
_FORMAT_CHECKER = jsonschema.FormatChecker(["date-time"])


def validate_event(event_data: Any) -> Tuple[bool, Optional[str]]:
    """
    Validates an incoming event dictionary against the canonical EVENT_SCHEMA.

    Args:
        event_data: The incoming payload object (expected dict).

    Returns:
        A tuple of (is_valid: bool, error_message: Optional[str]).
    """
    if not isinstance(event_data, dict):
        return False, "Payload must be a JSON object"

    try:
        validate(instance=event_data, schema=EVENT_SCHEMA, format_checker=_FORMAT_CHECKER)
        return True, None
    except ValidationError as err:
        return False, f"Validation error at '{'.'.join(str(p) for p in err.path) or 'root'}': {err.message}"
    except Exception as err:
        return False, f"Unexpected validation failure: {str(err)}"
