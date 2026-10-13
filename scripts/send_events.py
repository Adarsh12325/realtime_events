#!/usr/bin/env python3
"""
Interactive Event Generator & Ingestion CLI Tool.
Sends sample, ad-hoc, or streaming telemetry payloads to the Event Producer API.
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
import urllib.request
import urllib.error


def send_payload(url: str, payload: dict) -> dict:
    """Sends a single JSON payload to the ingest endpoint."""
    data_bytes = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data_bytes,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            status_code = response.getcode()
            body = json.loads(response.read().decode("utf-8"))
            return {"status_code": status_code, "body": body, "success": True}
    except urllib.error.HTTPError as http_err:
        try:
            body = json.loads(http_err.read().decode("utf-8"))
        except Exception:
            body = {"error": http_err.reason}
        return {"status_code": http_err.code, "body": body, "success": False}
    except Exception as exc:
        return {"status_code": 0, "body": {"error": str(exc)}, "success": False}


def run_samples_mode(url: str, sample_file: str):
    """Iterates through predefined sample events (both valid and invalid cases)."""
    if not os.path.exists(sample_file):
        print(f"[ERROR] Sample file not found at: {sample_file}")
        sys.exit(1)

    with open(sample_file, "r", encoding="utf-8") as f:
        samples = json.load(f)

    print(f"\n=================================================================")
    print(f" Sending Predefined Test Cases to: {url}")
    print(f" Total test cases: {len(samples)}")
    print(f"=================================================================\n")

    passed_expectations = 0
    for idx, item in enumerate(samples, 1):
        desc = item.get("description", "Unnamed Test")
        expected_valid = item.get("is_valid", True)
        payload = item.get("payload", {})

        print(f"[{idx}/{len(samples)}] Testing: {desc}")
        print(f"   Payload: {json.dumps(payload)}")
        result = send_payload(url, payload)

        status_code = result["status_code"]
        is_200 = (status_code == 200)
        matches_expectation = (is_200 == expected_valid)

        if matches_expectation:
            passed_expectations += 1
            status_tag = "PASS (Expected Result)"
        else:
            status_tag = "FAIL (Unexpected Response)"

        print(f"   Response [{status_code}]: {result['body']} -> {status_tag}\n")
        time.sleep(0.3)

    print(f"=================================================================")
    print(f" Test Run Completed: {passed_expectations}/{len(samples)} cases behaved as expected.")
    print(f"=================================================================\n")


def run_continuous_mode(url: str, count: int, delay: float):
    """Generates and streams randomized real-time sensor events."""
    print(f"\n=================================================================")
    print(f" Streaming {count} Synthetic Events to: {url}")
    print(f" Delay interval: {delay}s")
    print(f"=================================================================\n")

    for i in range(1, count + 1):
        sensor_id = random.choice(["sensor-north-1", "sensor-south-2", "sensor-east-3", "sensor-west-4"])
        telemetry = round(random.uniform(15.0, 85.0), 2)
        payload = {
            "event_id": f"{sensor_id}-{int(time.time() * 1000)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sensor_data": telemetry
        }

        result = send_payload(url, payload)
        if result["success"]:
            print(f"[{i}/{count}] OK (200) | Event: {payload['event_id']} | Value: {telemetry}")
        else:
            print(f"[{i}/{count}] ERR ({result['status_code']}) | Event: {payload['event_id']} | Details: {result['body']}")

        if i < count:
            time.sleep(delay)

    print("\nContinuous stream completed.\n")


def main():
    parser = argparse.ArgumentParser(description="Real-Time Event Ingestion Test Client")
    parser.add_argument("--url", default="http://localhost:5000/ingest", help="Ingestion API endpoint URL")
    parser.add_argument("--mode", choices=["samples", "stream"], default="samples", help="Mode: 'samples' or 'stream'")
    parser.add_argument("--count", type=int, default=10, help="Number of events in stream mode")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between streaming events in seconds")
    parser.add_argument("--samples-file", default=os.path.join(os.path.dirname(__file__), "sample_events.json"), help="Path to sample events JSON")

    args = parser.parse_args()

    if args.mode == "samples":
        run_samples_mode(args.url, args.samples_file)
    else:
        run_continuous_mode(args.url, args.count, args.delay)


if __name__ == "__main__":
    main()
