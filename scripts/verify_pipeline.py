#!/usr/bin/env python3
"""
Pipeline Verification and Sanity Check Utility.
Performs end-to-end operational verification of the running Docker Compose pipeline.
"""

import json
import subprocess
import sys
import time
import urllib.request
import urllib.error

PRODUCER_HEALTH_URL = "http://localhost:5000/health"
PRODUCER_INGEST_URL = "http://localhost:5000/ingest"


def run_command(cmd: str) -> str:
    """Executes a shell command and returns stdout."""
    try:
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return res.stdout.strip()
    except subprocess.CalledProcessError as e:
        return f"ERROR: {e.stderr.strip()}"


def check_health() -> bool:
    """Checks the Producer service health endpoint."""
    print("1. Probing Producer API Health Endpoint (/health)...")
    try:
        with urllib.request.urlopen(PRODUCER_HEALTH_URL, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"   Response Status: {resp.getcode()}")
            print(f"   Payload: {data}")
            return resp.getcode() == 200 and data.get("kafka_connected", False)
    except Exception as exc:
        print(f"   Health check failed: {exc}")
        return False


def ingest_verification_event(event_id: str) -> bool:
    """Sends a verification event through the ingestion endpoint."""
    print(f"\n2. Ingesting Verification Event '{event_id}'...")
    payload = {
        "event_id": event_id,
        "timestamp": "2026-10-13T12:00:00Z",
        "sensor_data": 77.7
    }
    req = urllib.request.Request(
        PRODUCER_INGEST_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"   Response Status: {resp.getcode()}")
            print(f"   Payload: {data}")
            return resp.getcode() == 200
    except Exception as exc:
        print(f"   Ingestion failed: {exc}")
        return False


def verify_consumer_logs(event_id: str) -> bool:
    """Inspects event_consumer container logs to confirm message receipt."""
    print(f"\n3. Checking Consumer Container Logs for '{event_id}'...")
    time.sleep(2)  # Allow Kafka and Consumer to poll and commit
    logs = run_command("docker-compose logs --tail=30 event_consumer")
    if event_id in logs:
        print(f"   [SUCCESS] Found event '{event_id}' in consumer logs!")
        return True
    else:
        print(f"   [NOTICE] Event ID not found in recent logs. Raw tail output:\n{logs}")
        return False


def main():
    print("=================================================================")
    print(" Real-Time Event Pipeline - Automated Verification Suite")
    print("=================================================================\n")

    health_ok = check_health()
    if not health_ok:
        print("\n[WARNING] Pipeline does not appear to be fully healthy or reachable on localhost:5000.")
        print("Please ensure 'docker-compose up -d' has completed and brokers are initialized.")
        sys.exit(1)

    test_id = f"verification-check-{int(time.time())}"
    ingest_ok = ingest_verification_event(test_id)
    if not ingest_ok:
        print("\n[ERROR] Ingest verification failed.")
        sys.exit(1)

    consumer_ok = verify_consumer_logs(test_id)

    print("\n=================================================================")
    if health_ok and ingest_ok and consumer_ok:
        print(" ALL VERIFICATION CHECKS PASSED: Pipeline is fully functional!")
    else:
        print(" VERIFICATION COMPLETED WITH WARNINGS: Review logs above.")
    print("=================================================================\n")


if __name__ == "__main__":
    main()
