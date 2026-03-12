"""Device simulator: pushes messages to Redis only. Replace with real devices or HTTP ingest."""
import os
import random
import sys
import time
import uuid

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))

from dotenv import load_dotenv
from mqtt_queue import push_message

load_dotenv()


def run(device_id: str, count: int = 10, interval: float = 1.0):
    for i in range(count):
        payload = {
            "message_uuid": str(uuid.uuid4()),
            "device_id": device_id,
            "temperature": round(random.uniform(18.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 80.0), 2),
            "timestamp": time.time(),
            "message_num": i + 1,
        }
        push_message(f"sensors/{device_id}/temperature", payload)
        time.sleep(interval)


if __name__ == "__main__":
    for device_id in ["device_001", "device_002", "device_003"]:
        run(device_id, count=5, interval=0.5)
