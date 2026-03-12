"""Redis → MQTT bridge. Drains buffer to broker with at-least-once delivery."""
import json
import os
import sys
import time

sys.path.append(os.path.dirname(__file__))

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

from mqtt_queue import (
    pop_for_processing,
    confirm_processed,
    requeue_to_main,
    requeue_processing_back_to_main,
)

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
CONNECT_RETRY_SEC = 5
REQUEUE_RETRIES = 3
PUBLISH_ACK_TIMEOUT = 10

connected = False
stats = {"confirmed": 0, "requeued": 0, "invalid": 0, "requeue_failed": 0}


def on_connect(client, userdata, flags, reason_code, properties):
    global connected
    connected = True


def on_disconnect(client, userdata, flags, reason_code, properties):
    global connected
    connected = False


def create_client():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="redis_bridge",
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    return client


def connect_with_retry(client, host, port, keepalive=60):
    while True:
        try:
            client.connect(host, port, keepalive=keepalive)
            return
        except Exception as e:
            print(f"Bridge: connect failed ({host}:{port}): {e}, retry in {CONNECT_RETRY_SEC}s")
            time.sleep(CONNECT_RETRY_SEC)


def requeue_safe(raw_message: str) -> bool:
    for attempt in range(REQUEUE_RETRIES):
        try:
            requeue_to_main(raw_message)
            return True
        except Exception as e:
            if attempt == REQUEUE_RETRIES - 1:
                print(f"Bridge: requeue failed after {REQUEUE_RETRIES} attempts: {e}")
                return False
            time.sleep(1)
    return False


def run_bridge():
    client = create_client()
    requeue_processing_back_to_main()

    try:
        connect_with_retry(client, MQTT_HOST, MQTT_PORT)
        client.loop_start()

        while True:
            msg, raw = pop_for_processing()
            if msg is None:
                continue

            topic = msg.get("topic")
            payload = msg.get("payload")
            if not topic or payload is None:
                confirm_processed(raw)
                stats["invalid"] += 1
                continue

            payload_str = json.dumps(payload) if isinstance(payload, dict) else str(payload)

            if not connected:
                if requeue_safe(raw):
                    stats["requeued"] += 1
                else:
                    stats["requeue_failed"] += 1
                continue

            try:
                result = client.publish(topic, payload_str, qos=1)
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    if requeue_safe(raw):
                        stats["requeued"] += 1
                    else:
                        stats["requeue_failed"] += 1
                    continue

                result.wait_for_publish(timeout=PUBLISH_ACK_TIMEOUT)
                if result.is_published():
                    confirm_processed(raw)
                    stats["confirmed"] += 1
                else:
                    if requeue_safe(raw):
                        stats["requeued"] += 1
                    else:
                        stats["requeue_failed"] += 1
            except Exception as e:
                if requeue_safe(raw):
                    stats["requeued"] += 1
                else:
                    stats["requeue_failed"] += 1

    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()
        print(f"Bridge stopped | confirmed={stats['confirmed']} requeued={stats['requeued']} invalid={stats['invalid']} requeue_failed={stats['requeue_failed']}")


if __name__ == "__main__":
    run_bridge()
