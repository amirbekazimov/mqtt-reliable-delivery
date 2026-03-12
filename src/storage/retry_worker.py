"""Retries undelivered messages from DB to MQTT."""
import os
import sys
import time

sys.path.append(os.path.dirname(__file__))

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

from database import get_undelivered_messages, mark_delivered, mark_failed

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
INTERVAL = 10
MAX_RETRIES = 5

connected = False


def on_connect(client, userdata, flags, reason_code, properties):
    global connected
    connected = True


def on_disconnect(client, userdata, flags, reason_code, properties):
    global connected
    connected = False


def run_retries(client):
    messages = get_undelivered_messages()
    if not messages:
        return

    for msg in messages:
        msg_id, device_id, topic, payload, retry_count = msg
        if retry_count >= MAX_RETRIES:
            continue
        if not connected:
            break
        try:
            result = client.publish(topic, payload, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                mark_delivered(msg_id)
            else:
                mark_failed(msg_id)
        except Exception:
            mark_failed(msg_id)


def main():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="retry_worker",
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    try:
        while True:
            run_retries(client)
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
