"""MQTT subscriber. Persists messages to PostgreSQL with deduplication by message_uuid."""
import json
import os
import signal
import sys
import uuid

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))

import paho.mqtt.client as mqtt

from database import save_message, mark_delivered

stats = {"received": 0, "delivered": 0, "duplicate": 0, "failed": 0}
_client = None


def _request_stop(signum=None, frame=None):
    if _client:
        _client.disconnect()


def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe("sensors/#", qos=1)


def on_message(client, userdata, msg):
    stats["received"] += 1
    try:
        payload = json.loads(msg.payload.decode())
        device_id = payload.get("device_id", "unknown")
        message_uuid = payload.get("message_uuid", str(uuid.uuid4()))

        message_id = save_message(
            device_id=device_id,
            topic=msg.topic,
            payload=json.dumps(payload),
            qos=msg.qos,
            message_uuid=message_uuid,
        )

        if message_id:
            mark_delivered(message_id)
            stats["delivered"] += 1
        else:
            stats["duplicate"] += 1

    except Exception as e:
        stats["failed"] += 1
        print(f"Subscriber error: {e}")


def main():
    global _client
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="reliable_subscriber",
    )
    _client = client
    client.on_connect = on_connect
    client.on_message = on_message

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    mqtt_host = os.getenv("MQTT_HOST", "localhost")
    mqtt_port = int(os.getenv("MQTT_PORT", 1883))
    client.connect(mqtt_host, mqtt_port, keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()

    print(f"Subscriber stopped | received={stats['received']} delivered={stats['delivered']} duplicate={stats['duplicate']} failed={stats['failed']}")


if __name__ == "__main__":
    main()
