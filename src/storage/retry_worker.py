import time
import sys
import os
import json

sys.path.append(os.path.dirname(__file__))

from database import get_undelivered_messages, mark_delivered, mark_failed

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
RETRY_INTERVAL = 10  # seconds between retry checks
MAX_RETRIES = 5

connected = False

def on_connect(client, userdata, flags, reason_code, properties):
    global connected
    connected = True
    print("✅ Retry worker connected to broker")

def on_disconnect(client, userdata, flags, reason_code, properties):
    global connected
    connected = False
    print("⚠️  Retry worker disconnected from broker")

def create_client():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="retry_worker"
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    return client

def retry_undelivered(client):
    messages = get_undelivered_messages()

    if not messages:
        print("✅ No undelivered messages")
        return

    print(f"🔄 Found {len(messages)} undelivered messages — retrying...")

    for msg in messages:
        msg_id, device_id, topic, payload, retry_count = msg

        if retry_count >= MAX_RETRIES:
            print(f"❌ Message {msg_id} exceeded max retries — skipping")
            continue

        if not connected:
            print(f"⚠️  Broker offline — will retry later")
            break

        try:
            result = client.publish(topic, payload, qos=1)

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                mark_delivered(msg_id)
                print(f"✅ Retried and delivered message {msg_id} (attempt {retry_count + 1})")
            else:
                mark_failed(msg_id)
                print(f"❌ Failed to retry message {msg_id}")

        except Exception as e:
            mark_failed(msg_id)
            print(f"❌ Error retrying message {msg_id}: {e}")

def start_retry_worker():
    client = create_client()

    print("🚀 Starting Retry Worker...")
    print(f"   Checking every {RETRY_INTERVAL} seconds\n")

    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()

        while True:
            print(f"\n⏰ [{time.strftime('%H:%M:%S')}] Checking undelivered messages...")
            retry_undelivered(client)
            time.sleep(RETRY_INTERVAL)

    except KeyboardInterrupt:
        print("\n⛔ Retry worker stopped")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    start_retry_worker()