import paho.mqtt.client as mqtt
import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))

from database import save_message, mark_delivered

stats = {"received": 0, "delivered": 0, "failed": 0}

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"✅ Subscriber connected, code: {reason_code}")
    client.subscribe("sensors/#", qos=1)
    print("📡 Listening on: sensors/#\n")

def on_message(client, userdata, msg):
    stats["received"] += 1
    try:
        payload = json.loads(msg.payload.decode())
        device_id = payload.get("device_id", "unknown")

        print(f"📨 Received [{stats['received']}] from {device_id}")
        print(f"   Temp: {payload.get('temperature')}°C | Humidity: {payload.get('humidity')}%")

        message_id = save_message(
            device_id=device_id,
            topic=msg.topic,
            payload=json.dumps(payload),
            qos=msg.qos
        )
        mark_delivered(message_id)
        stats["delivered"] += 1
        print(f"   ✅ Saved to DB (id={message_id})\n")

    except Exception as e:
        stats["failed"] += 1
        print(f"   ❌ Error: {e}\n")

def start_subscriber():
    # Use VERSION2 API explicitly
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="reliable_subscriber"
    )
    client.on_connect = on_connect
    client.on_message = on_message

    print("🚀 Starting MQTT Subscriber...")
    client.connect("localhost", 1883, keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print(f"\n⛔ Stopped | Received: {stats['received']} | Delivered: {stats['delivered']}")
        client.disconnect()

if __name__ == "__main__":
    start_subscriber()