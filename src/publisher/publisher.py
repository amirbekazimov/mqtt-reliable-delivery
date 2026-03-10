import paho.mqtt.client as mqtt
import json
import random
import time
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))

from dotenv import load_dotenv
from mqtt_queue import push_message

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Device {client._client_id.decode()} connected to broker")
    else:
        print(f"❌ Connection failed with code {rc}")

def on_publish(client, userdata, mid):
    print(f"📤 Message {mid} published successfully")

def create_device(device_id: str):
    """Create a simulated IoT device"""
    client = mqtt.Client(client_id=device_id)
    client.on_connect = on_connect
    client.on_publish = on_publish
    return client

def simulate_temperature(device_id: str, count: int = 10, interval: float = 1.0):
    """Simulate a temperature sensor sending data"""
    client = create_device(device_id)
    
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()
        
        print(f"\n🌡️  Starting temperature simulation for {device_id}")
        print(f"   Sending {count} messages every {interval}s\n")
        
        for i in range(count):
            payload = {
                "device_id": device_id,
                "temperature": round(random.uniform(18.0, 35.0), 2),
                "humidity": round(random.uniform(30.0, 80.0), 2),
                "timestamp": time.time(),
                "message_num": i + 1
            }
            
            topic = f"sensors/{device_id}/temperature"
            
            # Publish to MQTT
            result = client.publish(topic, json.dumps(payload), qos=1)
            
            # Also save to Redis queue
            push_message(topic, payload)
            
            print(f"📡 [{i+1}/{count}] {device_id} → temp: {payload['temperature']}°C, humidity: {payload['humidity']}%")
            
            time.sleep(interval)
        
        print(f"\n✅ {device_id} finished sending {count} messages")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    # Simulate 3 devices
    devices = ["device_001", "device_002", "device_003"]
    
    for device_id in devices:
        simulate_temperature(device_id, count=5, interval=0.5)