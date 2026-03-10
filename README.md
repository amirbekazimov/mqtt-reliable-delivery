# MQTT Reliable Delivery

> Guaranteed message delivery system for IoT devices using MQTT, Redis, and PostgreSQL.

## 🚨 Problem

In standard MQTT, messages can be lost when:

- Network connection drops between broker and subscriber
- Subscriber is temporarily offline
- Broker restarts unexpectedly

This project solves the **message loss problem** with a persistence layer and delivery tracking.

## ✅ Solution

- **Redis queue** buffers all incoming messages instantly
- **PostgreSQL** tracks every message with delivery status
- **Retry mechanism** handles failed deliveries (up to 5 attempts)
- **Device stats** track each device's message history

## 🏗️ Architecture

```
IoT Devices (simulated)
      │
      ▼
[MQTT Publisher] ──► [Mosquitto Broker] ──► [MQTT Subscriber]
                                                    │
                              ┌─────────────────────┤
                              ▼                     ▼
                         [Redis Queue]        [PostgreSQL]
                         (buffer)             (persistence)
```

## 🚀 Quick Start

### Requirements

- Docker & Docker Compose
- Python 3.9+

### Run

```bash
# 1. Clone
git clone https://github.com/yourusername/mqtt-reliable-delivery.git
cd mqtt-reliable-delivery

# 2. Setup environment
cp .env.example .env
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Start infrastructure
docker-compose up -d

# 4. Initialize database
cd src/storage && python3 init_db.py && cd ../..

# 5. Start subscriber (Terminal 1)
cd src/subscriber && python3 subscriber.py

# 6. Start publisher (Terminal 2)
cd src/publisher && python3 publisher.py
```

## 📊 Output Example

```
📨 Received [1] from device_001
   Temp: 22.34°C | Humidity: 63.87%
   ✅ Saved to DB (id=1)

📨 Received [2] from device_002
   Temp: 18.84°C | Humidity: 36.2%
   ✅ Saved to DB (id=2)
```

## 🧪 Tests

```bash
pytest tests/test_delivery.py -v
# 10 passed in 1.23s
```

## 🛠️ Tech Stack

| Technology       | Purpose              |
| ---------------- | -------------------- |
| Python           | Core language        |
| MQTT / Mosquitto | Message broker       |
| paho-mqtt        | Python MQTT client   |
| Redis            | Message queue buffer |
| PostgreSQL       | Message persistence  |
| Docker Compose   | Infrastructure       |
| pytest           | Testing              |

## 📁 Project Structure

```
mqtt-reliable-delivery/
├── docker-compose.yml
├── .env.example
├── src/
│   ├── broker/
│   │   └── mosquitto.conf
│   ├── publisher/
│   │   └── publisher.py      # IoT device simulator
│   ├── subscriber/
│   │   └── subscriber.py     # Message consumer
│   └── storage/
│       ├── database.py       # PostgreSQL layer
│       └── mqtt_queue.py     # Redis queue layer
└── tests/
    └── test_delivery.py      # 10 tests
```

## 💡 Use Cases

- Smart home sensor networks
- Industrial IoT monitoring
- Fleet tracking systems
- Any IoT system where message loss is unacceptable
