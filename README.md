# MQTT Reliable Delivery

> Guaranteed message delivery for IoT: buffer → broker → persistence. Survives broker and subscriber restarts.

## Problem

In standard MQTT, messages can be lost when:

- The broker is down or restarts
- The subscriber is temporarily offline
- The network drops between device and broker

This project solves the **message loss problem** with a single path: **Redis buffer → MQTT bridge → broker → subscriber → PostgreSQL**.

## Solution

- **Devices / publisher** push only to a **Redis buffer** (no direct MQTT, no DB). Works when the device has no internet or the broker is down.
- **Redis → MQTT bridge** is the single path to the broker: it drains the buffer and publishes to MQTT. When the broker restarts, the bridge reconnects and continues; nothing is lost.
- **Subscriber** receives from MQTT and persists to **PostgreSQL** with delivery status.
- **Retry worker** re-sends any undelivered messages from the DB (e.g. downstream failures).

## Architecture

```
Devices / Gateway (or simulator)
      │
      ▼
   [Redis]  ← buffer (single write path from devices)
      │
      ▼
[Redis Bridge]  →  [Mosquitto]  →  [Subscriber]  →  [PostgreSQL]
 (drains buffer)     (broker)      (save + ack)      (persistence)
                           │
                           └── [Retry Worker]  (re-send undelivered from DB)
```

- **Publisher** (in repo): device simulator — pushes to Redis only. In production, replace with real devices or an HTTP ingest that writes to Redis.
- **Redis bridge**: runs continuously; pops from Redis and publishes to MQTT. Survives broker restarts.
- **Subscriber**: receives from MQTT, saves to DB, marks delivered.
- **Retry worker**: periodically sends undelivered messages from DB to MQTT.

## Quick Start

### Requirements

- Docker & Docker Compose
- Python 3.9+ (for local run and tests)

### Run with Docker

```bash
# 1. Clone and setup (replace YOUR_USERNAME with your GitHub username)
git clone https://github.com/YOUR_USERNAME/mqtt-reliable-delivery.git
cd mqtt-reliable-delivery
cp .env.example .env

# 2. Start everything (broker, Redis, Postgres, bridge, subscriber, retry worker)
docker compose up -d

# 3. Initialize database (once)
docker compose exec subscriber python3 src/storage/database.py

# 4. Run device simulator (pushes to Redis; bridge will send to MQTT)
docker compose up publisher

# 5. Watch subscriber logs
docker logs -f mqtt_subscriber
```

### Test “survives restart”

1. Start stack: `docker compose up -d` (run `python3 src/storage/database.py` once to init DB).
2. Run publisher: `docker compose up publisher` (messages go to Redis).
3. Stop broker: `docker stop mqtt_broker`.
4. Run publisher again (messages keep going to Redis).
5. Start broker: `docker start mqtt_broker`.
6. Bridge reconnects and drains Redis → MQTT; subscriber receives and saves to DB.

## Output

On stop, bridge and subscriber print delivery stats (e.g. `confirmed=15 requeued=0`, `delivered=14 duplicate=1 failed=0`).

## Tests

```bash
# Start infra (broker, Redis, Postgres) then:
cd src/storage && python3 database.py && cd ../..
pytest tests/test_delivery.py -v
```

## Tech Stack

| Technology       | Purpose                    |
| ---------------- | -------------------------- |
| Python           | Core language              |
| MQTT / Mosquitto | Message broker             |
| paho-mqtt        | Python MQTT client         |
| Redis            | Message buffer (FIFO)      |
| PostgreSQL       | Message persistence        |
| Docker Compose   | Infrastructure             |
| pytest           | Testing                    |

## Project Structure

```
mqtt-reliable-delivery/
├── docker-compose.yml
├── .env.example
├── src/
│   ├── broker/
│   │   └── mosquitto.conf
│   ├── publisher/
│   │   └── publisher.py       # Device simulator (Redis only)
│   ├── subscriber/
│   │   └── subscriber.py      # MQTT → DB
│   └── storage/
│       ├── database.py        # PostgreSQL
│       ├── database.py        # PostgreSQL (+ run as script to init)
│       ├── mqtt_queue.py      # Redis queue
│       ├── redis_bridge.py    # Redis → MQTT
│       └── retry_worker.py    # DB undelivered → MQTT
└── tests/
    └── test_delivery.py
```

## Use Cases

- Smart home / sensor networks
- Industrial IoT monitoring
- Fleet tracking
- Any IoT system where message loss is unacceptable and devices may be offline or behind a gateway

## License

[MIT](LICENSE)
