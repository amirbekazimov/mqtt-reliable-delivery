"""PostgreSQL persistence for MQTT messages and device state."""
import json
import os
import uuid

import psycopg2
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "mqtt_delivery"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "root123"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            message_uuid VARCHAR(36) UNIQUE NOT NULL,
            device_id VARCHAR(100) NOT NULL,
            topic VARCHAR(255) NOT NULL,
            payload TEXT NOT NULL,
            qos INTEGER DEFAULT 1,
            delivered BOOLEAN DEFAULT FALSE,
            retry_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            delivered_at TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            id SERIAL PRIMARY KEY,
            message_uuid VARCHAR(36) NOT NULL,
            device_id VARCHAR(100) UNIQUE NOT NULL,
            last_seen TIMESTAMP,
            total_messages INTEGER DEFAULT 0,
            failed_messages INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'messages' AND column_name = 'message_uuid'
            ) THEN
                ALTER TABLE messages ADD COLUMN message_uuid VARCHAR(36);
                UPDATE messages SET message_uuid = gen_random_uuid()::text WHERE message_uuid IS NULL;
                ALTER TABLE messages ALTER COLUMN message_uuid SET NOT NULL;
                ALTER TABLE messages ADD CONSTRAINT messages_message_uuid_key UNIQUE (message_uuid);
            END IF;
        END $$
    """)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'devices' AND column_name = 'message_uuid'
            ) THEN
                ALTER TABLE devices ADD COLUMN message_uuid VARCHAR(36);
                UPDATE devices SET message_uuid = gen_random_uuid()::text WHERE message_uuid IS NULL;
                ALTER TABLE devices ALTER COLUMN message_uuid SET NOT NULL;
            END IF;
        END $$
    """)

    conn.commit()
    cur.close()
    conn.close()


def save_message(device_id: str, topic: str, payload: str, qos: int = 1, message_uuid: str = None):
    if message_uuid is None:
        message_uuid = str(uuid.uuid4())

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO messages (message_uuid, device_id, topic, payload, qos)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (message_uuid) DO NOTHING
        RETURNING id
    """, (message_uuid, device_id, topic, payload, qos))
    row = cur.fetchone()
    message_id = row[0] if row else None

    cur.execute("""
        INSERT INTO devices (device_id, message_uuid, last_seen, total_messages)
        VALUES (%s, %s, NOW(), 1)
        ON CONFLICT (device_id)
        DO UPDATE SET last_seen = NOW(), total_messages = devices.total_messages + 1, message_uuid = EXCLUDED.message_uuid
    """, (device_id, message_uuid))
    conn.commit()
    cur.close()
    conn.close()
    return message_id


def mark_delivered(message_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE messages SET delivered = TRUE, delivered_at = NOW() WHERE id = %s", (message_id,))
    conn.commit()
    cur.close()
    conn.close()


def mark_failed(message_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE messages SET retry_count = retry_count + 1 WHERE id = %s", (message_id,))
    conn.commit()
    cur.close()
    conn.close()


def get_undelivered_messages():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, device_id, topic, payload, retry_count
        FROM messages
        WHERE delivered = FALSE AND retry_count < 5
        ORDER BY created_at ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


if __name__ == "__main__":
    init_db()
