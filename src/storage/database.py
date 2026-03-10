import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )

def init_db():
    """Create tables if not exists"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
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
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(100) UNIQUE NOT NULL,
            last_seen TIMESTAMP,
            total_messages INTEGER DEFAULT 0,
            failed_messages INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Database initialized successfully")

def save_message(device_id: str, topic: str, payload: str, qos: int = 1):
    """Save incoming message to PostgreSQL"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO messages (device_id, topic, payload, qos)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (device_id, topic, payload, qos))
    
    message_id = cursor.fetchone()[0]
    
    # Update device stats
    cursor.execute("""
        INSERT INTO devices (device_id, last_seen, total_messages)
        VALUES (%s, %s, 1)
        ON CONFLICT (device_id)
        DO UPDATE SET 
            last_seen = %s,
            total_messages = devices.total_messages + 1
    """, (device_id, datetime.now(), datetime.now()))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return message_id

def mark_delivered(message_id: int):
    """Mark message as successfully delivered"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE messages 
        SET delivered = TRUE, delivered_at = NOW()
        WHERE id = %s
    """, (message_id,))
    
    conn.commit()
    cursor.close()
    conn.close()

def mark_failed(message_id: int):
    """Increment retry count for failed message"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE messages 
        SET retry_count = retry_count + 1
        WHERE id = %s
    """, (message_id,))
    
    conn.commit()
    cursor.close()
    conn.close()

def get_undelivered_messages():
    """Get all messages that failed to deliver"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, device_id, topic, payload, retry_count
        FROM messages
        WHERE delivered = FALSE AND retry_count < 5
        ORDER BY created_at ASC
    """)
    
    messages = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return messages