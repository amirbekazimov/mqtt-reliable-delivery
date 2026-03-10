import redis
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_redis():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True
    )

def push_message(topic: str, payload: dict):
    """Push message to Redis queue"""
    r = get_redis()
    message = json.dumps({
        "topic": topic,
        "payload": payload
    })
    r.lpush("mqtt_queue", message)
    print(f"📥 Queued: {topic} → {payload}")

def pop_message():
    """Pop message from Redis queue (blocking)"""
    r = get_redis()
    result = r.brpop("mqtt_queue", timeout=5)
    if result:
        _, message = result
        return json.loads(message)
    return None

def get_queue_length():
    """How many messages are waiting in queue"""
    r = get_redis()
    return r.llen("mqtt_queue")

def clear_queue():
    """Clear all messages from queue"""
    r = get_redis()
    r.delete("mqtt_queue")
    print("🗑️ Queue cleared")