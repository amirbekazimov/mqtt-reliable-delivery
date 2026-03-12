"""Redis queue for MQTT messages. Uses a processing list for at-least-once delivery."""
import json
import os

import redis
from dotenv import load_dotenv

load_dotenv()

QUEUE_KEY = os.getenv("MQTT_QUEUE_KEY", "mqtt_queue")
PROCESSING_KEY = os.getenv("MQTT_QUEUE_PROCESSING_KEY", "mqtt_queue_processing")

_redis_client = None


def get_redis():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True,
        )
    return _redis_client


def push_message(topic: str, payload: dict):
    r = get_redis()
    message = json.dumps({"topic": topic, "payload": payload})
    r.lpush(QUEUE_KEY, message)


def pop_message():
    r = get_redis()
    result = r.brpop(QUEUE_KEY, timeout=5)
    if result:
        _, message = result
        return json.loads(message)
    return None


def pop_for_processing():
    """Atomically move one message to processing list. Call confirm_processed or requeue_to_main."""
    r = get_redis()
    raw = r.brpoplpush(QUEUE_KEY, PROCESSING_KEY, timeout=5)
    if not raw:
        return None, None
    try:
        return json.loads(raw), raw
    except Exception:
        return {"topic": "", "payload": {"raw": raw}}, raw


def confirm_processed(raw_message: str):
    r = get_redis()
    r.lrem(PROCESSING_KEY, 1, raw_message)


def requeue_to_main(raw_message: str):
    r = get_redis()
    r.lpush(QUEUE_KEY, raw_message)
    r.lrem(PROCESSING_KEY, 1, raw_message)


def get_queue_length():
    return get_redis().llen(QUEUE_KEY)


def clear_queue():
    get_redis().delete(QUEUE_KEY, PROCESSING_KEY)


def requeue_processing_back_to_main():
    """On bridge startup: move any leftover processing messages back to main queue."""
    r = get_redis()
    n = 0
    while True:
        raw = r.lpop(PROCESSING_KEY)
        if not raw:
            break
        r.lpush(QUEUE_KEY, raw)
        n += 1
    return n
