import pytest
import json
import sys
import os
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'storage'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'publisher'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'subscriber'))

from database import init_db, save_message, mark_delivered, mark_failed, get_undelivered_messages
from mqtt_queue import push_message, pop_message, get_queue_length, clear_queue

# ─────────────────────────────────────────
# DATABASE TESTS
# ─────────────────────────────────────────

def test_init_db():
    """Database should initialize without errors"""
    init_db()

def test_save_message():
    """Message should be saved and return an ID"""
    msg_id = save_message(
        device_id="test_device",
        topic="sensors/test_device/temperature",
        payload=json.dumps({"temperature": 25.0, "humidity": 60.0}),
        qos=1
    )
    assert msg_id is not None
    assert isinstance(msg_id, int)
    assert msg_id > 0
    print(f"✅ Message saved with id={msg_id}")

def test_mark_delivered():
    """Message should be marked as delivered"""
    msg_id = save_message(
        device_id="test_device",
        topic="sensors/test/temp",
        payload=json.dumps({"temperature": 22.0}),
        qos=1
    )
    mark_delivered(msg_id)
    print(f"✅ Message {msg_id} marked as delivered")

def test_mark_failed():
    """Message retry count should increment"""
    msg_id = save_message(
        device_id="test_device_fail",
        topic="sensors/test/temp",
        payload=json.dumps({"temperature": 19.0}),
        qos=1
    )
    mark_failed(msg_id)
    mark_failed(msg_id)
    print(f"✅ Message {msg_id} marked as failed 2 times")

def test_get_undelivered_messages():
    """Should return list of undelivered messages"""
    # Save a message but don't deliver it
    save_message(
        device_id="undelivered_device",
        topic="sensors/undelivered/temp",
        payload=json.dumps({"temperature": 30.0}),
        qos=1
    )
    undelivered = get_undelivered_messages()
    assert isinstance(undelivered, list)
    assert len(undelivered) > 0
    print(f"✅ Found {len(undelivered)} undelivered messages")

# ─────────────────────────────────────────
# REDIS QUEUE TESTS
# ─────────────────────────────────────────

def test_push_message():
    """Message should be pushed to Redis queue"""
    clear_queue()
    push_message("sensors/test/temp", {"temperature": 25.0})
    assert get_queue_length() == 1
    print("✅ Message pushed to queue")

def test_pop_message():
    """Message should be popped from Redis queue"""
    clear_queue()
    push_message("sensors/test/temp", {"temperature": 25.0})
    msg = pop_message()
    assert msg is not None
    assert msg["topic"] == "sensors/test/temp"
    assert msg["payload"]["temperature"] == 25.0
    print(f"✅ Message popped: {msg}")

def test_queue_order():
    """Messages should come out in FIFO order"""
    clear_queue()
    push_message("sensors/test/temp", {"order": 1})
    push_message("sensors/test/temp", {"order": 2})
    push_message("sensors/test/temp", {"order": 3})

    msg1 = pop_message()
    msg2 = pop_message()
    msg3 = pop_message()

    assert msg1["payload"]["order"] == 1
    assert msg2["payload"]["order"] == 2
    assert msg3["payload"]["order"] == 3
    print("✅ Queue FIFO order correct")

def test_queue_length():
    """Queue length should be accurate"""
    clear_queue()
    assert get_queue_length() == 0
    push_message("sensors/test/temp", {"temperature": 20.0})
    push_message("sensors/test/temp", {"temperature": 21.0})
    assert get_queue_length() == 2
    print("✅ Queue length accurate")

def test_pop_empty_queue():
    """Popping empty queue should return None"""
    clear_queue()
    msg = pop_message()
    assert msg is None
    print("✅ Empty queue returns None")