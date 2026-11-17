# -*- coding: utf-8 -*-
import json
import os
import socket
import threading
import time
from typing import Dict, List

import httpx
import pika
from fastapi import FastAPI
from pydantic import BaseModel

SERVICE_NAME = os.getenv("SERVICE_NAME", "tag-service")
INSTANCE_ID = os.getenv("INSTANCE_ID") or socket.gethostname()
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE = os.getenv("EVENTS_EXCHANGE", "notebook.events")
ROUTING_KEY = os.getenv("NOTE_EVENTS_KEY", "note.changed")
QUEUE_NAME = os.getenv("TAG_QUEUE", "tag-service.note-events")
DISCOVERY_URL = os.getenv("DISCOVERY_URL", "")
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "http://tag:8000")

app = FastAPI(title=SERVICE_NAME)
index: Dict[str, Dict] = {}
consumed_events: List[Dict] = []


def discovery_loop():
    if not DISCOVERY_URL:
        return
    payload = {
        "service_name": SERVICE_NAME,
        "instance_id": INSTANCE_ID,
        "base_url": SERVICE_BASE_URL,
        "meta": {"role": "tag-consumer"},
    }
    while True:
        try:
            httpx.post(f"{DISCOVERY_URL}/heartbeat", json=payload, timeout=3.0)
        except Exception:
            pass
        time.sleep(10)


class EnrichRequest(BaseModel):
    text: str


def ensure_topology(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE, routing_key=ROUTING_KEY)


def handle_message(body: bytes) -> None:
    event = json.loads(body.decode("utf-8"))
    payload = event.get("payload", {})
    note_id = payload.get("id")
    record = {
        "note_id": note_id,
        "title": payload.get("title"),
        "tags": payload.get("tags", []),
        "event_type": event.get("event_type"),
        "consumed_at": time.time(),
        "consumer_instance": INSTANCE_ID,
        "producer_instance": event.get("producer_instance"),
    }
    if note_id:
        index[note_id] = record
    consumed_events.append(record)
    if len(consumed_events) > 200:
        del consumed_events[:-200]
    print(f"[consumer] {event.get('event_type')} note={note_id} by {INSTANCE_ID}", flush=True)


def consumer_loop():
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            ensure_topology(channel)
            channel.basic_qos(prefetch_count=1)

            def _callback(ch, method, properties, body):  # noqa: ARG001
                handle_message(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=_callback)
            print(f"[consumer] listening on {QUEUE_NAME}", flush=True)
            channel.start_consuming()
        except Exception as exc:  # noqa: BLE001
            print(f"[consumer] reconnect in 3s due to {exc}", flush=True)
            time.sleep(3)


@app.on_event("startup")
def start_consumer():
    threading.Thread(target=consumer_loop, name="rabbit-consumer", daemon=True).start()
    threading.Thread(target=discovery_loop, name="discovery", daemon=True).start()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
        "indexed_notes": len(index),
        "consumed_total": len(consumed_events),
    }


@app.post("/enrich")
def enrich(body: EnrichRequest):
    marker = f"[{SERVICE_NAME}:{INSTANCE_ID}]"
    return {
        "text": f"{body.text} {marker}",
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
        "protocol": "HTTP/JSON synchronous",
    }


@app.get("/index")
def get_index():
    return {
        "items": list(index.values()),
        "count": len(index),
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }


@app.get("/events")
def get_events():
    return {
        "items": consumed_events[-50:],
        "count": len(consumed_events),
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }
