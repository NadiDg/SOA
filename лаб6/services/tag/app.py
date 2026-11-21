
import json
import os
import socket
import sqlite3
import threading
import time
from contextlib import contextmanager
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
DLX = os.getenv("NOTE_DLX", "notebook.events.dlx")
DLQ_ROUTING_KEY = os.getenv("NOTE_DLQ_KEY", "note.changed.dlq")
DISCOVERY_URL = os.getenv("DISCOVERY_URL", "")
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "http://tag:8000")
DB_PATH = os.getenv("DB_PATH", "/data/tag.db")

FORCE_FAIL = os.getenv("FORCE_CONSUMER_FAIL", "true").lower() in {"1", "true", "yes"}

app = FastAPI(title=SERVICE_NAME)
_db_lock = threading.Lock()
consumed_events: List[Dict] = []
failed_events: List[Dict] = []

class EnrichRequest(BaseModel):
    text: str

@contextmanager
def db_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with _db_lock, db_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS inbox (
                message_id TEXT PRIMARY KEY,
                note_id TEXT,
                event_type TEXT,
                received_at REAL NOT NULL,
                processed_at REAL,
                status TEXT NOT NULL,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS tag_index (
                note_id TEXT PRIMARY KEY,
                title TEXT,
                tags TEXT,
                updated_at REAL NOT NULL
            );
            """
        )

def discovery_loop():
    if not DISCOVERY_URL:
        return
    payload = {
        "service_name": SERVICE_NAME,
        "instance_id": INSTANCE_ID,
        "base_url": SERVICE_BASE_URL,
        "meta": {"role": "tag-consumer", "force_fail": FORCE_FAIL},
    }
    while True:
        try:
            httpx.post(f"{DISCOVERY_URL}/heartbeat", json=payload, timeout=3.0)
        except Exception:
            pass
        time.sleep(10)

def ensure_topology(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=DLX, exchange_type="topic", durable=True)
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
        },
    )
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE, routing_key=ROUTING_KEY)

    dlq_name = os.getenv("NOTE_DLQ", "notebook.note-events.dlq")
    channel.queue_declare(queue=dlq_name, durable=True)
    channel.queue_bind(queue=dlq_name, exchange=DLX, routing_key=DLQ_ROUTING_KEY)

def should_fail(payload: Dict) -> bool:
    tags = [str(t).lower() for t in payload.get("tags", [])]
    title = str(payload.get("title", "")).lower()
    if "ok" in tags or title.startswith("ok:"):
        return False
    if FORCE_FAIL:
        return True
    return "fail" in tags or title.startswith("fail:")

def publish_success(channel, event: Dict):
    success = {
        "event_type": "NoteIndexed",
        "occurred_at": time.time(),
        "producer": SERVICE_NAME,
        "producer_instance": INSTANCE_ID,
        "payload": event.get("payload", {}),
    }
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key="note.saga.success",
        body=json.dumps(success).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )

def handle_message(channel, body: bytes, delivery_tag: int) -> None:
    event = json.loads(body.decode("utf-8"))
    payload = event.get("payload", {})
    message_id = event.get("message_id") or payload.get("id") or str(time.time())
    note_id = payload.get("id")
    now = time.time()

    with _db_lock, db_conn() as conn:
        existing = conn.execute("SELECT * FROM inbox WHERE message_id = ?", (message_id,)).fetchone()
        if existing and existing["status"] == "processed":
            channel.basic_ack(delivery_tag=delivery_tag)
            print(f"[inbox] duplicate ignored message_id={message_id}", flush=True)
            return
        if not existing:
            conn.execute(
                """
                INSERT INTO inbox(message_id, note_id, event_type, received_at, processed_at, status, error)
                VALUES (?, ?, ?, ?, NULL, 'received', NULL)
                """,
                (message_id, note_id, event.get("event_type"), now),
            )

    record = {
        "note_id": note_id,
        "title": payload.get("title"),
        "tags": payload.get("tags", []),
        "event_type": event.get("event_type"),
        "message_id": message_id,
        "consumer_instance": INSTANCE_ID,
    }

    if should_fail(payload):
        with _db_lock, db_conn() as conn:
            conn.execute(
                """
                UPDATE inbox SET status = 'failed', error = ?, processed_at = ?
                WHERE message_id = ?
                """,
                ("forced consumer failure for SAGA demo", time.time(), message_id),
            )
        failed_events.append({**record, "failed_at": time.time(), "error": "forced consumer failure"})

        channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
        print(f"[consumer] FAIL note={note_id} -> DLQ", flush=True)
        return

    with _db_lock, db_conn() as conn:
        conn.execute(
            """
            INSERT INTO tag_index(note_id, title, tags, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(note_id) DO UPDATE SET title=excluded.title, tags=excluded.tags, updated_at=excluded.updated_at
            """,
            (note_id, payload.get("title"), json.dumps(payload.get("tags", [])), time.time()),
        )
        conn.execute(
            """
            UPDATE inbox SET status = 'processed', processed_at = ?, error = NULL
            WHERE message_id = ?
            """,
            (time.time(), message_id),
        )
    publish_success(channel, event)
    consumed_events.append({**record, "consumed_at": time.time()})
    channel.basic_ack(delivery_tag=delivery_tag)
    print(f"[consumer] OK note={note_id}", flush=True)

def consumer_loop():
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            ensure_topology(channel)
            channel.basic_qos(prefetch_count=1)

            def _callback(ch, method, properties, body):
                try:
                    handle_message(ch, body, method.delivery_tag)
                except Exception as exc:

                    print(f"[consumer] error {exc}", flush=True)
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    except Exception:
                        pass

            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=_callback)
            print(f"[consumer] listening on {QUEUE_NAME} force_fail={FORCE_FAIL}", flush=True)
            channel.start_consuming()
        except Exception as exc:
            print(f"[consumer] reconnect in 3s due to {exc}", flush=True)
            time.sleep(3)

@app.on_event("startup")
def start_consumer():
    init_db()
    threading.Thread(target=consumer_loop, name="rabbit-consumer", daemon=True).start()
    threading.Thread(target=discovery_loop, name="discovery", daemon=True).start()

@app.get("/health")
def health():
    with _db_lock, db_conn() as conn:
        inbox_count = conn.execute("SELECT COUNT(*) AS c FROM inbox").fetchone()["c"]
        failed = conn.execute("SELECT COUNT(*) AS c FROM inbox WHERE status='failed'").fetchone()["c"]
        indexed = conn.execute("SELECT COUNT(*) AS c FROM tag_index").fetchone()["c"]
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
        "force_fail": FORCE_FAIL,
        "inbox": inbox_count,
        "failed": failed,
        "indexed_notes": indexed,
        "consumed_ok": len(consumed_events),
        "consumed_fail": len(failed_events),
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
    with _db_lock, db_conn() as conn:
        rows = conn.execute("SELECT * FROM tag_index").fetchall()
    return {
        "items": [dict(r) for r in rows],
        "count": len(rows),
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }

@app.get("/events")
def get_events():
    return {
        "ok": consumed_events[-50:],
        "failed": failed_events[-50:],
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }

@app.get("/inbox")
def get_inbox():
    with _db_lock, db_conn() as conn:
        rows = conn.execute("SELECT * FROM inbox ORDER BY received_at DESC LIMIT 50").fetchall()
    return {"items": [dict(r) for r in rows], "count": len(rows)}

from common_obs import mount_obs, set_shard_notes, set_replication_lag
mount_obs(app, role="shard")
