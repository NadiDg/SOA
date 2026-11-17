
import json
import os
import socket
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import httpx
import pika
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

SERVICE_NAME = os.getenv("SERVICE_NAME", "notebook-service")
INSTANCE_ID = os.getenv("INSTANCE_ID") or socket.gethostname()
AUTH_URL = os.getenv("AUTH_URL", "http://auth:8000")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE = os.getenv("EVENTS_EXCHANGE", "notebook.events")
ROUTING_KEY = os.getenv("NOTE_EVENTS_KEY", "note.changed")
DLQ_QUEUE = os.getenv("NOTE_DLQ", "notebook.note-events.dlq")
SUCCESS_QUEUE = os.getenv("NOTE_SUCCESS_QUEUE", "notebook.saga-success")
DISCOVERY_URL = os.getenv("DISCOVERY_URL", "")
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "http://notebook:8000")
DB_PATH = os.getenv("DB_PATH", "/data/notebook.db")
SAGA_TIMEOUT_SEC = int(os.getenv("SAGA_TIMEOUT_SEC", "8"))

app = FastAPI(title=SERVICE_NAME)
_db_lock = threading.Lock()

class NoteCreate(BaseModel):
    title: str = Field(min_length=1)
    content: str = ""
    tags: List[str] = []

class NoteUpdate(BaseModel):
    title: str | None = None
    content: str | None = None
    tags: List[str] | None = None

class EnrichRequest(BaseModel):
    text: str

def discovery_loop():
    if not DISCOVERY_URL:
        return
    payload = {
        "service_name": SERVICE_NAME,
        "instance_id": INSTANCE_ID,
        "base_url": SERVICE_BASE_URL,
        "meta": {"role": "notes-outbox-saga"},
    }
    while True:
        try:
            httpx.post(f"{DISCOVERY_URL}/heartbeat", json=payload, timeout=3.0)
        except Exception:
            pass
        time.sleep(10)

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
            CREATE TABLE IF NOT EXISTS notes (
                id TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                tags TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                handled_by TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbox (
                id TEXT PRIMARY KEY,
                aggregate_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL,
                published_at REAL,
                publish_attempts INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS saga_log (
                note_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                started_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                reason TEXT
            );
            """
        )

def row_to_note(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "owner": row["owner"],
        "title": row["title"],
        "content": row["content"],
        "tags": json.loads(row["tags"]),
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "handled_by": row["handled_by"],
    }

async def validate_token(authorization: str | None) -> Dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(f"{AUTH_URL}/validate", headers={"Authorization": authorization})
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="auth validation failed")
    return response.json()

def compensate_note(note_id: str, reason: str) -> bool:
    with _db_lock, db_conn() as conn:
        note = conn.execute("SELECT * FROM notes WHERE id = ?", (note_id,)).fetchone()
        if not note:
            return False
        if note["status"] == "compensated":
            conn.execute(
                "UPDATE saga_log SET status = ?, updated_at = ?, reason = ? WHERE note_id = ?",
                ("compensated", time.time(), reason, note_id),
            )
            return True
        conn.execute(
            "UPDATE notes SET status = ?, updated_at = ?, handled_by = ? WHERE id = ?",
            ("compensated", time.time(), INSTANCE_ID, note_id),
        )
        conn.execute(
            "UPDATE saga_log SET status = ?, updated_at = ?, reason = ? WHERE note_id = ?",
            ("compensated", time.time(), reason, note_id),
        )
    print(f"[saga] compensated note={note_id} reason={reason}", flush=True)
    return True

def complete_saga(note_id: str, reason: str = "consumer success") -> None:
    with _db_lock, db_conn() as conn:
        note = conn.execute("SELECT * FROM notes WHERE id = ?", (note_id,)).fetchone()
        if not note or note["status"] == "compensated":
            return
        conn.execute(
            "UPDATE notes SET status = ?, updated_at = ? WHERE id = ?",
            ("active", time.time(), note_id),
        )
        conn.execute(
            "UPDATE saga_log SET status = ?, updated_at = ?, reason = ? WHERE note_id = ?",
            ("completed", time.time(), reason, note_id),
        )

def outbox_publisher_loop():
    while True:
        try:
            with _db_lock, db_conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 20"
                ).fetchall()
            if not rows:
                time.sleep(1)
                continue
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            for row in rows:
                body = row["payload"].encode("utf-8")
                channel.basic_publish(
                    exchange=EXCHANGE,
                    routing_key=ROUTING_KEY,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,
                        message_id=row["id"],
                        headers={"event_type": row["event_type"], "aggregate_id": row["aggregate_id"]},
                    ),
                )
                with _db_lock, db_conn() as conn:
                    conn.execute(
                        "UPDATE outbox SET published_at = ?, publish_attempts = publish_attempts + 1 WHERE id = ?",
                        (time.time(), row["id"]),
                    )
                print(f"[outbox] published id={row['id']} type={row['event_type']}", flush=True)
            connection.close()
        except Exception as exc:
            print(f"[outbox] retry due to {exc}", flush=True)
            time.sleep(2)

def ensure_dlq_topology(channel):
    dlx = os.getenv("NOTE_DLX", "notebook.events.dlx")
    dlq_key = os.getenv("NOTE_DLQ_KEY", "note.changed.dlq")
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=dlx, exchange_type="topic", durable=True)
    channel.queue_declare(queue=DLQ_QUEUE, durable=True)
    channel.queue_bind(queue=DLQ_QUEUE, exchange=dlx, routing_key=dlq_key)

def dlq_consumer_loop():
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            ensure_dlq_topology(channel)

            def _callback(ch, method, properties, body):
                try:
                    event = json.loads(body.decode("utf-8"))
                    note_id = event.get("payload", {}).get("id") or event.get("aggregate_id")
                    if note_id:
                        compensate_note(note_id, reason="DLQ message received")
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=DLQ_QUEUE, on_message_callback=_callback)
            print(f"[saga] listening DLQ {DLQ_QUEUE}", flush=True)
            channel.start_consuming()
        except Exception as exc:
            print(f"[saga] DLQ reconnect due to {exc}", flush=True)
            time.sleep(3)

def success_consumer_loop():
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            channel.queue_declare(queue=SUCCESS_QUEUE, durable=True)
            channel.queue_bind(queue=SUCCESS_QUEUE, exchange=EXCHANGE, routing_key="note.saga.success")

            def _callback(ch, method, properties, body):
                try:
                    event = json.loads(body.decode("utf-8"))
                    note_id = event.get("payload", {}).get("id")
                    if note_id:
                        complete_saga(note_id, reason="NoteIndexed success event")
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=SUCCESS_QUEUE, on_message_callback=_callback)
            print(f"[saga] listening success queue {SUCCESS_QUEUE}", flush=True)
            channel.start_consuming()
        except Exception as exc:
            print(f"[saga] success reconnect due to {exc}", flush=True)
            time.sleep(3)

def timeout_watcher_loop():
    while True:
        now = time.time()
        with _db_lock, db_conn() as conn:
            rows = conn.execute(
                """
                SELECT note_id, started_at FROM saga_log
                WHERE status = 'started' AND (? - started_at) >= ?
                """,
                (now, SAGA_TIMEOUT_SEC),
            ).fetchall()
        for row in rows:
            compensate_note(row["note_id"], reason=f"saga timeout > {SAGA_TIMEOUT_SEC}s")
        time.sleep(1)

@app.on_event("startup")
def on_startup():
    init_db()
    threading.Thread(target=discovery_loop, daemon=True).start()
    threading.Thread(target=outbox_publisher_loop, name="outbox", daemon=True).start()
    threading.Thread(target=dlq_consumer_loop, name="dlq", daemon=True).start()
    threading.Thread(target=success_consumer_loop, name="success", daemon=True).start()
    threading.Thread(target=timeout_watcher_loop, name="timeout", daemon=True).start()

@app.get("/health")
def health():
    with _db_lock, db_conn() as conn:
        notes = conn.execute("SELECT COUNT(*) AS c FROM notes").fetchone()["c"]
        outbox_pending = conn.execute(
            "SELECT COUNT(*) AS c FROM outbox WHERE published_at IS NULL"
        ).fetchone()["c"]
        sagas = conn.execute("SELECT status, COUNT(*) AS c FROM saga_log GROUP BY status").fetchall()
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
        "notes": notes,
        "outbox_pending": outbox_pending,
        "saga_stats": {r["status"]: r["c"] for r in sagas},
        "saga_timeout_sec": SAGA_TIMEOUT_SEC,
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

@app.post("/notes")
async def create_note(body: NoteCreate, authorization: str | None = Header(default=None)):
    user = await validate_token(authorization)
    note_id = str(uuid.uuid4())
    outbox_id = str(uuid.uuid4())
    now = time.time()
    note = {
        "id": note_id,
        "owner": user["email"],
        "title": body.title,
        "content": body.content,
        "tags": body.tags,
        "status": "pending_saga",
        "created_at": now,
        "updated_at": now,
        "handled_by": INSTANCE_ID,
    }
    event = {
        "event_type": "NoteCreated",
        "occurred_at": now,
        "producer": SERVICE_NAME,
        "producer_instance": INSTANCE_ID,
        "message_id": outbox_id,
        "payload": note,
    }
    with _db_lock, db_conn() as conn:
        conn.execute(
            """
            INSERT INTO notes(id, owner, title, content, tags, status, created_at, updated_at, handled_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                note_id,
                note["owner"],
                note["title"],
                note["content"],
                json.dumps(note["tags"]),
                note["status"],
                now,
                now,
                INSTANCE_ID,
            ),
        )
        conn.execute(
            """
            INSERT INTO outbox(id, aggregate_id, event_type, payload, created_at, published_at, publish_attempts)
            VALUES (?, ?, ?, ?, ?, NULL, 0)
            """,
            (outbox_id, note_id, "NoteCreated", json.dumps(event), now),
        )
        conn.execute(
            """
            INSERT INTO saga_log(note_id, status, started_at, updated_at, reason)
            VALUES (?, 'started', ?, ?, ?)
            """,
            (note_id, now, now, "waiting for consumer or timeout/DLQ"),
        )
    return {
        "note": note,
        "pattern": "transactional outbox + saga",
        "outbox_id": outbox_id,
        "async_event_published": False,
        "async_event_staged_in_outbox": True,
        "sync_auth_call": {
            "target": f"{AUTH_URL}/validate",
            "user": user["email"],
            "auth_instance": user.get("instance"),
        },
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }

@app.put("/notes/{note_id}")
async def update_note(note_id: str, body: NoteUpdate, authorization: str | None = Header(default=None)):
    user = await validate_token(authorization)
    outbox_id = str(uuid.uuid4())
    now = time.time()
    with _db_lock, db_conn() as conn:
        row = conn.execute("SELECT * FROM notes WHERE id = ?", (note_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="note not found")
        if row["owner"] != user["email"]:
            raise HTTPException(status_code=403, detail="forbidden")
        title = body.title if body.title is not None else row["title"]
        content = body.content if body.content is not None else row["content"]
        tags = body.tags if body.tags is not None else json.loads(row["tags"])
        conn.execute(
            """
            UPDATE notes
            SET title = ?, content = ?, tags = ?, status = ?, updated_at = ?, handled_by = ?
            WHERE id = ?
            """,
            (title, content, json.dumps(tags), "pending_saga", now, INSTANCE_ID, note_id),
        )
        note = {
            "id": note_id,
            "owner": row["owner"],
            "title": title,
            "content": content,
            "tags": tags,
            "status": "pending_saga",
            "created_at": row["created_at"],
            "updated_at": now,
            "handled_by": INSTANCE_ID,
        }
        event = {
            "event_type": "NoteUpdated",
            "occurred_at": now,
            "producer": SERVICE_NAME,
            "producer_instance": INSTANCE_ID,
            "message_id": outbox_id,
            "payload": note,
        }
        conn.execute(
            """
            INSERT INTO outbox(id, aggregate_id, event_type, payload, created_at, published_at, publish_attempts)
            VALUES (?, ?, ?, ?, ?, NULL, 0)
            """,
            (outbox_id, note_id, "NoteUpdated", json.dumps(event), now),
        )
        conn.execute(
            """
            INSERT INTO saga_log(note_id, status, started_at, updated_at, reason)
            VALUES (?, 'started', ?, ?, ?)
            ON CONFLICT(note_id) DO UPDATE SET status='started', started_at=excluded.started_at,
              updated_at=excluded.updated_at, reason=excluded.reason
            """,
            (note_id, now, now, "update saga started"),
        )
    return {
        "note": note,
        "outbox_id": outbox_id,
        "async_event_staged_in_outbox": True,
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }

@app.get("/notes/{note_id}")
async def get_note(note_id: str, authorization: str | None = Header(default=None)):
    await validate_token(authorization)
    with _db_lock, db_conn() as conn:
        row = conn.execute("SELECT * FROM notes WHERE id = ?", (note_id,)).fetchone()
        saga = conn.execute("SELECT * FROM saga_log WHERE note_id = ?", (note_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="note not found")
    return {
        "note": row_to_note(row),
        "saga": dict(saga) if saga else None,
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }

@app.get("/notes")
async def list_notes(authorization: str | None = Header(default=None)):
    user = await validate_token(authorization)
    with _db_lock, db_conn() as conn:
        rows = conn.execute("SELECT * FROM notes WHERE owner = ? ORDER BY created_at DESC", (user["email"],)).fetchall()
    return {
        "items": [row_to_note(r) for r in rows],
        "count": len(rows),
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }

@app.get("/outbox")
def list_outbox():
    with _db_lock, db_conn() as conn:
        rows = conn.execute("SELECT * FROM outbox ORDER BY created_at DESC LIMIT 50").fetchall()
    return {
        "items": [dict(r) for r in rows],
        "count": len(rows),
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }

@app.get("/sagas")
def list_sagas():
    with _db_lock, db_conn() as conn:
        rows = conn.execute("SELECT * FROM saga_log ORDER BY started_at DESC LIMIT 50").fetchall()
    return {
        "items": [dict(r) for r in rows],
        "count": len(rows),
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }
