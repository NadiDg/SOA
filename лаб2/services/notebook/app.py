# -*- coding: utf-8 -*-
import json
import os
import socket
import threading
import time
import uuid
from typing import Dict, List

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
DISCOVERY_URL = os.getenv("DISCOVERY_URL", "")
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "http://notebook:8000")

app = FastAPI(title=SERVICE_NAME)
notes: Dict[str, Dict] = {}


def discovery_loop():
    if not DISCOVERY_URL:
        return
    payload = {
        "service_name": SERVICE_NAME,
        "instance_id": INSTANCE_ID,
        "base_url": SERVICE_BASE_URL,
        "meta": {"role": "notes"},
    }
    while True:
        try:
            httpx.post(f"{DISCOVERY_URL}/heartbeat", json=payload, timeout=3.0)
        except Exception:
            pass
        time.sleep(10)


@app.on_event("startup")
def on_startup():
    threading.Thread(target=discovery_loop, daemon=True).start()


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


def publish_event(event_type: str, payload: Dict) -> None:
    message = {
        "event_type": event_type,
        "occurred_at": time.time(),
        "producer": SERVICE_NAME,
        "producer_instance": INSTANCE_ID,
        "payload": payload,
    }
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=ROUTING_KEY,
        body=json.dumps(message).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )
    connection.close()


async def validate_token(authorization: str | None) -> Dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(f"{AUTH_URL}/validate", headers={"Authorization": authorization})
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="auth validation failed")
    return response.json()


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": INSTANCE_ID}


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
    note = {
        "id": note_id,
        "owner": user["email"],
        "title": body.title,
        "content": body.content,
        "tags": body.tags,
        "created_at": time.time(),
        "updated_at": time.time(),
        "handled_by": INSTANCE_ID,
    }
    notes[note_id] = note
    try:
        publish_event("NoteCreated", note)
        queued = True
        queue_error = None
    except Exception as exc:  # noqa: BLE001
        queued = False
        queue_error = str(exc)
    return {
        "note": note,
        "async_event_published": queued,
        "queue_error": queue_error,
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
    note = notes.get(note_id)
    if not note:
        raise HTTPException(status_code=404, detail="note not found")
    if note["owner"] != user["email"]:
        raise HTTPException(status_code=403, detail="forbidden")
    if body.title is not None:
        note["title"] = body.title
    if body.content is not None:
        note["content"] = body.content
    if body.tags is not None:
        note["tags"] = body.tags
    note["updated_at"] = time.time()
    note["handled_by"] = INSTANCE_ID
    try:
        publish_event("NoteUpdated", note)
        queued = True
        queue_error = None
    except Exception as exc:  # noqa: BLE001
        queued = False
        queue_error = str(exc)
    return {
        "note": note,
        "async_event_published": queued,
        "queue_error": queue_error,
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }


@app.get("/notes/{note_id}")
async def get_note(note_id: str, authorization: str | None = Header(default=None)):
    await validate_token(authorization)
    note = notes.get(note_id)
    if not note:
        raise HTTPException(status_code=404, detail="note not found")
    return {"note": note, "service": SERVICE_NAME, "instance": INSTANCE_ID}


@app.get("/notes")
async def list_notes(authorization: str | None = Header(default=None)):
    user = await validate_token(authorization)
    owned = [n for n in notes.values() if n["owner"] == user["email"]]
    return {"items": owned, "count": len(owned), "service": SERVICE_NAME, "instance": INSTANCE_ID}
