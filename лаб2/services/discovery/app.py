# -*- coding: utf-8 -*-
import os
import socket
import time
from typing import Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

SERVICE_NAME = os.getenv("SERVICE_NAME", "discovery-service")
INSTANCE_ID = os.getenv("INSTANCE_ID") or socket.gethostname()
TTL_SECONDS = int(os.getenv("DISCOVERY_TTL", "30"))

app = FastAPI(title=SERVICE_NAME)
registry: Dict[str, Dict] = {}


class Registration(BaseModel):
    service_name: str = Field(min_length=1)
    instance_id: str = Field(min_length=1)
    base_url: str = Field(min_length=1)
    meta: Dict = {}


def _key(service_name: str, instance_id: str) -> str:
    return f"{service_name}::{instance_id}"


def purge_expired() -> None:
    now = time.time()
    expired = [k for k, v in registry.items() if now - v["last_seen"] > TTL_SECONDS]
    for k in expired:
        del registry[k]


@app.get("/health")
def health():
    purge_expired()
    return {"status": "ok", "service": SERVICE_NAME, "instance": INSTANCE_ID, "entries": len(registry)}


@app.post("/register")
def register(body: Registration):
    registry[_key(body.service_name, body.instance_id)] = {
        "service_name": body.service_name,
        "instance_id": body.instance_id,
        "base_url": body.base_url.rstrip("/"),
        "meta": body.meta,
        "last_seen": time.time(),
    }
    return {"status": "registered", "ttl_seconds": TTL_SECONDS}


@app.post("/heartbeat")
def heartbeat(body: Registration):
    return register(body)


@app.get("/services")
def list_services():
    purge_expired()
    grouped: Dict[str, list] = {}
    for item in registry.values():
        grouped.setdefault(item["service_name"], []).append(item)
    return {"services": grouped, "count": len(registry)}


@app.get("/services/{service_name}")
def get_service(service_name: str):
    purge_expired()
    items = [v for v in registry.values() if v["service_name"] == service_name]
    if not items:
        raise HTTPException(status_code=404, detail="service not found")
    return {"service_name": service_name, "instances": items, "count": len(items)}
