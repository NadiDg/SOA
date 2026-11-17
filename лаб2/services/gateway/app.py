# -*- coding: utf-8 -*-
import os
import socket
import threading
import time
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

SERVICE_NAME = os.getenv("SERVICE_NAME", "gateway-service")
INSTANCE_ID = os.getenv("INSTANCE_ID") or socket.gethostname()
AUTH_URL = os.getenv("AUTH_URL", "http://auth:8000")
NOTEBOOK_URL = os.getenv("NOTEBOOK_URL", "http://notebook:8000")
TAG_URL = os.getenv("TAG_URL", "http://tag:8000")
DISCOVERY_URL = os.getenv("DISCOVERY_URL", "http://discovery:8000")
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "http://gateway:8000")

app = FastAPI(title=SERVICE_NAME)


def discovery_loop():
    payload = {
        "service_name": SERVICE_NAME,
        "instance_id": INSTANCE_ID,
        "base_url": SERVICE_BASE_URL,
        "meta": {"role": "edge"},
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


class PipelineRequest(BaseModel):
    text: str = Field(default="NOTE")


class RegisterRequest(BaseModel):
    email: str
    password: str
    display_name: str = "User"


class LoginRequest(BaseModel):
    email: str
    password: str


class NoteCreate(BaseModel):
    title: str
    content: str = ""
    tags: List[str] = []


async def call_json(method: str, url: str, **kwargs) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.request(method, url, **kwargs)
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": INSTANCE_ID}


@app.get("/")
def root():
    return {
        "service": SERVICE_NAME,
        "role": "API Gateway / Edge",
        "routes": [
            "POST /api/pipeline",
            "POST /api/auth/register",
            "POST /api/auth/login",
            "POST /api/notes",
            "GET /api/notes",
            "GET /api/tags/events",
            "GET /api/tags/index",
            "GET /api/whoami-notebook",
        ],
    }


@app.post("/api/pipeline")
async def sync_pipeline(body: PipelineRequest):

    steps = []
    current = body.text

    auth_resp = await call_json("POST", f"{AUTH_URL}/enrich", json={"text": current})
    current = auth_resp["text"]
    steps.append({"step": 1, "via": AUTH_URL, **auth_resp})

    notebook_resp = await call_json("POST", f"{NOTEBOOK_URL}/enrich", json={"text": current})
    current = notebook_resp["text"]
    steps.append({"step": 2, "via": NOTEBOOK_URL, **notebook_resp})

    tag_resp = await call_json("POST", f"{TAG_URL}/enrich", json={"text": current})
    current = tag_resp["text"]
    steps.append({"step": 3, "via": TAG_URL, **tag_resp})

    return {
        "mode": "synchronous orchestration",
        "protocol": "HTTP/JSON",
        "discovery": "Docker Compose DNS names auth/notebook/tag",
        "input": body.text,
        "output": current,
        "steps": steps,
        "gateway": {"service": SERVICE_NAME, "instance": INSTANCE_ID},
    }


@app.post("/api/auth/register")
async def register(body: RegisterRequest):
    return await call_json("POST", f"{AUTH_URL}/register", json=body.model_dump())


@app.post("/api/auth/login")
async def login(body: LoginRequest):
    return await call_json("POST", f"{AUTH_URL}/login", json=body.model_dump())


@app.post("/api/notes")
async def create_note(body: NoteCreate, authorization: str | None = Header(default=None)):
    headers = {"Authorization": authorization} if authorization else {}
    return await call_json("POST", f"{NOTEBOOK_URL}/notes", json=body.model_dump(), headers=headers)


@app.get("/api/notes")
async def list_notes(authorization: str | None = Header(default=None)):
    headers = {"Authorization": authorization} if authorization else {}
    return await call_json("GET", f"{NOTEBOOK_URL}/notes", headers=headers)


@app.get("/api/notes/{note_id}")
async def get_note(note_id: str, authorization: str | None = Header(default=None)):
    headers = {"Authorization": authorization} if authorization else {}
    return await call_json("GET", f"{NOTEBOOK_URL}/notes/{note_id}", headers=headers)


@app.get("/api/tags/events")
async def tag_events():
    return await call_json("GET", f"{TAG_URL}/events")


@app.get("/api/tags/index")
async def tag_index():
    return await call_json("GET", f"{TAG_URL}/index")


@app.get("/api/whoami-notebook")
async def whoami_notebook():
    return await call_json("GET", f"{NOTEBOOK_URL}/health")


@app.get("/api/health/all")
async def health_all():
    result = {"gateway": {"service": SERVICE_NAME, "instance": INSTANCE_ID}}
    for name, url in ("auth", AUTH_URL), ("notebook", NOTEBOOK_URL), ("tag", TAG_URL), ("discovery", DISCOVERY_URL):
        try:
            result[name] = await call_json("GET", f"{url}/health")
        except Exception as exc:  # noqa: BLE001
            result[name] = {"status": "down", "error": str(exc)}
    return result


@app.get("/api/discovery")
async def discovery_snapshot():
    return await call_json("GET", f"{DISCOVERY_URL}/services")


@app.api_route("/proxy/{service}/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def raw_proxy(service: str, path: str, request: Request):
    mapping = {"auth": AUTH_URL, "notebook": NOTEBOOK_URL, "tag": TAG_URL}
    base = mapping.get(service)
    if not base:
        raise HTTPException(status_code=404, detail="unknown service")
    url = f"{base}/{path}"
    body = await request.body()
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.request(
            request.method,
            url,
            content=body,
            headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            params=request.query_params,
        )
    return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {})
