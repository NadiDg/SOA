# -*- coding: utf-8 -*-
import os
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict

import httpx
import jwt
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

SERVICE_NAME = os.getenv("SERVICE_NAME", "auth-service")
INSTANCE_ID = os.getenv("INSTANCE_ID") or socket.gethostname()
JWT_SECRET = os.getenv("JWT_SECRET", "soa-lab2-secret")
JWT_ALG = "HS256"
DISCOVERY_URL = os.getenv("DISCOVERY_URL", "")
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "http://auth:8000")

app = FastAPI(title=SERVICE_NAME)
users: Dict[str, Dict] = {}


def discovery_loop():
    if not DISCOVERY_URL:
        return
    payload = {
        "service_name": SERVICE_NAME,
        "instance_id": INSTANCE_ID,
        "base_url": SERVICE_BASE_URL,
        "meta": {"role": "identity"},
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


class RegisterRequest(BaseModel):
    email: str = Field(min_length=3)
    password: str = Field(min_length=4)
    display_name: str = "User"


class LoginRequest(BaseModel):
    email: str
    password: str


class EnrichRequest(BaseModel):
    text: str
    token: str | None = None


def issue_token(email: str) -> str:
    payload = {
        "sub": email,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=12),
        "iss": SERVICE_NAME,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": INSTANCE_ID}


@app.post("/register")
def register(body: RegisterRequest):
    if body.email in users:
        raise HTTPException(status_code=409, detail="user already exists")
    users[body.email] = {
        "email": body.email,
        "password": body.password,
        "display_name": body.display_name,
        "created_at": time.time(),
    }
    token = issue_token(body.email)
    return {
        "email": body.email,
        "display_name": body.display_name,
        "token": token,
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }


@app.post("/login")
def login(body: LoginRequest):
    user = users.get(body.email)
    if not user or user["password"] != body.password:
        raise HTTPException(status_code=401, detail="invalid credentials")
    token = issue_token(body.email)
    return {
        "email": body.email,
        "token": token,
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
    }


@app.get("/validate")
def validate(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail=f"invalid token: {exc}") from exc
    email = payload.get("sub")
    user = users.get(email)
    if not user:
        raise HTTPException(status_code=401, detail="unknown user")
    return {
        "valid": True,
        "email": email,
        "display_name": user["display_name"],
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
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
