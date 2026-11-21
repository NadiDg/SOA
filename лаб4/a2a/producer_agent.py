
from __future__ import annotations

import os
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8080").rstrip("/")
AGENT_NAME = "notebook-producer-agent"
AGENT_URL = os.getenv("PRODUCER_A2A_URL", "http://localhost:9101")
DEMO_EMAIL = os.getenv("DEMO_EMAIL", "nadia@gmail.com")
DEMO_PASSWORD = os.getenv("DEMO_PASSWORD", "pass123")

app = FastAPI(title=AGENT_NAME)
_tasks: Dict[str, Dict[str, Any]] = {}
_token: Optional[str] = None

class SkillRequest(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    content: str = ""
    tags: List[str] = ["ok", "a2a"]
    email: str = DEMO_EMAIL
    password: str = DEMO_PASSWORD

class TaskCreate(BaseModel):
    skill: str
    input: Dict[str, Any] = {}

def agent_card() -> Dict[str, Any]:
    return {
        "name": AGENT_NAME,
        "description": "A2A producer agent wrapping notebook-service message production skills from Lab 3.",
        "url": AGENT_URL,
        "version": "1.0.0",
        "protocol": "A2A",
        "capabilities": {"streaming": False, "pushNotifications": False},
        "skills": [
            {
                "id": "produce_note_created",
                "name": "Produce NoteCreated",
                "description": "Authenticate and create a note, staging an outbox event for consumers.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "content": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["title"],
                },
            },
            {
                "id": "list_produced_notes",
                "name": "List produced notes",
                "description": "List notes currently owned by the producer identity.",
                "input_schema": {"type": "object", "properties": {}},
            },
        ],
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
    }

def _auth(email: str, password: str) -> str:
    global _token
    with httpx.Client(timeout=15.0) as client:
        reg = client.post(
            f"{GATEWAY_URL}/api/auth/register",
            json={"email": email, "password": password, "display_name": "A2A Producer"},
        )
        if reg.status_code >= 400:
            login = client.post(f"{GATEWAY_URL}/api/auth/login", json={"email": email, "password": password})
            login.raise_for_status()
            data = login.json()
        else:
            data = reg.json()
    _token = data["token"]
    return _token

@app.get("/.well-known/agent.json")
@app.get("/agent-card")
def get_agent_card():
    return agent_card()

@app.get("/health")
def health():
    return {"status": "ok", "agent": AGENT_NAME, "tasks": len(_tasks)}

@app.post("/skills/produce_note_created")
def produce_note_created(body: SkillRequest):
    token = _auth(body.email, body.password)
    with httpx.Client(timeout=15.0) as client:
        resp = client.post(
            f"{GATEWAY_URL}/api/notes",
            headers={"Authorization": f"Bearer {token}"},
            json={"title": body.title, "content": body.content, "tags": body.tags},
        )
        resp.raise_for_status()
        note = resp.json()
    task_id = str(uuid.uuid4())
    task = {
        "id": task_id,
        "skill": "produce_note_created",
        "state": "completed",
        "created_at": time.time(),
        "output": note,
        "a2a_message": {
            "type": "NoteCreated",
            "note_id": note.get("note", {}).get("id"),
            "producer": AGENT_NAME,
        },
    }
    _tasks[task_id] = task
    return task

@app.post("/skills/list_produced_notes")
def list_produced_notes(email: str = DEMO_EMAIL, password: str = DEMO_PASSWORD):
    token = _auth(email, password)
    with httpx.Client(timeout=15.0) as client:
        resp = client.get(f"{GATEWAY_URL}/api/notes", headers={"Authorization": f"Bearer {token}"})
        resp.raise_for_status()
        return resp.json()

@app.post("/tasks")
def create_task(body: TaskCreate):
    if body.skill == "produce_note_created":
        return produce_note_created(SkillRequest(**body.input))
    if body.skill == "list_produced_notes":
        return {"state": "completed", "output": list_produced_notes()}
    raise HTTPException(status_code=404, detail="unknown skill")

@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    task = _tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    return task
