
from __future__ import annotations

import os
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8080").rstrip("/")
PRODUCER_URL = os.getenv("PRODUCER_A2A_URL", "http://localhost:9101").rstrip("/")
AGENT_NAME = "tag-consumer-agent"
AGENT_URL = os.getenv("CONSUMER_A2A_URL", "http://localhost:9102")

app = FastAPI(title=AGENT_NAME)
_tasks: Dict[str, Dict[str, Any]] = {}
_consumed: List[Dict[str, Any]] = []

class ConsumeRequest(BaseModel):
    title: str = Field(default="A2A from consumer", max_length=200)
    content: str = "produced via A2A integration"
    tags: List[str] = ["ok", "a2a", "consumer"]

def agent_card() -> Dict[str, Any]:
    return {
        "name": AGENT_NAME,
        "description": "A2A consumer agent that discovers producer skills and reacts to produced notes.",
        "url": AGENT_URL,
        "version": "1.0.0",
        "protocol": "A2A",
        "skills": [
            {
                "id": "discover_producer",
                "name": "Discover producer Agent Card",
                "description": "Fetch producer /.well-known/agent.json and list skills.",
            },
            {
                "id": "consume_from_producer",
                "name": "Consume from producer",
                "description": "Ask producer to produce a note, then verify consumer-side visibility via tag inbox/events.",
            },
            {
                "id": "read_tag_events",
                "name": "Read tag events",
                "description": "Read Lab3 tag-service events through gateway.",
            },
        ],
    }

@app.get("/.well-known/agent.json")
@app.get("/agent-card")
def get_agent_card():
    return agent_card()

@app.get("/health")
def health():
    return {"status": "ok", "agent": AGENT_NAME, "consumed": len(_consumed), "tasks": len(_tasks)}

@app.post("/skills/discover_producer")
def discover_producer():
    with httpx.Client(timeout=15.0) as client:
        for path in ("/.well-known/agent.json", "/agent-card"):
            resp = client.get(f"{PRODUCER_URL}{path}")
            if resp.status_code == 200:
                card = resp.json()
                return {"producer_url": PRODUCER_URL, "agent_card": card, "skills": card.get("skills", [])}
    raise HTTPException(status_code=502, detail="producer agent card unavailable")

@app.post("/skills/read_tag_events")
def read_tag_events():
    with httpx.Client(timeout=15.0) as client:
        events = client.get(f"{GATEWAY_URL}/api/tags/events").json()
        inbox = client.get(f"{GATEWAY_URL}/api/tags/inbox").json()
    return {"events": events, "inbox": inbox}

@app.post("/skills/consume_from_producer")
def consume_from_producer(body: ConsumeRequest):
    discovery = discover_producer()
    with httpx.Client(timeout=20.0) as client:
        produced = client.post(
            f"{PRODUCER_URL}/skills/produce_note_created",
            json={"title": body.title, "content": body.content, "tags": body.tags},
        )
        produced.raise_for_status()
        produced_task = produced.json()
        time.sleep(2)
        events = client.get(f"{GATEWAY_URL}/api/tags/events").json()
        inbox = client.get(f"{GATEWAY_URL}/api/tags/inbox").json()

    task_id = str(uuid.uuid4())
    record = {
        "id": task_id,
        "skill": "consume_from_producer",
        "state": "completed",
        "created_at": time.time(),
        "producer_discovery": {
            "name": discovery["agent_card"].get("name"),
            "skills": [s.get("id") for s in discovery.get("skills", [])],
        },
        "producer_task": produced_task,
        "consumer_observation": {"events": events, "inbox": inbox},
    }
    _tasks[task_id] = record
    _consumed.append(record)
    return record

@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    task = _tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    return task
