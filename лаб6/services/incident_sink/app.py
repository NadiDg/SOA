
from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List

from fastapi import FastAPI, Request

app = FastAPI(title="incident-sink")
TICKETS: List[Dict[str, Any]] = []

@app.get("/health")
def health():
    return {"status": "ok", "tickets": len(TICKETS)}

@app.get("/tickets")
def list_tickets():
    return {"items": TICKETS[-100:], "count": len(TICKETS)}

@app.post("/tickets")
async def create_ticket(request: Request):
    body = await request.json()
    ticket = {
        "id": str(uuid.uuid4()),
        "created_at": time.time(),
        "source": body.get("source", "manual"),
        "title": body.get("title") or body.get("summary") or "SOA incident",
        "severity": body.get("severity", "critical"),
        "description": body.get("description") or body.get("text") or "",
        "raw": body,
    }
    TICKETS.append(ticket)
    print({"event": "ticket_created", **{k: ticket[k] for k in ("id", "title", "severity", "source")}}, flush=True)
    return ticket

@app.post("/alerts/alertmanager")
async def alertmanager_webhook(request: Request):
    payload = await request.json()
    created = []
    for alert in payload.get("alerts", [payload]):
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        ticket = {
            "id": str(uuid.uuid4()),
            "created_at": time.time(),
            "source": "alertmanager",
            "title": labels.get("alertname") or annotations.get("summary") or "Alert",
            "severity": labels.get("severity", "warning"),
            "description": annotations.get("description") or str(alert),
            "status": alert.get("status", payload.get("status")),
            "raw": alert,
        }
        TICKETS.append(ticket)
        created.append(ticket)
    return {"created": len(created), "items": created}
