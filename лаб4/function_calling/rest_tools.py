
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

import httpx

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8080").rstrip("/")
DEFAULT_EMAIL = os.getenv("DEMO_EMAIL", "nadia@gmail.com")
DEFAULT_PASSWORD = os.getenv("DEMO_PASSWORD", "pass123")

INJECTION_PATTERNS = [
    r"ignore\s+(all\s+)?previous\s+instructions",
    r"system\s*prompt",
    r"reveal\s+(your\s+)?(secrets?|api[_\s-]?keys?)",
    r"<\s*script\s*>",
    r"sudo\s+",
    r"drop\s+table",
    r"rm\s+-rf",
]

class GuardrailError(ValueError):
    pass

def validate_text(value: str, field: str = "text") -> str:
    if value is None:
        return ""
    text = str(value)
    if len(text) > 2000:
        raise GuardrailError(f"{field} exceeds 2000 characters")
    lowered = text.lower()
    for pattern in INJECTION_PATTERNS:
        if re.search(pattern, lowered):
            raise GuardrailError(f"prompt injection pattern blocked in {field}")
    return text

def validate_email(email: str) -> str:
    email = validate_text(email, "email").strip()
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        raise GuardrailError("invalid email format")
    return email

class NotebookRestClient:
    def __init__(self, base_url: str = GATEWAY_URL):
        self.base_url = base_url.rstrip("/")
        self.token: Optional[str] = None
        self.email: Optional[str] = None

    def _headers(self) -> Dict[str, str]:
        if not self.token:
            raise GuardrailError("not authenticated; call register_or_login first")
        return {"Authorization": f"Bearer {self.token}"}

    def register_or_login(self, email: str, password: str, display_name: str = "Agent User") -> Dict[str, Any]:
        email = validate_email(email)
        password = validate_text(password, "password")
        display_name = validate_text(display_name, "display_name")
        with httpx.Client(timeout=15.0) as client:
            reg = client.post(
                f"{self.base_url}/api/auth/register",
                json={"email": email, "password": password, "display_name": display_name},
            )
            if reg.status_code >= 400:
                login = client.post(
                    f"{self.base_url}/api/auth/login",
                    json={"email": email, "password": password},
                )
                login.raise_for_status()
                data = login.json()
            else:
                data = reg.json()
        self.token = data["token"]
        self.email = email
        return {"email": email, "authenticated": True, "instance": data.get("instance")}

    def list_notes(self) -> Dict[str, Any]:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(f"{self.base_url}/api/notes", headers=self._headers())
            resp.raise_for_status()
            return resp.json()

    def get_note(self, note_id: str) -> Dict[str, Any]:
        note_id = validate_text(note_id, "note_id")
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(f"{self.base_url}/api/notes/{note_id}", headers=self._headers())
            resp.raise_for_status()
            return resp.json()

    def create_note(self, title: str, content: str = "", tags: Optional[List[str]] = None) -> Dict[str, Any]:
        title = validate_text(title, "title")
        content = validate_text(content, "content")
        tags = [validate_text(t, "tag") for t in (tags or ["ok"])]
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(
                f"{self.base_url}/api/notes",
                headers=self._headers(),
                json={"title": title, "content": content, "tags": tags},
            )
            resp.raise_for_status()
            return resp.json()

    def update_note(
        self,
        note_id: str,
        title: Optional[str] = None,
        content: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        note_id = validate_text(note_id, "note_id")
        body: Dict[str, Any] = {}
        if title is not None:
            body["title"] = validate_text(title, "title")
        if content is not None:
            body["content"] = validate_text(content, "content")
        if tags is not None:
            body["tags"] = [validate_text(t, "tag") for t in tags]
        with httpx.Client(timeout=15.0) as client:
            resp = client.put(
                f"{self.base_url}/api/notes/{note_id}",
                headers=self._headers(),
                json=body,
            )
            resp.raise_for_status()
            return resp.json()

    def find_notes_by_owner_hint(self, owner_email: str) -> Dict[str, Any]:

        owner_email = validate_email(owner_email)
        if self.email and self.email.lower() != owner_email.lower():
            raise GuardrailError("owner_email does not match authenticated user")
        return self.list_notes()

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "register_or_login",
            "description": "Register or login a notebook user and store JWT for next calls.",
            "parameters": {
                "type": "object",
                "properties": {
                    "email": {"type": "string"},
                    "password": {"type": "string"},
                    "display_name": {"type": "string"},
                },
                "required": ["email", "password"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_notes",
            "description": "List notes of the authenticated user.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_note",
            "description": "Create a note. Use tags including 'ok' for successful indexing path.",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "content": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["title"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_note",
            "description": "Update an existing note by id.",
            "parameters": {
                "type": "object",
                "properties": {
                    "note_id": {"type": "string"},
                    "title": {"type": "string"},
                    "content": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["note_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "find_notes_by_owner_hint",
            "description": "Find notes for a given owner email (must be the authenticated user).",
            "parameters": {
                "type": "object",
                "properties": {"owner_email": {"type": "string"}},
                "required": ["owner_email"],
            },
        },
    },
]

def dispatch_tool(client: NotebookRestClient, name: str, arguments: Dict[str, Any]) -> Any:
    if not hasattr(client, name):
        raise GuardrailError(f"unknown tool: {name}")
    method = getattr(client, name)
    return method(**(arguments or {}))
