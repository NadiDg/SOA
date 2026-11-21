
from __future__ import annotations

import os
import socket
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode

import httpx
import jwt
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jwt import PyJWKClient
from pydantic import BaseModel, Field

SERVICE_NAME = os.getenv("SERVICE_NAME", "gateway-service")
INSTANCE_ID = os.getenv("INSTANCE_ID") or socket.gethostname()
AUTH_URL = os.getenv("AUTH_URL", "http://auth:8000")
NOTEBOOK_URL = os.getenv("NOTEBOOK_URL", "http://notebook:8000")
TAG_URL = os.getenv("TAG_URL", "http://tag:8000")
DISCOVERY_URL = os.getenv("DISCOVERY_URL", "http://discovery:8000")
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "http://gateway:8000")

KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080").rstrip("/")
KEYCLOAK_PUBLIC_URL = os.getenv("KEYCLOAK_PUBLIC_URL", "http://localhost:8081").rstrip("/")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "notebook")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "notebook-gateway")
KEYCLOAK_CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET", "")
OAUTH_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI", "http://localhost:8080/oauth/callback")

ISSUER = f"{KEYCLOAK_PUBLIC_URL}/realms/{KEYCLOAK_REALM}"
JWKS_URL = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
TOKEN_URL = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token"
AUTHZ_URL = f"{KEYCLOAK_PUBLIC_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/auth"

CORS_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "CORS_ORIGINS",
        "https://cad.kpi.ua,http://cad.kpi.ua,https://www.cad.kpi.ua,http://localhost:8080,http://127.0.0.1:8080",
    ).split(",")
    if o.strip()
]

app = FastAPI(title=SERVICE_NAME)
_jwks_client: Optional[PyJWKClient] = None
_jwks_lock = threading.Lock()

static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["*"],
    expose_headers=["*"],
)

def discovery_loop():
    payload = {
        "service_name": SERVICE_NAME,
        "instance_id": INSTANCE_ID,
        "base_url": SERVICE_BASE_URL,
        "meta": {"role": "edge-secure"},
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

class NoteCreate(BaseModel):
    title: str
    content: str = ""
    tags: List[str] = []

def get_jwks_client() -> PyJWKClient:
    global _jwks_client
    with _jwks_lock:
        if _jwks_client is None:
            _jwks_client = PyJWKClient(JWKS_URL, cache_keys=True)
        return _jwks_client

def extract_scopes(claims: Dict[str, Any]) -> Set[str]:
    scopes: Set[str] = set()
    scope_claim = claims.get("scope") or claims.get("scp") or ""
    if isinstance(scope_claim, str):
        scopes.update(s for s in scope_claim.split() if s)
    elif isinstance(scope_claim, list):
        scopes.update(str(s) for s in scope_claim)
    realm_access = claims.get("realm_access") or {}
    scopes.update(str(r) for r in (realm_access.get("roles") or []))
    resource_access = claims.get("resource_access") or {}
    client_roles = (resource_access.get(KEYCLOAK_CLIENT_ID) or {}).get("roles") or []
    scopes.update(str(r) for r in client_roles)
    return scopes

def verify_bearer_token(authorization: Optional[str]) -> Dict[str, Any]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    try:
        signing_key = get_jwks_client().get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=ISSUER,
            options={"verify_aud": False},
        )
    except Exception as exc:
        raise HTTPException(status_code=401, detail=f"invalid JWT: {exc}") from exc
    claims["_scopes"] = sorted(extract_scopes(claims))
    return claims

def require_auth():
    async def _dep(authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
        return verify_bearer_token(authorization)

    return _dep

def require_scopes(*needed: str):
    async def _dep(authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
        claims = verify_bearer_token(authorization)
        have = set(claims.get("_scopes") or [])
        missing = [s for s in needed if s not in have]
        if missing:
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "insufficient_scope",
                    "required": list(needed),
                    "missing": missing,
                    "have": sorted(have),
                },
            )
        return claims

    return _dep

async def call_json(method: str, url: str, **kwargs) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.request(method, url, **kwargs)
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json() if response.content else {}

async def _ensure_local_user(email: str) -> Dict[str, Any]:
    password = os.getenv("SHADOW_USER_PASSWORD", "KeycloakBridge1!")
    async with httpx.AsyncClient(timeout=10.0) as client:
        reg = await client.post(
            f"{AUTH_URL}/register",
            json={"email": email, "password": password, "display_name": email.split("@")[0]},
        )
        if reg.status_code < 400:
            return reg.json()
        login = await client.post(f"{AUTH_URL}/login", json={"email": email, "password": password})
        if login.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"local auth bridge failed: {login.text}")
        return login.json()

def actor_email(claims: Dict[str, Any]) -> str:
    if claims.get("email"):
        return str(claims["email"])
    username = claims.get("preferred_username") or claims.get("sub") or "user"
    return f"{username}@keycloak.local"

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "instance": INSTANCE_ID,
        "cors_origins": CORS_ORIGINS,
        "oidc_issuer": ISSUER,
        "client_id": KEYCLOAK_CLIENT_ID,
    }

@app.get("/", response_class=HTMLResponse)
def root_page():
    login_html = static_dir / "login.html"
    if login_html.exists():
        return HTMLResponse(login_html.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Lab5 Gateway</h1><a href='/oauth/login'>Login with Keycloak</a>")

@app.get("/api/security/info")
def security_info():
    return {
        "cors_origins": CORS_ORIGINS,
        "oauth2": {
            "flow": "authorization_code",
            "issuer": ISSUER,
            "authorize_url": AUTHZ_URL,
            "token_url_internal": TOKEN_URL,
            "jwks_url_internal": JWKS_URL,
            "redirect_uri": OAUTH_REDIRECT_URI,
            "client_id": KEYCLOAK_CLIENT_ID,
            "scopes": ["openid", "profile", "email", "notebook.read", "notebook.write", "tags.read"],
        },
        "protected_routes": {
            "/api/notes [GET]": ["notebook.read"],
            "/api/notes [POST/PUT]": ["notebook.write"],
            "/api/tags/* [GET]": ["tags.read"],
        },
        "external_idp_hint": "In Keycloak Admin add Identity Provider (Google/GitHub/Atlassian) for Login with external account.",
    }

@app.get("/oauth/login")
def oauth_login(state: str = "lab5"):
    params = {
        "client_id": KEYCLOAK_CLIENT_ID,
        "response_type": "code",
        "scope": "openid profile email notebook.read notebook.write tags.read",
        "redirect_uri": OAUTH_REDIRECT_URI,
        "state": state,
    }
    return RedirectResponse(f"{AUTHZ_URL}?{urlencode(params)}")

@app.get("/oauth/callback")
async def oauth_callback(code: str = Query(...), state: str = Query("lab5")):
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": OAUTH_REDIRECT_URI,
        "client_id": KEYCLOAK_CLIENT_ID,
    }
    if KEYCLOAK_CLIENT_SECRET:
        data["client_secret"] = KEYCLOAK_CLIENT_SECRET
    async with httpx.AsyncClient(timeout=20.0) as client:
        token_resp = await client.post(TOKEN_URL, data=data)
    if token_resp.status_code >= 400:
        raise HTTPException(status_code=400, detail=token_resp.text)
    tokens = token_resp.json()
    access = tokens.get("access_token", "")
    html = f"""<!doctype html>
<html><head><meta charset='utf-8'><title>OAuth OK</title></head>
<body style="font-family:Segoe UI;max-width:920px;margin:2rem auto">
<h1>OAuth2 Authorization Code – success</h1>
<p>state={state}</p>
<p>token_type={tokens.get('token_type')} expires_in={tokens.get('expires_in')}</p>
<h3>access_token (JWT)</h3>
<textarea id="tok" style="width:100%;height:140px">{access}</textarea>
<p><button onclick="navigator.clipboard.writeText(document.getElementById('tok').value)">Copy token</button></p>
<pre id="out"></pre>
<script>
async function probe() {{
  const t = document.getElementById('tok').value;
  const r = await fetch('/api/security/me', {{headers: {{Authorization: 'Bearer ' + t}}}});
  document.getElementById('out').textContent = await r.text();
}}
probe();
</script>
</body></html>"""
    return HTMLResponse(html)

@app.get("/api/security/me")
async def security_me(claims: Dict[str, Any] = Depends(require_auth())):
    return {
        "sub": claims.get("sub"),
        "preferred_username": claims.get("preferred_username"),
        "email": claims.get("email"),
        "scopes": claims.get("_scopes"),
        "iss": claims.get("iss"),
    }

@app.get("/api/health/all")
async def health_all():
    result = {"gateway": {"service": SERVICE_NAME, "instance": INSTANCE_ID, "cors_origins": CORS_ORIGINS}}
    for name, url in ("auth", AUTH_URL), ("notebook", NOTEBOOK_URL), ("tag", TAG_URL), ("discovery", DISCOVERY_URL):
        try:
            result[name] = await call_json("GET", f"{url}/health")
        except Exception as exc:
            result[name] = {"status": "down", "error": str(exc)}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            kc = await client.get(f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}")
            result["keycloak"] = {"status": "ok" if kc.status_code == 200 else "down", "realm": KEYCLOAK_REALM}
    except Exception as exc:
        result["keycloak"] = {"status": "down", "error": str(exc)}
    return result

@app.get("/api/discovery")
async def discovery_snapshot():
    return await call_json("GET", f"{DISCOVERY_URL}/services")

@app.post("/api/pipeline")
async def sync_pipeline(body: PipelineRequest, claims: Dict[str, Any] = Depends(require_scopes("notebook.read"))):
    steps = []
    current = body.text
    for url in (AUTH_URL, NOTEBOOK_URL, TAG_URL):
        resp = await call_json("POST", f"{url}/enrich", json={"text": current})
        current = resp["text"]
        steps.append({"via": url, **resp})
    return {
        "mode": "synchronous orchestration",
        "output": current,
        "steps": steps,
        "actor": claims.get("preferred_username"),
        "gateway": {"service": SERVICE_NAME, "instance": INSTANCE_ID},
    }

@app.post("/api/notes")
async def create_note(body: NoteCreate, claims: Dict[str, Any] = Depends(require_scopes("notebook.write"))):
    local = await _ensure_local_user(actor_email(claims))
    return await call_json(
        "POST",
        f"{NOTEBOOK_URL}/notes",
        json=body.model_dump(),
        headers={"Authorization": f"Bearer {local['token']}"},
    )

@app.get("/api/notes")
async def list_notes(claims: Dict[str, Any] = Depends(require_scopes("notebook.read"))):
    local = await _ensure_local_user(actor_email(claims))
    return await call_json("GET", f"{NOTEBOOK_URL}/notes", headers={"Authorization": f"Bearer {local['token']}"})

@app.get("/api/notes/{note_id}")
async def get_note(note_id: str, claims: Dict[str, Any] = Depends(require_scopes("notebook.read"))):
    local = await _ensure_local_user(actor_email(claims))
    return await call_json(
        "GET", f"{NOTEBOOK_URL}/notes/{note_id}", headers={"Authorization": f"Bearer {local['token']}"}
    )

@app.put("/api/notes/{note_id}")
async def update_note(note_id: str, request: Request, claims: Dict[str, Any] = Depends(require_scopes("notebook.write"))):
    local = await _ensure_local_user(actor_email(claims))
    body = await request.json()
    return await call_json(
        "PUT",
        f"{NOTEBOOK_URL}/notes/{note_id}",
        json=body,
        headers={"Authorization": f"Bearer {local['token']}"},
    )

@app.get("/api/tags/events")
async def tag_events(claims: Dict[str, Any] = Depends(require_scopes("tags.read"))):
    return await call_json("GET", f"{TAG_URL}/events")

@app.get("/api/tags/index")
async def tag_index(claims: Dict[str, Any] = Depends(require_scopes("tags.read"))):
    return await call_json("GET", f"{TAG_URL}/index")

@app.get("/api/tags/inbox")
async def tag_inbox(claims: Dict[str, Any] = Depends(require_scopes("tags.read"))):
    return await call_json("GET", f"{TAG_URL}/inbox")

@app.get("/api/outbox")
async def notebook_outbox(claims: Dict[str, Any] = Depends(require_scopes("notebook.read"))):
    return await call_json("GET", f"{NOTEBOOK_URL}/outbox")

@app.get("/api/sagas")
async def notebook_sagas(claims: Dict[str, Any] = Depends(require_scopes("notebook.read"))):
    return await call_json("GET", f"{NOTEBOOK_URL}/sagas")

@app.get("/api/cors-check")
def cors_check():
    return {
        "ok": True,
        "message": "CORS configured for cad.kpi.ua",
        "allow_origins": CORS_ORIGINS,
    }
