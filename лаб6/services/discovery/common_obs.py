
from __future__ import annotations

import json
import os
import socket
import time
import uuid
from contextvars import ContextVar
from typing import Callable, Optional

CURRENT_TRACE_ID: ContextVar[str] = ContextVar("current_trace_id", default="")

from fastapi import Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

SERVICE_NAME = os.getenv("SERVICE_NAME", "service")
INSTANCE_ID = os.getenv("INSTANCE_ID") or socket.gethostname()
ROLE = os.getenv("OBS_ROLE", "worker")

REQUESTS = Counter(
    "soa_http_requests_total",
    "HTTP requests",
    ["service", "role", "instance", "method", "endpoint", "status"],
)
ERRORS = Counter(
    "soa_http_errors_total",
    "HTTP 5xx/4xx errors",
    ["service", "role", "instance", "endpoint", "status"],
)
LATENCY = Histogram(
    "soa_http_request_duration_seconds",
    "HTTP request latency",
    ["service", "role", "instance", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
INFLIGHT = Gauge(
    "soa_http_inflight_requests",
    "In-flight HTTP requests",
    ["service", "role", "instance"],
)
UP = Gauge("soa_service_up", "Service up gauge", ["service", "role", "instance"])
REPLICATION_LAG = Gauge(
    "soa_replication_lag_seconds",
    "Synthetic replication lag for demo alerts",
    ["service", "role", "instance"],
)
SHARD_NOTES = Gauge(
    "soa_shard_notes_total",
    "Notes stored on shard (business metric)",
    ["service", "role", "instance"],
)

def slog(level: str, message: str, **fields):
    payload = {
        "ts": time.time(),
        "level": level,
        "service": SERVICE_NAME,
        "role": ROLE,
        "instance": INSTANCE_ID,
        "message": message,
        **fields,
    }
    print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)

class ObservabilityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):

        if request.url.path == "/metrics":
            return await call_next(request)

        header_trace = request.headers.get("x-trace-id")
        traceparent = request.headers.get("traceparent")
        if header_trace:
            trace_id = header_trace
        elif traceparent:
            parts = traceparent.split("-")
            trace_id = parts[1] if len(parts) >= 4 else traceparent
        else:
            trace_id = str(uuid.uuid4())
        span_id = str(uuid.uuid4())[:16]
        request.state.trace_id = trace_id
        request.state.span_id = span_id
        trace_token = CURRENT_TRACE_ID.set(trace_id)

        endpoint = request.url.path
        method = request.method
        INFLIGHT.labels(SERVICE_NAME, ROLE, INSTANCE_ID).inc()
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["X-Trace-Id"] = trace_id
            response.headers["X-Span-Id"] = span_id
            response.headers["X-Service-Instance"] = INSTANCE_ID
            return response
        except Exception as exc:
            slog("error", "unhandled_exception", trace_id=trace_id, span_id=span_id, error=str(exc))
            raise
        finally:
            CURRENT_TRACE_ID.reset(trace_token)
            elapsed = time.perf_counter() - start
            INFLIGHT.labels(SERVICE_NAME, ROLE, INSTANCE_ID).dec()
            REQUESTS.labels(SERVICE_NAME, ROLE, INSTANCE_ID, method, endpoint, str(status)).inc()
            LATENCY.labels(SERVICE_NAME, ROLE, INSTANCE_ID, endpoint).observe(elapsed)
            if status >= 400:
                ERRORS.labels(SERVICE_NAME, ROLE, INSTANCE_ID, endpoint, str(status)).inc()
            slog(
                "info",
                "http_request",
                trace_id=trace_id,
                span_id=span_id,
                method=method,
                endpoint=endpoint,
                status=status,
                latency_ms=round(elapsed * 1000, 3),
            )

def mount_obs(app, role: str = "worker"):
    global ROLE
    ROLE = role
    os.environ["OBS_ROLE"] = role
    UP.labels(SERVICE_NAME, ROLE, INSTANCE_ID).set(1)
    REPLICATION_LAG.labels(SERVICE_NAME, ROLE, INSTANCE_ID).set(0)
    app.add_middleware(ObservabilityMiddleware)

    @app.get("/metrics")
    def metrics():
        return StarletteResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app

def set_replication_lag(seconds: float):
    REPLICATION_LAG.labels(SERVICE_NAME, ROLE, INSTANCE_ID).set(seconds)

def set_shard_notes(count: int):
    SHARD_NOTES.labels(SERVICE_NAME, ROLE, INSTANCE_ID).set(count)
