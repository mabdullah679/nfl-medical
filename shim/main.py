from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, get_args, get_origin

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from .checks import REQUIRED_ROLES, ROUTERS
from .config import settings
from .deps import get_registry, set_registry
from .result import CheckState
from .roles import RoleConfigurationError, RoleRegistry

SHIM_VERSION = "1.0.0"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    registry = RoleRegistry.load()
    registry.validate_environment(REQUIRED_ROLES)
    set_registry(registry)
    yield


app = FastAPI(
    title="NFL Data Platform check shim",
    version=SHIM_VERSION,
    description=(
        "Turns every non-HTTP pipeline check into an HTTP request. Each check runs "
        "under its own dedicated least-privilege identity declared in shim/roles/."
    ),
    lifespan=lifespan,
)

meta = APIRouter(tags=["meta"])


@app.exception_handler(HTTPException)
async def structured_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
    if isinstance(exc.detail, dict):
        return JSONResponse(status_code=exc.status_code, content=exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "state": CheckState.UNVERIFIABLE.value,
            "reason": str(exc.detail),
            "summary": f"Request failed: {exc.detail}",
        },
    )


@app.exception_handler(RequestValidationError)
async def invalid_parameters(request: Request, exc: RequestValidationError) -> JSONResponse:
    problems = [
        f"{'.'.join(str(p) for p in err.get('loc', ()) if p != 'query')}: {err.get('msg')}"
        for err in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content={
            "ok": False,
            "state": CheckState.UNVERIFIABLE.value,
            "reason": "bad parameters; the check did not run: " + "; ".join(problems),
            "summary": (
                "Check did NOT run: " + "; ".join(problems) + ". See GET /checks for "
                "each endpoint's parameters."
            ),
            "path": str(request.url.path),
        },
    )


@app.exception_handler(Exception)
async def unexpected_exception(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "ok": False,
            "state": CheckState.UNVERIFIABLE.value,
            "reason": f"unhandled {type(exc).__name__}: {exc}",
            "summary": (
                "Check could NOT be evaluated due to an unexpected shim error. This is "
                "not a pass and not a failed check."
            ),
            "path": str(request.url.path),
        },
    )


@meta.get("/health")
def health() -> JSONResponse:
    registry = get_registry()
    endpoints = settings.dependency_endpoints()
    wanted = registry.dependency_names()

    from .clients import tcp_reachable

    def probe(name: str) -> tuple[str, dict[str, Any]]:
        ep = endpoints.get(name)
        if ep is None:
            return name, {
                "reachable": None,
                "detail": "no endpoint configured for this dependency",
            }
        reachable, detail = tcp_reachable(ep)
        return name, {
            "target": ep.label,
            "reachable": reachable,
            "detail": detail,
            "proves": (
                "a TCP listener is accepting; it does NOT prove authentication or "
                "schema readiness"
            ),
        }

    with ThreadPoolExecutor(max_workers=max(1, len(wanted))) as pool:
        results = dict(pool.map(probe, wanted))

    unreachable = sorted(k for k, v in results.items() if v.get("reachable") is False)
    status_code = 200 if not unreachable else 503
    return JSONResponse(
        status_code=status_code,
        content={
            "ok": not unreachable,
            "shim": "up",
            "version": SHIM_VERSION,
            "roles_loaded": len(registry.all()),
            "checks_registered": len(REQUIRED_ROLES),
            "dependencies": results,
            "dependencies_unreachable": unreachable,
            "summary": (
                "Shim is up and every declared dependency has a listener."
                if not unreachable
                else f"Shim is up but these dependencies are unreachable: "
                f"{', '.join(unreachable)}. Checks that need them will return 503."
            ),
        },
    )


def _param_spec(route: APIRoute) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for dep in route.dependant.query_params:
        annotation = dep.field_info.annotation
        if get_origin(annotation) is not None:
            allowed = [str(a) for a in get_args(annotation)]
            type_name = "enum"
        else:
            allowed = []
            type_name = getattr(annotation, "__name__", str(annotation))
        spec: dict[str, Any] = {
            "name": dep.name,
            "required": dep.field_info.is_required(),
            "type": type_name,
            "description": dep.field_info.description or "",
        }
        if allowed:
            spec["allowed_values"] = allowed
        examples = getattr(dep.field_info, "examples", None)
        if examples:
            spec["example"] = examples[0]
        out.append(spec)
    return out


@meta.get("/checks")
def checks_index() -> dict[str, Any]:
    registry = get_registry()
    by_endpoint = {r.endpoint: r for r in registry.all()}

    entries: list[dict[str, Any]] = []
    for router in ROUTERS:
        for route in router.routes:
            if not isinstance(route, APIRoute) or "GET" not in route.methods:
                continue
            role = by_endpoint.get(route.path)
            entry: dict[str, Any] = {
                "endpoint": route.path,
                "method": "GET",
                "params": _param_spec(route),
            }
            if role is None:
                entry["role"] = None
                entry["warning"] = (
                    f"no role declaration maps to {route.path}; this endpoint has no "
                    f"declared identity"
                )
            else:
                entry.update(role.public_view())
            entries.append(entry)

    entries.sort(key=lambda e: e["endpoint"])
    return {
        "shim_version": SHIM_VERSION,
        "least_privilege_model": (
            "One endpoint, one credential variable, and one principal behind it. "
            "At startup the shim compares the Postgres and MySQL user inside each "
            "DSN with the identity its role file declares, and refuses to start when "
            "they differ, when two endpoints resolve to the same principal without "
            "both declaring it under `shared_with`, or when a credential is absent. "
            "It never falls back to a broader credential. The S3 checks share one "
            "floci key and say so (`shared_with`): floci accepts any key, so S3 "
            "calls prove reachability locally, not an IAM identity. Each entry's "
            "`local_enforcement` states which grants the local backing store really "
            "enforces. Credential VALUES are never returned by this index, only "
            "variable names."
        ),
        "response_contract": {
            "200": "the check RAN; `ok` is the verdict and `state` is pass or fail",
            "422": "bad parameters; the check did not run",
            "501": "the check is not implemented here and is explicitly NOT verified",
            "503": "the check COULD NOT RUN (dependency down, credential rejected, or "
            "a value needed for the verdict was unavailable). Never a pass.",
        },
        "check_count": len(entries),
        "checks": entries,
        "meta_endpoints": [
            {"endpoint": "/health", "role": by_endpoint.get("/health", None) and "health"},
            {"endpoint": "/checks", "role": "index"},
        ],
    }


app.include_router(meta)
for _router in ROUTERS:
    app.include_router(_router)


def _preflight() -> None:
    try:
        registry = RoleRegistry.load()
        registry.validate_environment(REQUIRED_ROLES)
    except RoleConfigurationError as exc:
        sys.stderr.write(f"\n{exc}\n\n")
        raise SystemExit(78) from exc


def run() -> None:
    import uvicorn

    _preflight()
    uvicorn.run(
        "shim.main:app",
        host=os.environ.get("SHIM_HOST", "0.0.0.0"),
        port=int(os.environ.get("SHIM_PORT", "8096")),
        log_level=os.environ.get("SHIM_LOG_LEVEL", "info"),
    )


if __name__ == "__main__":
    run()
