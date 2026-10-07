from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import sql

from ..clients import postgres
from ..config import settings
from ..deps import role_dep
from ..params import interval_bounds, qualified_table
from ..result import CheckResult, CheckState
from ..roles import Role

router = APIRouter(prefix="/checks", tags=["observability"])

ROLES = {"events"}

EVENTS_CHECK = "events"

Status = Literal["started", "success", "retry", "failed", "skipped"]
Stage = Literal["replication", "ingestion", "extraction", "reporting", "delivery", "platform"]

ALERT = sql.SQL("COALESCE(metrics->>'alert', 'false') = 'true'")


def _bad_request(reason: str) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={
            "ok": False,
            "state": CheckState.UNVERIFIABLE.value,
            "check": EVENTS_CHECK,
            "reason": reason,
            "summary": f"Check did NOT run: {reason}",
        },
    )


@router.get("/events", response_model=None)
def events(
    interval_start: str | None = Query(
        None,
        description=(
            "Interval start as YYYYMMDD or YYYY-MM-DD. Required with interval_end "
            "unless unscoped=true."
        ),
        examples=["20260801"],
    ),
    interval_end: str | None = Query(
        None,
        description="Interval end (last day covered) as YYYYMMDD or YYYY-MM-DD.",
        examples=["20260831"],
    ),
    unscoped: bool = Query(
        False,
        description=(
            "true lists events recorded with NO interval (NULL bounds), which is "
            "where an alert lands when the run's conf yielded no valid interval. "
            "Cannot be combined with interval_start/interval_end."
        ),
    ),
    status: Status | None = Query(None, description="Only events with this status."),
    stage: Stage | None = Query(None, description="Only events of this stage."),
    alerts_only: bool = Query(
        False, description="Only alert rows (status failed with metrics.alert = true)."
    ),
    limit: int = Query(
        50, ge=1, description="Rows returned, newest first. Capped by the shim."
    ),
    role: Role = Depends(role_dep("events")),
) -> CheckResult:
    have_bounds = interval_start is not None or interval_end is not None
    if unscoped and have_bounds:
        raise _bad_request(
            "unscoped=true lists events that have no interval; it cannot be combined "
            "with interval_start/interval_end"
        )
    if not unscoped and (interval_start is None or interval_end is None):
        raise _bad_request(
            "give both interval_start and interval_end, or unscoped=true for events "
            "recorded without an interval"
        )

    cap = settings.events_row_cap
    row_limit = min(limit, cap)
    table = qualified_table(settings.events_table)

    clauses: list[sql.Composable] = []
    args: list[Any] = []
    params: dict[str, Any] = {
        "unscoped": unscoped,
        "status": status,
        "stage": stage,
        "alerts_only": alerts_only,
        "limit": row_limit,
    }
    if unscoped:
        clauses.append(sql.SQL("interval_start IS NULL AND interval_end IS NULL"))
        scope = "no interval (NULL bounds)"
    else:
        s, e, s_date, e_date = interval_bounds(interval_start or "", interval_end or "")
        clauses.append(sql.SQL("interval_start = %s AND interval_end = %s"))
        args += [s_date, e_date]
        params.update(interval_start=s, interval_end=e)
        scope = f"exactly {s}..{e}"
    if status is not None:
        clauses.append(sql.SQL("status = %s"))
        args.append(status)
    if stage is not None:
        clauses.append(sql.SQL("stage = %s"))
        args.append(stage)
    if alerts_only:
        clauses.append(sql.SQL("status = 'failed' AND ") + ALERT)
    where = sql.SQL(" AND ").join(clauses)

    count_stmt = sql.SQL(
        "SELECT stage, status, COUNT(*), COUNT(*) FILTER (WHERE status = 'failed' AND {alert}) "
        "FROM {table} WHERE {where} GROUP BY stage, status ORDER BY stage, status"
    ).format(table=table, where=where, alert=ALERT)
    rows_stmt = sql.SQL(
        "SELECT event_id, stage, step, status, interval_start, interval_end, "
        "       event_time, dag_id, run_id, message, metrics, correlation_id "
        "FROM {table} WHERE {where} ORDER BY event_time DESC, event_id DESC LIMIT %s"
    ).format(table=table, where=where)

    with postgres(role, EVENTS_CHECK) as conn:
        with conn.cursor() as cur:
            cur.execute(count_stmt, args)
            grouped = cur.fetchall()
            cur.execute(rows_stmt, [*args, row_limit])
            fetched = cur.fetchall()

    by_status: dict[str, int] = {}
    by_stage: dict[str, int] = {}
    total = 0
    alerts = 0
    for g_stage, g_status, n, n_alerts in grouped:
        total += int(n)
        alerts += int(n_alerts)
        by_status[str(g_status)] = by_status.get(str(g_status), 0) + int(n)
        by_stage[str(g_stage)] = by_stage.get(str(g_stage), 0) + int(n)

    out: list[dict[str, Any]] = []
    for (
        event_id,
        e_stage,
        step,
        e_status,
        i_start,
        i_end,
        event_time,
        dag_id,
        run_id,
        message,
        metrics,
        correlation,
    ) in fetched:
        metrics = metrics if isinstance(metrics, dict) else {}
        is_alert = e_status == "failed" and metrics.get("alert") is True
        out.append(
            {
                "event_id": int(event_id),
                "stage": e_stage,
                "step": step,
                "status": e_status,
                "interval_start": str(i_start) if i_start else None,
                "interval_end": str(i_end) if i_end else None,
                "event_time": event_time.isoformat() if event_time else None,
                "dag_id": dag_id,
                "run_id": run_id,
                "correlation_id": correlation,
                "message": message,
                "alert": is_alert,
                "severity": metrics.get("severity"),
                "error_summary": metrics.get("error_summary"),
                "log_url": metrics.get("log_url"),
                "log_api_path": metrics.get("log_api_path"),
                "metrics": metrics,
            }
        )

    measured = {
        "events_table": settings.events_table,
        "scope": scope,
        "counts": {
            "matched": total,
            "alerts": alerts,
            "by_status": by_status,
            "by_stage": by_stage,
            "returned": len(out),
        },
        "truncated": total > len(out),
        "order": "event_time descending (newest first)",
        "events": out,
    }
    filters = ", ".join(
        f"{k}={v}"
        for k, v in (("status", status), ("stage", stage))
        if v is not None
    )
    if alerts_only:
        filters = (filters + ", " if filters else "") + "alerts_only=true"
    described = f"for {scope}" + (f" with {filters}" if filters else "")
    if total:
        summary = (
            f"{total} event(s) recorded {described}: "
            + ", ".join(f"{n} {k}" for k, n in sorted(by_status.items()))
            + f"; {alerts} alert(s). {len(out)} returned, newest first. `ok` here "
            f"means only that matching events exist; /checks/lineage gives the "
            f"pipeline verdict."
        )
    else:
        summary = f"NO event is recorded {described}."

    return CheckResult.verdict(
        total > 0,
        check=EVENTS_CHECK,
        role=role.name,
        summary=summary,
        measured=measured,
        threshold={"matched_min": 1, "row_cap": cap},
        params=params,
    )
