from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import sql

from ..clients import postgres
from ..config import settings
from ..deps import role_dep
from ..params import CORRELATION_RE, qualified_table
from ..result import CheckResult, CheckState
from ..roles import Role
from .lineage import PIPELINE_STAGES

router = APIRouter(prefix="/checks", tags=["observability"])

ROLES = {"trace", "alert_delivery", "monitor"}

TRACE_CHECK = "trace"
DELIVERY_CHECK = "alert_delivery"
MONITOR_CHECK = "monitor"

MONITOR_PROBES = ("replication_heartbeat", "connector_status", "orchestrator")


def _bad_request(check: str, reason: str) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={
            "ok": False,
            "state": CheckState.UNVERIFIABLE.value,
            "check": check,
            "reason": reason,
            "summary": f"Check did NOT run: {reason}",
        },
    )


def _correlation(check: str, value: str) -> str:
    if not CORRELATION_RE.match(value):
        raise _bad_request(
            check,
            f"correlation_id={value!r} must be 8 to 96 letters, digits or underscores",
        )
    return value


def _delivery_state(sent: int, attempts: int, age_seconds: float, grace: int) -> str:
    if sent:
        return "sent"
    if attempts >= settings.alert_max_attempts:
        return "abandoned"
    return "pending" if age_seconds <= grace else "undelivered"


@router.get("/trace", response_model=None)
def trace(
    correlation_id: str = Query(
        ...,
        description=(
            "The id one pipeline attempt carries through every stage: the "
            "conf.correlation_id given to the triggers, nfl_<start>_<end> when none "
            "was given, or the incident id of a watcher alert."
        ),
        examples=["nfl_20260801_20260831"],
    ),
    role: Role = Depends(role_dep("trace")),
) -> CheckResult:
    cid = _correlation(TRACE_CHECK, correlation_id)
    events_table = qualified_table(settings.events_table)
    delivery_table = qualified_table(settings.alert_delivery_table)
    grace = settings.alert_delivery_grace_seconds

    stmt = sql.SQL(
        "SELECT e.event_id, e.stage, e.step, e.status, e.interval_start, e.interval_end, "
        "       e.event_time, e.dag_id, e.run_id, e.message, e.metrics, "
        "       EXTRACT(EPOCH FROM now() - e.event_time), "
        "       (SELECT count(*) FROM {delivery} d WHERE d.event_id = e.event_id), "
        "       (SELECT count(*) FROM {delivery} d WHERE d.event_id = e.event_id "
        "          AND d.status = 'sent'), "
        "       (SELECT max(d.attempted_at) FROM {delivery} d WHERE d.event_id = e.event_id "
        "          AND d.status = 'sent') "
        "FROM {events} e WHERE e.correlation_id = %s "
        "ORDER BY e.event_time, e.event_id LIMIT %s"
    ).format(events=events_table, delivery=delivery_table)
    count_stmt = sql.SQL("SELECT count(*) FROM {events} WHERE correlation_id = %s").format(
        events=events_table
    )

    cap = settings.events_row_cap
    with postgres(role, TRACE_CHECK) as conn:
        with conn.cursor() as cur:
            cur.execute(count_stmt, [cid])
            total = int(cur.fetchone()[0])
            cur.execute(stmt, [cid, cap])
            fetched = cur.fetchall()

    events: list[dict[str, Any]] = []
    latest_terminal: dict[str, str] = {}
    runs: dict[str, list[str]] = {}
    undelivered: list[int] = []
    for (
        event_id, stage, step, status, i_start, i_end, event_time, dag_id, run_id,
        message, metrics, age, attempts, sent, sent_at,
    ) in fetched:
        metrics = metrics if isinstance(metrics, dict) else {}
        notify = metrics.get("notify")
        delivery = None
        if notify:
            delivery = {
                "channel": notify,
                "state": _delivery_state(int(sent), int(attempts), float(age), grace),
                "attempts": int(attempts),
                "sent_at": sent_at.isoformat() if sent_at else None,
            }
            if delivery["state"] in {"undelivered", "abandoned"}:
                undelivered.append(int(event_id))
        if status in {"success", "failed"}:
            latest_terminal[str(stage)] = str(status)
        if dag_id and run_id and run_id not in runs.setdefault(str(dag_id), []):
            runs[str(dag_id)].append(str(run_id))
        events.append(
            {
                "event_id": int(event_id),
                "event_time": event_time.isoformat(),
                "stage": stage,
                "step": step,
                "status": status,
                "dag_id": dag_id,
                "run_id": run_id,
                "interval_start": str(i_start) if i_start else None,
                "interval_end": str(i_end) if i_end else None,
                "message": message,
                "alert": status == "failed" and metrics.get("alert") is True,
                "severity": metrics.get("severity"),
                "error_summary": metrics.get("error_summary"),
                "log_url": metrics.get("log_url"),
                "log_api_path": metrics.get("log_api_path"),
                "log_group": metrics.get("log_group"),
                "delivery": delivery,
            }
        )

    stages_seen = [s for s in PIPELINE_STAGES if s in {e["stage"] for e in events}]
    stages_missing = [s for s in PIPELINE_STAGES if s not in stages_seen]
    stages_failed = [s for s in PIPELINE_STAGES if latest_terminal.get(s) == "failed"]
    stages_unfinished = [s for s in stages_seen if s not in latest_terminal]
    truncated = total > len(events)
    complete = (
        bool(events)
        and not truncated
        and not stages_missing
        and not stages_failed
        and not stages_unfinished
        and not undelivered
    )

    measured = {
        "correlation_id": cid,
        "counts": {
            "matched": total,
            "returned": len(events),
            "alerts": sum(1 for e in events if e["alert"]),
            "notifications": sum(1 for e in events if e["delivery"]),
        },
        "truncated": truncated,
        "order": "event_time ascending (oldest first)",
        "stages_with_events": stages_seen,
        "stages_missing_events": stages_missing,
        "stages_failed": stages_failed,
        "stages_unfinished": stages_unfinished,
        "latest_terminal_status": latest_terminal,
        "runs_by_dag": runs,
        "undelivered_alert_event_ids": undelivered,
        "log_filter_pattern": cid,
        "events": events,
    }
    if not events:
        summary = f"NO event carries correlation_id {cid}."
    elif complete:
        summary = (
            f"Thread {cid}: {total} event(s) across all five stages, each stage's latest "
            f"outcome a success, every notification delivered."
        )
    else:
        problems = []
        if truncated:
            problems.append(f"only {len(events)} of {total} events returned")
        if stages_missing:
            problems.append("no event for " + ", ".join(stages_missing))
        if stages_failed:
            problems.append("latest outcome failed for " + ", ".join(stages_failed))
        if stages_unfinished:
            problems.append("no terminal event for " + ", ".join(stages_unfinished))
        if undelivered:
            problems.append(f"{len(undelivered)} notification(s) not delivered")
        summary = f"Thread {cid}: {total} event(s); " + "; ".join(problems) + "."

    return CheckResult.verdict(
        complete,
        check=TRACE_CHECK,
        role=role.name,
        summary=summary,
        measured=measured,
        threshold={
            "stages_required": list(PIPELINE_STAGES),
            "delivery_grace_seconds": grace,
            "row_cap": cap,
        },
        params={"correlation_id": cid},
    )


@router.get("/alerts/delivery", response_model=None)
def alert_delivery(
    since_minutes: int = Query(
        60, ge=1, le=1440, description="Look at events recorded in the last N minutes."
    ),
    run_id: str | None = Query(None, description="Only events of this run id."),
    correlation_id: str | None = Query(None, description="Only events of this thread."),
    role: Role = Depends(role_dep("alert_delivery")),
) -> CheckResult:
    events_table = qualified_table(settings.events_table)
    delivery_table = qualified_table(settings.alert_delivery_table)
    grace = settings.alert_delivery_grace_seconds
    cap = settings.events_row_cap

    clauses: list[sql.Composable] = [
        sql.SQL("e.metrics ? 'notify'"),
        sql.SQL("e.event_time > now() - make_interval(mins => %s)"),
    ]
    args: list[Any] = [since_minutes]
    params: dict[str, Any] = {"since_minutes": since_minutes}
    if run_id is not None:
        clauses.append(sql.SQL("e.run_id = %s"))
        args.append(run_id)
        params["run_id"] = run_id
    if correlation_id is not None:
        clauses.append(sql.SQL("e.correlation_id = %s"))
        args.append(_correlation(DELIVERY_CHECK, correlation_id))
        params["correlation_id"] = correlation_id
    filtered = run_id is not None or correlation_id is not None

    stmt = sql.SQL(
        "SELECT e.event_id, e.stage, e.step, e.status, e.event_time, e.run_id, "
        "       e.correlation_id, e.metrics, EXTRACT(EPOCH FROM now() - e.event_time), "
        "       d.attempts, d.sent, d.sent_at, d.recipients, d.message_id, d.last_detail "
        "FROM {events} e LEFT JOIN LATERAL ("
        "  SELECT count(*) AS attempts, "
        "         count(*) FILTER (WHERE status = 'sent') AS sent, "
        "         max(attempted_at) FILTER (WHERE status = 'sent') AS sent_at, "
        "         max(recipients) FILTER (WHERE status = 'sent') AS recipients, "
        "         max(message_id) FILTER (WHERE status = 'sent') AS message_id, "
        "         (array_agg(detail ORDER BY attempt DESC))[1] AS last_detail "
        "  FROM {delivery} x WHERE x.event_id = e.event_id) d ON true "
        "WHERE {where} ORDER BY e.event_time DESC, e.event_id DESC LIMIT %s"
    ).format(
        events=events_table, delivery=delivery_table, where=sql.SQL(" AND ").join(clauses)
    )
    count_stmt = sql.SQL("SELECT count(*) FROM {events} e WHERE {where}").format(
        events=events_table, where=sql.SQL(" AND ").join(clauses)
    )

    with postgres(role, DELIVERY_CHECK) as conn:
        with conn.cursor() as cur:
            cur.execute(count_stmt, args)
            total = int(cur.fetchone()[0])
            cur.execute(stmt, [*args, cap])
            fetched = cur.fetchall()

    rows: list[dict[str, Any]] = []
    by_state: dict[str, int] = {}
    for (
        event_id, stage, step, status, event_time, e_run, e_corr, metrics, age,
        attempts, sent, sent_at, recipients, message_id, last_detail,
    ) in fetched:
        metrics = metrics if isinstance(metrics, dict) else {}
        state = _delivery_state(int(sent or 0), int(attempts or 0), float(age), grace)
        by_state[state] = by_state.get(state, 0) + 1
        rows.append(
            {
                "event_id": int(event_id),
                "event_time": event_time.isoformat(),
                "stage": stage,
                "step": step,
                "status": status,
                "run_id": e_run,
                "correlation_id": e_corr,
                "severity": metrics.get("severity"),
                "channel": metrics.get("notify"),
                "delivery_state": state,
                "attempts": int(attempts or 0),
                "sent_at": sent_at.isoformat() if sent_at else None,
                "seconds_to_delivery": (
                    round((sent_at - event_time).total_seconds(), 1) if sent_at else None
                ),
                "recipients": recipients,
                "message_id": message_id,
                "last_attempt_detail": last_detail,
            }
        )

    truncated = total > len(rows)
    bad = by_state.get("undelivered", 0) + by_state.get("abandoned", 0)
    pending = by_state.get("pending", 0)
    passing = not truncated and bad == 0 and pending == 0 and (total > 0 or not filtered)
    if total == 0:
        summary = (
            "NO notifiable event matches the filter in the last "
            f"{since_minutes} minute(s)."
            if filtered
            else f"No notifiable event in the last {since_minutes} minute(s); nothing to deliver."
        )
    else:
        summary = (
            f"{total} notifiable event(s) in the last {since_minutes} minute(s): "
            + ", ".join(f"{n} {k}" for k, n in sorted(by_state.items()))
            + (f"; only {len(rows)} returned" if truncated else "")
            + f". Pending means younger than the {grace}s grace period."
        )

    return CheckResult.verdict(
        passing,
        check=DELIVERY_CHECK,
        role=role.name,
        summary=summary,
        measured={
            "counts": {"matched": total, "returned": len(rows), "by_state": by_state},
            "truncated": truncated,
            "order": "event_time descending (newest first)",
            "events": rows,
        },
        threshold={
            "undelivered_max": 0,
            "pending_max": 0,
            "matched_min": 1 if filtered else 0,
            "delivery_grace_seconds": grace,
            "max_attempts": settings.alert_max_attempts,
            "row_cap": cap,
        },
        params=params,
    )


@router.get("/monitor", response_model=None)
def monitor(role: Role = Depends(role_dep("monitor"))) -> CheckResult:
    table = qualified_table(settings.monitor_table)
    max_age = settings.monitor_max_sample_age_seconds

    stmt = sql.SQL(
        "SELECT DISTINCT ON (probe) probe, sampled_at, healthy, value, threshold, detail, "
        "       EXTRACT(EPOCH FROM now() - sampled_at) "
        "FROM {table} ORDER BY probe, sampled_at DESC, sample_id DESC"
    ).format(table=table)
    rate_stmt = sql.SQL(
        "SELECT probe, count(*) FROM {table} "
        "WHERE sampled_at > now() - interval '5 minutes' GROUP BY probe"
    ).format(table=table)

    with postgres(role, MONITOR_CHECK) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt)
            latest = cur.fetchall()
            cur.execute(rate_stmt)
            recent = {str(p): int(n) for p, n in cur.fetchall()}

    probes: dict[str, Any] = {}
    for probe, sampled_at, healthy, value, threshold, detail, age in latest:
        detail = detail if isinstance(detail, dict) else {}
        probes[str(probe)] = {
            "sampled_at": sampled_at.isoformat(),
            "sample_age_seconds": round(float(age), 1),
            "fresh": float(age) <= max_age,
            "level": detail.get("level"),
            "probe_ran": healthy is not None,
            "healthy": healthy,
            "value": value,
            "threshold": threshold,
            "summary": detail.get("summary"),
            "incident": detail.get("incident"),
            "samples_last_5_minutes": recent.get(str(probe), 0),
        }

    missing = [p for p in MONITOR_PROBES if p not in probes]
    stale = [p for p in MONITOR_PROBES if p in probes and not probes[p]["fresh"]]
    not_ok = [
        p for p in MONITOR_PROBES if p in probes and probes[p]["fresh"] and probes[p]["level"] != "ok"
    ]
    watcher_alive = not missing and not stale
    passing = watcher_alive and not not_ok

    if missing or stale:
        summary = (
            "The watcher is NOT sampling: "
            + "; ".join(
                part
                for part in (
                    f"no sample ever for {', '.join(missing)}" if missing else "",
                    f"newest sample older than {max_age}s for {', '.join(stale)}"
                    if stale
                    else "",
                )
                if part
            )
            + ". Nothing is watching these on its own."
        )
    elif not_ok:
        summary = "The watcher is sampling and reports: " + "; ".join(
            f"{p} is {probes[p]['level']} ({probes[p]['summary']})" for p in not_ok
        )
    else:
        summary = (
            f"The watcher sampled all {len(MONITOR_PROBES)} probes within the last "
            f"{max_age}s and each is ok."
        )

    return CheckResult.verdict(
        passing,
        check=MONITOR_CHECK,
        role=role.name,
        summary=summary,
        measured={
            "watcher_alive": watcher_alive,
            "probes_missing": missing,
            "probes_stale": stale,
            "probes_not_ok": not_ok,
            "probes": probes,
        },
        threshold={"max_sample_age_seconds": max_age, "probes_required": list(MONITOR_PROBES)},
    )
