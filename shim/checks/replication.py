from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Depends

from ..clients import connect_rest_get, mysql
from ..config import settings
from ..deps import role_dep
from ..result import CheckResult, CheckUnverifiable
from ..roles import Role

router = APIRouter(prefix="/checks/replication", tags=["replication"])

ROLES = {"replication_lag", "replication_connector_status"}

LAG_CHECK = "replication.source_capture_lag"
LAG_SCOPE = (
    "source capture lag, MySQL to Kafka only: the MySQL binlog write position against "
    "the position the Debezium SOURCE connector has committed. It does NOT cover the "
    "JDBC sink or staging; /checks/replication/connector-status covers the sink's "
    "state and /checks/ingestion/freshness covers end-to-end staleness."
)
STATUS_CHECK = "replication.connector_status"

# Kafka Connect flushes committed source offsets on a timer (60s by default), so
# the committed position trails real consumption between flushes. One sample
# that shows "behind" therefore cannot tell a stalled connector from a pending
# flush. The shim remembers when it first saw the committed position stuck at a
# given offset while the source was ahead, and only calls it a failure once it
# has WATCHED that position stay put for longer than the lag budget.
_behind_first_seen: dict[str, float] = {}


def _master_status(conn: Any) -> dict[str, Any]:
    with conn.cursor() as cur:
        for statement in ("SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"):
            try:
                cur.execute(statement)
            except Exception:
                continue
            row = cur.fetchone()
            if row:
                return {"statement": statement, **row}
    raise CheckUnverifiable(
        check=LAG_CHECK,
        role="replication_lag",
        dependency="mysql_source",
        reason=(
            "neither SHOW BINARY LOG STATUS nor SHOW MASTER STATUS returned a row; "
            "binary logging is probably disabled on the source"
        ),
    )


def _server_unix_time(conn: Any) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT UNIX_TIMESTAMP() AS ts")
        row = cur.fetchone()
    return float(row["ts"])


def _binlog_ordinal(filename: str) -> int | None:
    _, _, suffix = filename.rpartition(".")
    return int(suffix) if suffix.isdigit() else None


@router.get("/lag", response_model=None)
def replication_lag(role: Role = Depends(role_dep("replication_lag"))) -> CheckResult:
    threshold = settings.replication_lag_threshold_seconds

    status = connect_rest_get(
        role,
        LAG_CHECK,
        f"/connectors/{settings.debezium_connector}/status",
    )
    connector_state = str(status.get("connector", {}).get("state", "UNKNOWN"))

    with mysql(role, LAG_CHECK) as conn:
        master = _master_status(conn)
        server_now = _server_unix_time(conn)

    source_file = str(master.get("File", ""))
    source_pos = int(master.get("Position", 0) or 0)

    offsets = connect_rest_get(
        role, LAG_CHECK, f"/connectors/{settings.debezium_connector}/offsets"
    )
    consumed = _consumed_offset(offsets, status)

    measured: dict[str, Any] = {
        "scope": LAG_SCOPE,
        "end_to_end": False,
        "connector": settings.debezium_connector,
        "source_binlog_file": source_file,
        "source_binlog_position": source_pos,
        "source_server_unix_time": server_now,
        "connector_state": connector_state,
        "offsets_endpoint_entries": len(offsets.get("offsets", []) or []),
        "shim_observed_at_unix": time.time(),
    }

    running_tasks = [
        t for t in (status.get("tasks") or []) if t.get("state") == "RUNNING"
    ]
    measured["tasks_total"] = len(status.get("tasks") or [])
    measured["tasks_running"] = len(running_tasks)

    if connector_state == "RUNNING" and not running_tasks:
        return CheckResult.failed(
            check=LAG_CHECK,
            role=role.name,
            summary=(
                f"Source capture is NOT progressing: connector "
                f"'{settings.debezium_connector}' reports RUNNING but has "
                f"{measured['tasks_total']} task(s) and NONE are running, so binlog "
                f"position {source_file}:{source_pos} is not being consumed by anything. "
                f"A committed offset may still be readable from a previous run; it does "
                f"not mean lag is zero."
            ),
            measured=measured,
            threshold={
                "lag_seconds_max": threshold,
                "connector_state": "RUNNING",
                "tasks_running_min": 1,
            },
        )

    if connector_state != "RUNNING":
        return CheckResult.failed(
            check=LAG_CHECK,
            role=role.name,
            summary=(
                f"Source capture is NOT progressing: Debezium connector "
                f"'{settings.debezium_connector}' is in state {connector_state}, so "
                f"binlog position {source_file}:{source_pos} is not being consumed. "
                f"Lag is unbounded, not merely above the {threshold}s threshold."
            ),
            measured=measured,
            threshold={"lag_seconds_max": threshold, "connector_state": "RUNNING"},
        )

    if consumed is None:
        raise CheckUnverifiable(
            check=LAG_CHECK,
            role=role.name,
            dependency="kafka_connect",
            reason=(
                "connector reports RUNNING but exposes no committed source offset via "
                "the Connect status API, so consumed binlog position cannot be read. "
                "Lag is therefore UNKNOWN — not zero. Enable the Debezium JMX metrics "
                "sink or query the connect-offsets topic to make this measurable."
            ),
            detail_extra=measured,
        )

    measured["consumed_binlog_file"] = consumed["file"]
    measured["consumed_binlog_position"] = consumed["position"]
    measured["consumed_source_unix_time"] = consumed.get("source_unix_time")

    source_ordinal = _binlog_ordinal(source_file)
    consumed_ordinal = _binlog_ordinal(str(consumed["file"]))
    if source_ordinal is not None and consumed_ordinal is not None:
        measured["binlog_files_behind"] = source_ordinal - consumed_ordinal
        if source_ordinal == consumed_ordinal:
            measured["bytes_behind"] = source_pos - int(consumed["position"])

    if consumed.get("source_unix_time") is None:
        raise CheckUnverifiable(
            check=LAG_CHECK,
            role=role.name,
            dependency="kafka_connect",
            reason=(
                "committed binlog position is known but carries no source timestamp, so "
                "lag cannot be expressed in SECONDS. Byte/file distance is reported in "
                "measured values; the seconds threshold could not be evaluated."
            ),
            detail_extra=measured,
        )

    # The committed offset's timestamp is the time of the last CONSUMED event.
    # On an idle source that timestamp ages forever while nothing is waiting, so
    # "now minus last event" is only a lag when unconsumed binlog exists.
    caught_up = (
        measured.get("binlog_files_behind") == 0
        and measured.get("bytes_behind") is not None
        and measured["bytes_behind"] <= 0
    )
    measured["caught_up"] = caught_up
    measured["seconds_since_last_consumed_event"] = round(
        max(0.0, server_now - float(consumed["source_unix_time"])), 3
    )
    threshold_block = {
        "lag_seconds_max": threshold,
        "connector_state": "RUNNING",
        "tasks_running_min": 1,
    }
    if caught_up:
        _behind_first_seen.clear()
        lag_seconds = 0.0
        measured["lag_basis"] = (
            "committed binlog position equals the source write position, so no "
            "event is waiting"
        )
    else:
        upper_bound = measured["seconds_since_last_consumed_event"]
        stuck_key = f"{consumed['file']}:{consumed['position']}"
        now = time.time()
        if stuck_key not in _behind_first_seen:
            _behind_first_seen.clear()
            _behind_first_seen[stuck_key] = now
        watched = now - _behind_first_seen[stuck_key]
        measured["lag_upper_bound_seconds"] = upper_bound
        measured["committed_position_watched_stuck_seconds"] = round(watched, 3)

        if upper_bound <= threshold:
            lag_seconds = upper_bound
            measured["lag_basis"] = (
                "unconsumed binlog exists; lag is reported as the age of the last "
                "committed event, an upper bound on the age of the oldest waiting event"
            )
        elif watched > threshold:
            measured["lag_seconds"] = round(watched, 3)
            measured["lag_basis"] = (
                "observed: the committed position did not advance for this long "
                "while the source was ahead"
            )
            return CheckResult.failed(
                check=LAG_CHECK,
                role=role.name,
                summary=(
                    f"Source capture (MySQL to Kafka) is STALLED: the source is at "
                    f"{source_file}:{source_pos} "
                    f"but the committed position has stayed at {consumed['file']}:"
                    f"{consumed['position']} for {watched:.0f}s of observation, over "
                    f"the {threshold}s budget."
                ),
                measured=measured,
                threshold=threshold_block,
            )
        else:
            raise CheckUnverifiable(
                check=LAG_CHECK,
                role=role.name,
                dependency="kafka_connect",
                reason=(
                    f"the source is ahead of the committed position "
                    f"({source_file}:{source_pos} vs {consumed['file']}:"
                    f"{consumed['position']}) and the last committed event is "
                    f"{upper_bound:.0f}s old, but the shim has only watched that "
                    f"position for {watched:.0f}s. Kafka Connect commits offsets on a "
                    f"timer, so this is either a pending flush or a stall; lag can be "
                    f"proven neither within nor over the {threshold}s budget yet. "
                    f"Call again: it resolves to a pass once the offset commits, or to "
                    f"a fail once the position has been watched stuck for over "
                    f"{threshold}s."
                ),
                detail_extra=measured,
            )
    measured["lag_seconds"] = round(lag_seconds, 3)

    within = lag_seconds <= threshold
    return CheckResult.verdict(
        within,
        check=LAG_CHECK,
        role=role.name,
        summary=(
            f"Source capture lag (MySQL to Kafka only, NOT end-to-end) is "
            f"{lag_seconds:.1f}s against a {threshold}s threshold: "
            f"{'within' if within else 'OVER'} budget. Source at "
            f"{source_file}:{source_pos}, committed to {consumed['file']}:"
            f"{consumed['position']}. Nothing here shows that rows reach staging."
        ),
        measured=measured,
        threshold=threshold_block,
    )


def _consumed_offset(
    offsets: dict[str, Any], status: dict[str, Any]
) -> dict[str, Any] | None:
    """Connect 3.6+ publishes committed source offsets at /connectors/<n>/offsets.
    The /status payload carries none, so that path is only a fallback for forks that
    do embed them."""
    candidates: list[dict[str, Any]] = []
    for entry in offsets.get("offsets", []) or []:
        if isinstance(entry, dict) and isinstance(entry.get("offset"), dict):
            candidates.append(entry["offset"])
    for task in status.get("tasks", []) or []:
        embedded = task.get("source_offset") or task.get("offset")
        if isinstance(embedded, dict):
            candidates.append(embedded)

    for offset in candidates:
        file = offset.get("file") or offset.get("binlog_filename")
        pos = offset.get("pos") or offset.get("binlog_position")
        if file is None or pos is None:
            continue
        source_unix: float | None = None
        for field in ("ts_ms", "ts_sec"):
            raw_value = offset.get(field)
            if raw_value is None:
                continue
            raw = float(raw_value)
            source_unix = raw / 1000.0 if field == "ts_ms" else raw
            break
        return {"file": file, "position": int(pos), "source_unix_time": source_unix}
    return None


def _connector_report(role: Role, label: str, name: str) -> dict[str, Any]:
    status = connect_rest_get(role, STATUS_CHECK, f"/connectors/{name}/status")
    tasks = [
        {
            "id": t.get("id"),
            "state": t.get("state"),
            "worker_id": t.get("worker_id"),
            "trace": _first_line(t.get("trace")),
        }
        for t in (status.get("tasks") or [])
    ]
    state = str((status.get("connector") or {}).get("state", "UNKNOWN"))
    running = [t for t in tasks if t["state"] == "RUNNING"]
    failed = [t for t in tasks if t["state"] == "FAILED"]
    return {
        "role_in_pipeline": label,
        "connector": name,
        "connector_state": state,
        "connector_type": status.get("type"),
        "task_count": len(tasks),
        "tasks_running": len(running),
        "tasks_failed": len(failed),
        "tasks_not_running": len(tasks) - len(running),
        "tasks": tasks,
        "ok": state == "RUNNING" and bool(tasks) and len(running) == len(tasks),
    }


@router.get("/connector-status", response_model=None)
def connector_status(
    role: Role = Depends(role_dep("replication_connector_status")),
) -> CheckResult:
    required = (
        ("source", settings.debezium_connector),
        ("sink", settings.sink_connector),
    )
    missing_config = [label for label, name in required if not name.strip()]
    if missing_config:
        raise CheckUnverifiable(
            check=STATUS_CHECK,
            role=role.name,
            dependency="kafka_connect",
            reason=(
                f"no connector name is configured for: {', '.join(missing_config)}. "
                f"Replication needs both the source and the sink connector, so the "
                f"check cannot be evaluated with one of them unnamed."
            ),
        )

    connectors = {label: _connector_report(role, label, name) for label, name in required}
    unhealthy = [label for label, c in connectors.items() if not c["ok"]]
    ok = not unhealthy

    states = [c["connector_state"] for c in connectors.values()]
    measured = {
        "connectors_required": [name for _, name in required],
        "connectors_healthy": len(connectors) - len(unhealthy),
        "connectors_unhealthy": [connectors[label]["connector"] for label in unhealthy],
        "connector_state": next((s for s in states if s != "RUNNING"), "RUNNING"),
        "connector_state_basis": (
            "RUNNING only when every required connector is RUNNING; otherwise the "
            "first non-RUNNING state"
        ),
        "task_count": sum(c["task_count"] for c in connectors.values()),
        "tasks_running": sum(c["tasks_running"] for c in connectors.values()),
        "tasks_failed": sum(c["tasks_failed"] for c in connectors.values()),
        "tasks_not_running": sum(c["tasks_not_running"] for c in connectors.values()),
        "connectors": connectors,
    }
    threshold = {
        "connectors_required": [name for _, name in required],
        "connector_state": "RUNNING",
        "tasks_not_running_max": 0,
        "tasks_running_min_per_connector": 1,
    }

    def describe(c: dict[str, Any]) -> str:
        text = (
            f"{c['role_in_pipeline']} '{c['connector']}' {c['connector_state']} with "
            f"{c['tasks_running']}/{c['task_count']} tasks running"
        )
        traces = "; ".join(t["trace"] for t in c["tasks"] if t["trace"])
        return text + (f" (first error: {traces})" if traces else "")

    detail = "; ".join(describe(c) for c in connectors.values())
    if ok:
        summary = (
            f"All {len(connectors)} required replication connectors are RUNNING with "
            f"every task running: {detail}."
        )
    else:
        summary = (
            f"Replication is unhealthy: {len(unhealthy)} of {len(connectors)} required "
            f"connectors ({', '.join(unhealthy)}) not fully RUNNING, so rows are not "
            f"flowing from MySQL to staging. {detail}."
        )

    return CheckResult.verdict(
        ok,
        check=STATUS_CHECK,
        role=role.name,
        summary=summary,
        measured=measured,
        threshold=threshold,
    )


def _first_line(trace: Any) -> str:
    if not trace:
        return ""
    return str(trace).strip().splitlines()[0][:300]
