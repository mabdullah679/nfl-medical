"""NFL DMS control-plane STUB.

THIS IS NOT AWS DMS. It never replicates a single row. Debezium does the real
MySQL -> Postgres CDC; this service only answers DMS-shaped control-plane
questions so that the Bruno collection can be written once against the AWS DMS
request shape and later repointed at real AWS by switching the Bruno
environment (variable dms_endpoint).

Honesty contract:
  - Every response carries "_stub": true and an X-NFL-Stub: true header.
  - Every field carries a provenance tag (measured / derived / static / unknown).
  - Status is a translation of the Debezium connector and task states read from
    Kafka Connect at request time, and of nothing else. Start/Stop commands are
    recorded under _stub_intent and never change any reported field.
  - A DMS field with no local source (progress, per-table state, validation,
    stop reason, elapsed time, sizing) is null with provenance "unknown".

The AWS wire protocol is awsjson1.1: POST / with an X-Amz-Target header naming
AmazonDMSv20160101.<Operation>. Errors use the AWS shape: HTTP 400, "__type"
and "message" in the body, x-amzn-ErrorType header.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from provenance import PROVENANCE_MEANING, derived, measured, render, static, unknown
from sources import (
    CONNECT_URL,
    PG_DB,
    PG_HOST,
    PG_PORT,
    PG_STAGING_SCHEMA,
    probe_connect,
    probe_staging_counts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("nfl.stub")

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
ACCOUNT = os.environ.get("NFL_STUB_ACCOUNT_ID", "000000000000")
INSTANCE_ID = os.environ.get("NFL_DMS_INSTANCE_ID", "nfl-replication-instance")

# The ARN the rest of the stack is configured with (infra/nfl.env) wins, so the
# stub answers for the same task Airflow asks about.
_CONFIGURED_TASK_ARN = os.environ.get("NFL_DMS_REPLICATION_TASK_ARN", "").strip()
if _CONFIGURED_TASK_ARN:
    TASK_ARN = _CONFIGURED_TASK_ARN
    TASK_ID = _CONFIGURED_TASK_ARN.rsplit(":", 1)[-1]
else:
    TASK_ID = os.environ.get("NFL_DMS_TASK_ID", "NFL-HAWKS-STAGING")
    TASK_ARN = f"arn:aws:dms:{REGION}:{ACCOUNT}:task:{TASK_ID}"
INSTANCE_ARN = f"arn:aws:dms:{REGION}:{ACCOUNT}:rep:{INSTANCE_ID}"
SOURCE_ENDPOINT_ARN = f"arn:aws:dms:{REGION}:{ACCOUNT}:endpoint:nfl-source-mysql"
TARGET_ENDPOINT_ARN = f"arn:aws:dms:{REGION}:{ACCOUNT}:endpoint:nfl-target-postgres"

STUB_BANNER = r"""
===============================================================================
  NFL DMS CONTROL-PLANE  *** STUB ***   NOT AWS DMS. REPLICATES NOTHING.
-------------------------------------------------------------------------------
  Real replication is performed by Debezium / Kafka Connect.
  This service answers control-plane shape only, so the same requests can be
  repointed at real AWS DMS by switching the Bruno environment (dms_endpoint).

  Status is DERIVED from Kafka Connect state : {connect}
  Row counts are MEASURED from Postgres      : {pg}
  Start/Stop commands start and stop nothing and never change Status; the
  last one received is reported as _stub_intent only.
  DMS fields with no local source are null + "unknown".

  Every response: "_stub": true  and header  X-NFL-Stub: true
===============================================================================
"""

HEARTBEAT_TABLE = os.environ.get("NFL_STUB_HEARTBEAT_TABLE", "replication_heartbeat")

VALID_START_TYPES = ("start-replication", "resume-processing", "reload-target")


class DmsFault(Exception):
    def __init__(self, error_type: str, message: str) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.message = message


class ControlIntent:
    """Last Start/Stop command received. Reported as _stub_intent; read by
    nothing that computes a DMS field."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.last_command: str | None = None
        self.last_command_at: str | None = None
        self.last_start_type: str | None = None

    def record(self, command: str, start_type: str | None = None) -> None:
        with self._lock:
            self.last_command = command
            self.last_command_at = _now()
            if start_type:
                self.last_start_type = start_type

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "last_command": self.last_command,
                "last_command_at": self.last_command_at,
                "last_start_type": self.last_start_type,
                "effect": (
                    "none: this stub starts and stops nothing, and no reported "
                    "field is computed from this record"
                ),
            }


INTENT = ControlIntent()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stub_envelope(body: dict[str, Any]) -> dict[str, Any]:
    body["_stub"] = True
    body["_stub_warning"] = (
        "SYNTHETIC CONTROL PLANE. This is not AWS DMS and performs no "
        "replication. Real CDC is performed by Debezium. Fields tagged "
        "'unknown' could not be established and are null by design."
    )
    body["_stub_provenance_legend"] = PROVENANCE_MEANING
    body["_stub_generated_at"] = _now()
    return body


def _task_status(probe: Any) -> Any:
    """Translate the Connect reading into DMS status vocabulary. FAILED is
    tested first so no other state can mask it."""
    if not probe.ok:
        return unknown(f"Cannot determine replication status: {probe.error}")

    state = probe.data
    seen = (
        f"Kafka Connect reports connector '{state.connector_name}' "
        f"state={state.connector_state}, task states={state.task_states}."
    )
    if state.any_failed:
        return derived("failed", f"{seen} Rule: connector or any task FAILED -> 'failed'.")
    if state.connector_state == "RUNNING" and state.all_tasks_running:
        return derived(
            "running",
            f"{seen} Rule: connector RUNNING and at least one task, all RUNNING -> 'running'.",
        )
    if state.connector_state in ("PAUSED", "STOPPED"):
        return derived("stopped", f"{seen} Rule: connector PAUSED or STOPPED -> 'stopped'.")
    return unknown(
        f"{seen} No rule maps this combination to a DMS status, so none is reported."
    )


def _migration_type(probe: Any) -> Any:
    if not probe.ok:
        return unknown(f"Connector config unreadable: {probe.error}")
    state = probe.data
    if not state.config_read:
        return unknown("Kafka Connect did not return the connector config.")
    mode = state.snapshot_mode or "initial"
    how = (
        f"Debezium snapshot.mode={state.snapshot_mode!r}"
        + ("" if state.snapshot_mode else " (unset, Debezium default 'initial')")
    )
    mapping = {
        "initial": "full-load-and-cdc",
        "initial_only": "full-load",
        "never": "cdc",
        "no_data": "cdc",
        "schema_only": "cdc",
    }
    if mode in mapping:
        return derived(
            mapping[mode],
            f"{how}. Rule: initial -> full-load-and-cdc, initial_only -> full-load, "
            f"never/no_data/schema_only -> cdc.",
        )
    return unknown(f"{how} has no DMS migration-type equivalent.")


def _captured_tables(probe: Any) -> list[str]:
    if probe.ok and probe.data.tables_captured:
        names = (t.split(".")[-1] for t in probe.data.tables_captured)
        return sorted(n for n in names if n != HEARTBEAT_TABLE)
    return []


CAPTURED_SOURCE = (
    f"Debezium connector config table.include.list, schema prefix removed, "
    f"without the heartbeat table '{HEARTBEAT_TABLE}' (not a business table; "
    f"this stub's Postgres role is not granted SELECT on it)."
)


def _upstream(connect_probe: Any, pg: Any) -> dict[str, Any]:
    state = connect_probe.data if connect_probe.ok else None
    return {
        "kafka_connect": {
            "url": CONNECT_URL,
            "reachable": connect_probe.ok,
            "error": connect_probe.error or None,
            "connector_state": state.connector_state if state else None,
            "task_states": state.task_states if state else None,
        },
        "postgres": {
            "dsn": f"{PG_HOST}:{PG_PORT}/{PG_DB}",
            "schema": PG_STAGING_SCHEMA,
            "reachable": pg.ok,
            "error": pg.error or None,
        },
    }


def _landed(connect_probe: Any, pg: Any) -> dict[str, Any]:
    """Row presence per captured table, under a stub-only key. It is not DMS
    load progress and is deliberately not placed in a DMS field."""
    captured = _captured_tables(connect_probe)
    if not captured:
        why = "The Debezium table.include.list could not be read, so the captured table set is unknown."
        return {"captured_tables": unknown(why), "captured_tables_with_rows": unknown(why)}
    tables = measured(captured, CAPTURED_SOURCE)
    if not pg.ok:
        return {
            "captured_tables": tables,
            "captured_tables_with_rows": unknown(f"Postgres unreadable: {pg.error}"),
        }
    return {
        "captured_tables": tables,
        "captured_tables_with_rows": derived(
            sum(1 for name in captured if pg.data.get(name, 0) > 0),
            f"Number of captured tables whose COUNT(*) in Postgres schema "
            f"'{PG_STAGING_SCHEMA}' is at least 1. Says nothing about whether a load finished.",
        ),
    }


def _build_task() -> dict[str, Any]:
    connect_probe = probe_connect()
    pg = probe_staging_counts()
    no_progress = (
        "Debezium exposes no full-load progress through the Kafka Connect REST API, "
        "and row presence in Postgres cannot tell a finished load from a partial one."
    )
    task = {
        "ReplicationTaskIdentifier": static(TASK_ID, "Configured stub identity."),
        "ReplicationTaskArn": static(TASK_ARN, "Configured ARN in AWS DMS shape."),
        "SourceEndpointArn": static(SOURCE_ENDPOINT_ARN, "Synthetic ARN."),
        "TargetEndpointArn": static(TARGET_ENDPOINT_ARN, "Synthetic ARN."),
        "ReplicationInstanceArn": static(INSTANCE_ARN, "Synthetic ARN."),
        "MigrationType": _migration_type(connect_probe),
        "Status": _task_status(connect_probe),
        "StopReason": unknown(
            "Kafka Connect reports no stop reason and this stub does not infer one. "
            "A Stop command received by the stub stopped nothing, so it is not a reason either."
        ),
        "ReplicationTaskStats": {
            "FullLoadProgressPercent": unknown(no_progress),
            "TablesLoaded": unknown(no_progress),
            "TablesLoading": unknown(no_progress),
            "TablesQueued": unknown("Debezium has no table queue concept."),
            "TablesErrored": unknown(
                "Debezium reports failure per task, not per table. No per-table error "
                "count exists to read; see Status for task failure."
            ),
            "ElapsedTimeMillis": unknown("Kafka Connect does not report task elapsed time."),
            "FreshStartDate": unknown("Not tracked by Kafka Connect."),
            "StartDate": unknown("Kafka Connect does not report when the connector started."),
        },
        "ReplicationTaskCreationDate": unknown("Kafka Connect does not report a creation date."),
        "_stub_landed": _landed(connect_probe, pg),
        "_stub_upstream": _upstream(connect_probe, pg),
        "_stub_intent": INTENT.snapshot(),
    }
    return render(task)


def _require_task_arn(body: dict[str, Any]) -> None:
    arn = body.get("ReplicationTaskArn")
    if arn is None:
        raise DmsFault(
            "ValidationException",
            "1 validation error detected: Value null at 'replicationTaskArn' failed to "
            "satisfy constraint: Member must not be null",
        )
    if arn != TASK_ARN:
        raise DmsFault("ResourceNotFoundFault", f"Replication task not found: {arn}")


def _filter_values(body: dict[str, Any], supported: tuple[str, ...]) -> list[tuple[str, list]]:
    filters = body.get("Filters") or []
    if not isinstance(filters, list):
        raise DmsFault("InvalidParameterValueException", "Filters must be a list.")
    out = []
    for item in filters:
        name = item.get("Name") if isinstance(item, dict) else None
        values = item.get("Values") if isinstance(item, dict) else None
        if name not in supported or not isinstance(values, list):
            raise DmsFault(
                "InvalidParameterValueException",
                f"Unsupported filter {name!r}. This STUB supports only: "
                f"{', '.join(sorted(supported))}, each with a Values list.",
            )
        out.append((name, values))
    return out


def _matches_filters(body: dict[str, Any], known: dict[str, str]) -> bool:
    return all(known[name] in values for name, values in _filter_values(body, tuple(known)))


def op_describe_replication_tasks(body: dict[str, Any]) -> dict[str, Any]:
    if not _matches_filters(body, {"replication-task-arn": TASK_ARN, "replication-task-id": TASK_ID}):
        raise DmsFault("ResourceNotFoundFault", "No replication task matches the given filters.")
    return _stub_envelope({"ReplicationTasks": [_build_task()]})


def op_describe_table_statistics(body: dict[str, Any]) -> dict[str, Any]:
    _require_task_arn(body)
    connect_probe = probe_connect()
    pg = probe_staging_counts()

    names = _captured_tables(connect_probe)
    if not names:
        return _stub_envelope(
            {
                "ReplicationTaskArn": TASK_ARN,
                "TableStatistics": [],
                "_stub_tables_enumerated": False,
                "_stub_note": (
                    "The task's tables could not be enumerated, so the empty list means "
                    "'unknown', not 'no tables'. "
                    + (
                        connect_probe.error
                        or "The Debezium connector config returned no table.include.list."
                    )
                ),
            }
        )

    no_counters = (
        "Needs per-operation CDC counters. The Kafka Connect REST API does not expose "
        "them and this stub reads neither JMX metrics nor the topics."
    )
    no_validation = "This stub performs no source-vs-target validation."
    stats = []
    for name in names:
        landed = pg.data.get(name) if pg.ok else None
        if landed is not None:
            full_load_rows = measured(
                landed,
                f'Exact COUNT(*) of "{PG_STAGING_SCHEMA}"."{name}" in Postgres: total rows '
                f"present now, full-load and CDC rows together. DMS reports those "
                f"separately; this stub cannot.",
            )
        else:
            full_load_rows = unknown(
                pg.error
                or f"Table '{name}' is captured by Debezium but is not among the tables "
                f"of schema '{PG_STAGING_SCHEMA}' this stub's Postgres role can see: it "
                f"does not exist there or the role has no privilege on it."
            )
        stats.append(
            {
                "SchemaName": static(PG_STAGING_SCHEMA, "Configured target schema."),
                "TableName": measured(name, CAPTURED_SOURCE),
                "Inserts": unknown(no_counters),
                "Updates": unknown(no_counters),
                "Deletes": unknown(no_counters),
                "Ddls": unknown(no_counters),
                "FullLoadRows": full_load_rows,
                "FullLoadCondtnlChkFailedRows": unknown("No local equivalent."),
                "FullLoadErrorRows": unknown("No local equivalent."),
                "TableState": unknown(
                    "Neither Kafka Connect nor Postgres reports a per-table load state. "
                    "Row presence is in FullLoadRows and is not a load state."
                ),
                "ValidationState": unknown(no_validation),
                "ValidationPendingRecords": unknown(no_validation),
                "ValidationFailedRecords": unknown(no_validation),
            }
        )

    return _stub_envelope(
        {
            "ReplicationTaskArn": TASK_ARN,
            "TableStatistics": render(stats),
            "_stub_tables_enumerated": True,
        }
    )


def op_describe_replication_instances(body: dict[str, Any]) -> dict[str, Any]:
    if not _matches_filters(body, {"replication-instance-arn": INSTANCE_ARN}):
        raise DmsFault(
            "ResourceNotFoundFault", "No replication instance matches the given filters."
        )
    probe = probe_connect()
    no_compute = "No local equivalent: Debezium runs in a container with unrelated sizing."
    instance = {
        "ReplicationInstanceIdentifier": static(INSTANCE_ID, "Configured stub identity."),
        "ReplicationInstanceArn": static(INSTANCE_ARN, "Synthetic ARN."),
        "ReplicationInstanceClass": unknown(no_compute),
        "ReplicationInstanceStatus": derived(
            "available",
            f"Kafka Connect at {CONNECT_URL} answered its connector status endpoint. "
            f"Rule: answered -> 'available'.",
        )
        if probe.ok
        else unknown(f"Kafka Connect did not answer: {probe.error}"),
        "AllocatedStorage": unknown("No local equivalent to provisioned DMS storage."),
        "EngineVersion": unknown("This stub implements no DMS engine."),
        "PubliclyAccessible": unknown("No local equivalent."),
        "MultiAZ": unknown("No local equivalent."),
        "AvailabilityZone": unknown("No local equivalent."),
    }
    return _stub_envelope({"ReplicationInstances": [render(instance)]})


def op_describe_endpoints(body: dict[str, Any]) -> dict[str, Any]:
    wanted = {SOURCE_ENDPOINT_ARN, TARGET_ENDPOINT_ARN}
    for _, values in _filter_values(body, ("endpoint-arn",)):
        wanted &= {v for v in values if isinstance(v, str)}
    if not wanted:
        raise DmsFault("ResourceNotFoundFault", "No endpoint matches the given filters.")
    pg = probe_staging_counts()
    source = {
        "EndpointIdentifier": static("nfl-source-mysql", "Configured stub identity."),
        "EndpointArn": static(SOURCE_ENDPOINT_ARN, "Synthetic ARN."),
        "EndpointType": static("SOURCE", "Configured."),
        "EngineName": static("mysql", "Configured: the source this stack replicates from."),
        "Status": unknown(
            "This stub holds no MySQL credential and does not test the source; "
            "use the shim's replication checks for a verdict."
        ),
        "ServerName": static(
            os.environ.get("NFL_MYSQL_HOST", "localhost"), "Configured source host."
        ),
        "Port": static(int(os.environ.get("NFL_MYSQL_PORT", "3307")), "Configured source port."),
        "SslMode": unknown("Not configured or verified by this stub."),
    }
    target = {
        "EndpointIdentifier": static("nfl-target-postgres", "Configured stub identity."),
        "EndpointArn": static(TARGET_ENDPOINT_ARN, "Synthetic ARN."),
        "EndpointType": static("TARGET", "Configured."),
        "EngineName": static("postgres", "Configured: the target this stack replicates to."),
        "Status": derived(
            "active",
            f"This stub connected to Postgres and read schema '{PG_STAGING_SCHEMA}'. "
            f"Rule: readable -> 'active'.",
        )
        if pg.ok
        else unknown(f"Postgres unreadable: {pg.error}"),
        "ServerName": static(PG_HOST, "Configured target host."),
        "Port": static(PG_PORT, "Configured target port."),
        "DatabaseName": static(PG_DB, "Configured target database."),
        "SslMode": unknown("Not configured or verified by this stub."),
    }
    endpoints = [e for e in (source, target) if e["EndpointArn"].value in wanted]
    return _stub_envelope({"Endpoints": render(endpoints)})


def op_start_replication_task(body: dict[str, Any]) -> dict[str, Any]:
    _require_task_arn(body)
    start_type = body.get("StartReplicationTaskType")
    if start_type is None:
        raise DmsFault(
            "ValidationException",
            "1 validation error detected: Value null at 'startReplicationTaskType' failed "
            "to satisfy constraint: Member must not be null",
        )
    if start_type not in VALID_START_TYPES:
        raise DmsFault(
            "InvalidParameterValueException",
            f"Invalid StartReplicationTaskType {start_type!r}. Valid values: "
            f"{', '.join(VALID_START_TYPES)}.",
        )
    INTENT.record("StartReplicationTask", start_type)
    log.warning(
        "StartReplicationTask received (type=%s). STUB RECORDED INTENT ONLY - nothing was started.",
        start_type,
    )
    return _stub_envelope(
        {
            "ReplicationTask": _build_task(),
            "_stub_action": (
                "Recorded under _stub_intent. NOTHING WAS STARTED. Status is the "
                "Debezium state read just now and is unaffected by this command. Real "
                "AWS DMS would answer 'starting'."
            ),
        }
    )


def op_stop_replication_task(body: dict[str, Any]) -> dict[str, Any]:
    _require_task_arn(body)
    INTENT.record("StopReplicationTask")
    log.warning("StopReplicationTask received. STUB RECORDED INTENT ONLY - nothing was stopped.")
    return _stub_envelope(
        {
            "ReplicationTask": _build_task(),
            "_stub_action": (
                "Recorded under _stub_intent. NOTHING WAS STOPPED. Status is the "
                "Debezium state read just now and is unaffected by this command. Real "
                "AWS DMS would answer 'stopping'. To halt replication, pause the "
                "Debezium connector through Kafka Connect."
            ),
        }
    )


OPERATIONS = {
    "DescribeReplicationTasks": op_describe_replication_tasks,
    "DescribeTableStatistics": op_describe_table_statistics,
    "DescribeReplicationInstances": op_describe_replication_instances,
    "DescribeEndpoints": op_describe_endpoints,
    "StartReplicationTask": op_start_replication_task,
    "StopReplicationTask": op_stop_replication_task,
}

app = FastAPI(
    title="NFL DMS Control-Plane STUB",
    description="NOT AWS DMS. Synthetic control plane for local pipeline testing.",
    version="1.0.0",
)


@app.middleware("http")
async def stamp_stub_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-NFL-Stub"] = "true"
    response.headers["X-NFL-Stub-Service"] = "dms-control-plane"
    response.headers["Warning"] = '199 nfl-dms-stub "Synthetic DMS control plane; not AWS DMS"'
    return response


@app.get("/health")
async def health() -> JSONResponse:
    connect = probe_connect()
    pg = probe_staging_counts()
    healthy = connect.ok and pg.ok
    return JSONResponse(
        status_code=200 if healthy else 503,
        content=_stub_envelope(
            {
                "service": "nfl-dms-control-plane-stub",
                "healthy": healthy,
                "degraded": not healthy,
                "healthy_meaning": (
                    "true iff, on this request, Kafka Connect returned the status of "
                    "the configured connector AND the staging schema was read from "
                    "Postgres. Otherwise HTTP 503."
                ),
                "upstreams": _upstream(connect, pg),
            }
        ),
    )


def _fault_response(
    error_type: str, message: str, status_code: int = 400, **extra: Any
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=_stub_envelope({"__type": error_type, "message": message, **extra}),
        headers={"x-amzn-ErrorType": error_type},
    )


@app.exception_handler(StarletteHTTPException)
async def http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    return _fault_response("StubHttpError", str(exc.detail), exc.status_code)


@app.post("/")
async def dms_endpoint(request: Request) -> JSONResponse:
    target = request.headers.get("x-amz-target", "")
    operation = target.split(".")[-1] if "." in target else ""

    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - DMS accepts an empty body as {}
        body = {}
    if not isinstance(body, dict):
        body = {}

    if not operation:
        return _fault_response(
            "InvalidParameterValueException",
            "Missing X-Amz-Target header. Expected 'AmazonDMSv20160101.<Operation>'.",
        )

    handler = OPERATIONS.get(operation)
    if handler is None:
        log.info("Unsupported operation requested: %s", operation)
        return _fault_response(
            "UnknownOperationException",
            f"Unknown operation: {target}. This STUB implements only: "
            f"{', '.join(sorted(OPERATIONS))}. Real AWS DMS implements many more; an "
            f"operation missing here is a stub limitation, not an AWS one.",
            404,
            _stub_supported_operations=sorted(OPERATIONS),
        )

    log.info("STUB serving %s", operation)
    try:
        return JSONResponse(content=handler(body))
    except DmsFault as fault:
        log.info("STUB %s -> %s: %s", operation, fault.error_type, fault.message)
        return _fault_response(fault.error_type, fault.message)
    except Exception:  # noqa: BLE001 - a crash must still be labelled as the stub's
        log.exception("STUB %s crashed", operation)
        return _fault_response(
            "InternalFailure", f"The stub failed while serving {operation}; see its log.", 500
        )


@app.on_event("startup")
async def announce() -> None:
    connect = probe_connect()
    pg = probe_staging_counts()
    banner = STUB_BANNER.format(
        connect="REACHABLE" if connect.ok else f"UNREACHABLE ({connect.error})",
        pg="REACHABLE" if pg.ok else f"UNREACHABLE ({pg.error})",
    )
    for line in banner.strip().splitlines():
        log.warning(line)
