"""In-process tests for the stub's honesty rules, with both upstream probes
replaced by fixed readings. Run inside the image:

    docker exec nfl-dms-stub python selftest.py

Covers the states that cannot be induced on the live stack without breaking it
(FAILED connector, Kafka Connect down, Postgres down). Exit code 0 only if every
case passed.
"""

from __future__ import annotations

import logging
import sys

from fastapi.testclient import TestClient

import app as stub
from sources import ConnectState, SourceResult

TABLES = ["hawks.patient_account", "hawks.payment", "hawks.replication_heartbeat"]
COUNTS = {"patient_account": 5, "payment": 1, "replication_heartbeat": 9}


def connect(connector: str, tasks: list[str]) -> SourceResult:
    return SourceResult(
        ok=True,
        data=ConnectState(
            connector_state=connector,
            task_states=tasks,
            connector_name="c",
            tables_captured=TABLES,
            config_read=True,
            snapshot_mode="initial",
        ),
    )


logging.disable(logging.CRITICAL)

DOWN = SourceResult(ok=False, error="down")
PG_OK = SourceResult(ok=True, data=COUNTS)
client = TestClient(stub.app)
failures: list[str] = []


def use(connect_result: SourceResult, pg_result: SourceResult = PG_OK) -> None:
    stub.probe_connect = lambda: connect_result
    stub.probe_staging_counts = lambda: pg_result


def call(op: str, body: dict | None = None):
    return client.post("/", headers={"X-Amz-Target": f"AmazonDMSv20160101.{op}"}, json=body or {})


def check(name: str, got, want) -> None:
    ok = got == want
    print(f"{'ok  ' if ok else 'FAIL'} {name}: got {got!r}" + ("" if ok else f", want {want!r}"))
    if not ok:
        failures.append(name)


def marked(name: str, resp) -> None:
    check(f"{name} carries _stub and X-NFL-Stub", (resp.json().get("_stub"), resp.headers.get("x-nfl-stub")), (True, "true"))


def task_after(op: str | None, connect_result: SourceResult) -> dict:
    use(connect_result)
    if op == "StopReplicationTask":
        resp = call(op, {"ReplicationTaskArn": stub.TASK_ARN})
        return resp.json()["ReplicationTask"]
    if op == "StartReplicationTask":
        resp = call(op, {"ReplicationTaskArn": stub.TASK_ARN, "StartReplicationTaskType": "resume-processing"})
        return resp.json()["ReplicationTask"]
    return call("DescribeReplicationTasks").json()["ReplicationTasks"][0]


RUNNING = connect("RUNNING", ["RUNNING"])
FAILED_TASK = connect("RUNNING", ["FAILED"])
FAILED_CONNECTOR = connect("FAILED", [])
PAUSED = connect("PAUSED", ["PAUSED"])

check("running connector -> running", task_after(None, RUNNING)["Status"], "running")
check("Stop on a running connector still reads running", task_after("StopReplicationTask", RUNNING)["Status"], "running")
check("Describe after Stop still reads running", task_after(None, RUNNING)["Status"], "running")
check("Stop on a FAILED task reads failed", task_after("StopReplicationTask", FAILED_TASK)["Status"], "failed")
check("Start on a FAILED task reads failed", task_after("StartReplicationTask", FAILED_TASK)["Status"], "failed")
check("FAILED connector with no tasks reads failed", task_after(None, FAILED_CONNECTOR)["Status"], "failed")
check("paused connector reads stopped", task_after(None, PAUSED)["Status"], "stopped")
check("Start on a paused connector still reads stopped", task_after("StartReplicationTask", PAUSED)["Status"], "stopped")
check("paused connector with a FAILED task reads failed", task_after(None, connect("PAUSED", ["FAILED"]))["Status"], "failed")
check("RUNNING connector with zero tasks has no status", task_after(None, connect("RUNNING", []))["Status"], None)
check("UNASSIGNED connector has no status", task_after(None, connect("UNASSIGNED", ["UNASSIGNED"]))["Status"], None)
check("Kafka Connect down -> Status null", task_after(None, DOWN)["Status"], None)
check("Start with Kafka Connect down -> Status null", task_after("StartReplicationTask", DOWN)["Status"], None)

task = task_after("StopReplicationTask", RUNNING)
check("intent is recorded under _stub_intent", task["_stub_intent"]["last_command"], "StopReplicationTask")
check("Status provenance", task["_provenance"]["Status"], "derived")
check("StopReason is null/unknown after Stop", (task["StopReason"], task["_provenance"]["StopReason"]), (None, "unknown"))
stats = task["ReplicationTaskStats"]
for field in ("FullLoadProgressPercent", "TablesLoaded", "TablesLoading", "TablesQueued", "TablesErrored", "ElapsedTimeMillis", "StartDate"):
    check(f"{field} is null/unknown", (stats[field], stats["_provenance"][field]), (None, "unknown"))
check("FAILED task: TablesErrored is not 0", task_after(None, FAILED_TASK)["ReplicationTaskStats"]["TablesErrored"], None)
check("_stub_landed counts captured tables only", task["_stub_landed"]["captured_tables_with_rows"], 2)

use(RUNNING)
for op, body in (
    ("StopReplicationTask", {"ReplicationTaskArn": stub.TASK_ARN}),
    ("StartReplicationTask", {"ReplicationTaskArn": stub.TASK_ARN, "StartReplicationTaskType": "resume-processing"}),
):
    resp = call(op, body)
    check(f"{op} shape", (resp.status_code, isinstance(resp.json().get("ReplicationTask"), dict), "ReplicationTasks" in resp.json()), (200, True, False))
    marked(op, resp)

resp = call("DescribeTableStatistics", {"ReplicationTaskArn": stub.TASK_ARN})
rows = resp.json()["TableStatistics"]
check("table statistics list captured business tables, not the heartbeat", [r["TableName"] for r in rows], ["patient_account", "payment"])
check("no response text calls a table absent from Postgres", "absent from Postgres" in resp.text, False)
check("_stub_landed lists business tables only", task["_stub_landed"]["captured_tables"], ["patient_account", "payment"])
check("FullLoadRows is the Postgres count", [r["FullLoadRows"] for r in rows], [5, 1])
check("TableState null/unknown", [(r["TableState"], r["_provenance"]["TableState"]) for r in rows], [(None, "unknown")] * 2)
check("ValidationState null/unknown", [(r["ValidationState"], r["_provenance"]["ValidationState"]) for r in rows], [(None, "unknown")] * 2)

use(RUNNING, DOWN)
rows = call("DescribeTableStatistics", {"ReplicationTaskArn": stub.TASK_ARN}).json()["TableStatistics"]
check("Postgres down -> FullLoadRows null, not 0", [r["FullLoadRows"] for r in rows], [None, None])
use(DOWN)
body = call("DescribeTableStatistics", {"ReplicationTaskArn": stub.TASK_ARN}).json()
check("Kafka Connect down -> tables not enumerated", (body["TableStatistics"], body["_stub_tables_enumerated"]), ([], False))

use(RUNNING)
bogus = "arn:aws:dms:us-east-1:000000000000:task:NOT-THE-TASK"
for op, body in (
    ("DescribeTableStatistics", {"ReplicationTaskArn": bogus}),
    ("StopReplicationTask", {"ReplicationTaskArn": bogus}),
    ("StartReplicationTask", {"ReplicationTaskArn": bogus, "StartReplicationTaskType": "resume-processing"}),
    ("DescribeReplicationTasks", {"Filters": [{"Name": "replication-task-arn", "Values": [bogus]}]}),
    ("DescribeReplicationTasks", {"Filters": [{"Name": "replication-task-id", "Values": ["NOT-THE-TASK"]}]}),
    ("DescribeReplicationInstances", {"Filters": [{"Name": "replication-instance-arn", "Values": [bogus]}]}),
    ("DescribeEndpoints", {"Filters": [{"Name": "endpoint-arn", "Values": [bogus]}]}),
):
    resp = call(op, body)
    check(f"{op} foreign task {list(body)[0]}", (resp.status_code, resp.json().get("__type"), resp.headers.get("x-amzn-errortype")), (400, "ResourceNotFoundFault", "ResourceNotFoundFault"))
    marked(f"{op} fault", resp)
before = task_after(None, RUNNING)["_stub_intent"]["last_command_at"]
call("StopReplicationTask", {"ReplicationTaskArn": bogus})
check("foreign-ARN Stop records no intent", task_after(None, RUNNING)["_stub_intent"]["last_command_at"], before)
check("missing ARN", (call("StopReplicationTask").status_code, call("StopReplicationTask").json().get("__type")), (400, "ValidationException"))
check("bad start type", call("StartReplicationTask", {"ReplicationTaskArn": stub.TASK_ARN, "StartReplicationTaskType": "nope"}).json().get("__type"), "InvalidParameterValueException")
check("unsupported filter", call("DescribeReplicationTasks", {"Filters": [{"Name": "migration-type", "Values": ["cdc"]}]}).json().get("__type"), "InvalidParameterValueException")
check("matching filter", call("DescribeReplicationTasks", {"Filters": [{"Name": "replication-task-arn", "Values": [stub.TASK_ARN]}]}).status_code, 200)
check("unsupported instance filter", call("DescribeReplicationInstances", {"Filters": [{"Name": "endpoint-arn", "Values": [bogus]}]}).json().get("__type"), "InvalidParameterValueException")
check("unsupported endpoint filter", call("DescribeEndpoints", {"Filters": [{"Name": "replication-instance-arn", "Values": [bogus]}]}).json().get("__type"), "InvalidParameterValueException")
resp = call("DescribeReplicationInstances", {"Filters": [{"Name": "replication-instance-arn", "Values": [stub.INSTANCE_ARN]}]})
check("matching instance filter", (resp.status_code, [i["ReplicationInstanceArn"] for i in resp.json()["ReplicationInstances"]]), (200, [stub.INSTANCE_ARN]))
check("no instance filter", call("DescribeReplicationInstances").status_code, 200)
resp = call("DescribeEndpoints", {"Filters": [{"Name": "endpoint-arn", "Values": [stub.TARGET_ENDPOINT_ARN, bogus]}]})
check("matching endpoint filter returns that endpoint only", (resp.status_code, [e["EndpointArn"] for e in resp.json()["Endpoints"]]), (200, [stub.TARGET_ENDPOINT_ARN]))
resp = call("DescribeEndpoints")
check("no endpoint filter returns both", (resp.status_code, [e["EndpointArn"] for e in resp.json()["Endpoints"]]), (200, [stub.SOURCE_ENDPOINT_ARN, stub.TARGET_ENDPOINT_ARN]))

for label, c, p, want in (
    ("both up", RUNNING, PG_OK, (200, True, False)),
    ("Kafka Connect down", DOWN, PG_OK, (503, False, True)),
    ("Postgres down", RUNNING, DOWN, (503, False, True)),
    ("both down", DOWN, DOWN, (503, False, True)),
):
    use(c, p)
    resp = client.get("/health")
    check(f"/health {label}", (resp.status_code, resp.json()["healthy"], resp.json()["degraded"]), want)
    marked(f"/health {label}", resp)

use(DOWN, DOWN)
inst = call("DescribeReplicationInstances").json()["ReplicationInstances"][0]
check("instance status unknown when Connect down", inst["ReplicationInstanceStatus"], None)
check("instance class is not invented", inst["ReplicationInstanceClass"], None)
eps = call("DescribeEndpoints").json()["Endpoints"]
check("endpoint Status unknown when untested/unreadable", [e["Status"] for e in eps], [None, None])

for name, resp in (("unknown operation", call("Nope")), ("missing target", client.post("/", json={})), ("unrouted path", client.get("/nope"))):
    marked(name, resp)


def boom() -> SourceResult:
    raise RuntimeError("probe exploded")


stub.probe_connect = boom
resp = TestClient(stub.app, raise_server_exceptions=False).post("/", headers={"X-Amz-Target": "AmazonDMSv20160101.DescribeReplicationTasks"}, json={})
check("crash is a labelled 500", resp.status_code, 500)
marked("crash", resp)

print(f"\n{len(failures)} failure(s)")
sys.exit(1 if failures else 0)
