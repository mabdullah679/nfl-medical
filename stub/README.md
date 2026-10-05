# NFL DMS Control-Plane STUB

## This is not AWS DMS. It replicates nothing.

Not one row moves because of this service. The real MySQL -> Postgres CDC is
performed by **Debezium / Kafka Connect** on 8097.

This service exists for exactly one reason: Airflow and the Bruno collection
must issue **DMS-shaped** requests, so that the same artefacts run against real
AWS later by changing **one environment variable** and nothing else.

A stub that returned plausible-looking progress would be worse than no stub at
all — a green check would prove nothing while looking identical to a real pass.
So this one is built to be impossible to mistake for the real thing:

- Every response contains `"_stub": true` and a `_stub_warning`.
- Every response carries headers `X-NFL-Stub: true`,
  `X-NFL-Stub-Service: dms-control-plane`, and a `Warning: 199` header.
- Every field carries a **provenance tag** (`measured`, `derived`, `static`
  or `unknown`) in a `_provenance` map, with the exact rule or source in
  `_provenance_detail`.
- A DMS field with no local source is **`null` with provenance `unknown`**
  and a stated reason. No DMS-vocabulary value is emitted unless a stated rule
  produces it from a live reading.
- The startup log prints a banner naming itself a stub and reporting whether
  each upstream is reachable.

---

## Run it

It is the compose service `dms-stub` (container `nfl-dms-stub`) and starts with
the stack; `scripts/up.sh` waits for it and fails if either upstream is
unreachable from it. After changing stub code:

```bash
docker compose --project-directory infra -f infra/docker-compose.yml \
  --env-file infra/nfl.env up -d --build --no-deps dms-stub
```

Listens on **8098**. The compose service is the only supported way to run it.

`GET /health` returns **200 only when both upstreams answered on that request**
(Kafka Connect returned the configured connector's status, and the staging
schema was read from Postgres). Otherwise it returns **503** with
`healthy: false`, `degraded: true` and the upstream errors. The compose
healthcheck looks only at the status code, so a degraded stub shows as
`unhealthy` in `docker ps`.

The AWS wire protocol is reproduced exactly — `POST /` with an `X-Amz-Target`
header — so an unchanged AWS SDK call or Bruno request reaches it:

```bash
curl -X POST http://localhost:8098/ \
  -H 'X-Amz-Target: AmazonDMSv20160101.DescribeReplicationTasks' \
  -H 'Content-Type: application/x-amz-json-1.1' -d '{}'
```

---

## Field-by-field

### `DescribeReplicationTasks`

| Field | Provenance | Truth |
|---|---|---|
| `Status` | **derived** from the measured connector state | Translated, on every request, from the Debezium connector and task states Kafka Connect reports. Rules, in order: connector or any task `FAILED` -> `failed`; connector `RUNNING` with at least one task, all `RUNNING` -> `running`; connector `PAUSED` or `STOPPED` -> `stopped`; anything else, or Kafka Connect unreachable -> **`null` / unknown**. Start/Stop commands are not an input. The raw states are in `_stub_upstream.kafka_connect`. |
| `StopReason` | **unknown -> null** | Kafka Connect reports no stop reason and the stub infers none. |
| `MigrationType` | **derived** | From the connector's `snapshot.mode`: `initial` -> `full-load-and-cdc`, `initial_only` -> `full-load`, `never`/`no_data`/`schema_only` -> `cdc`; other modes or unreadable config -> `null`. |
| `FullLoadProgressPercent`, `TablesLoaded`, `TablesLoading`, `TablesQueued`, `TablesErrored` | **unknown -> null** | Debezium exposes no load progress or per-table error count over the Connect REST API, and row presence cannot tell a finished load from a partial one. |
| `ElapsedTimeMillis`, `StartDate`, `FreshStartDate`, `ReplicationTaskCreationDate` | **unknown -> null** | Kafka Connect reports none of them. |
| All `*Arn`, `ReplicationTaskIdentifier` | static | Configured constants in AWS shape. |
| `_stub_landed` (not a DMS field) | measured / derived | `captured_tables`: the connector's `table.include.list` without the heartbeat table `replication_heartbeat`. `captured_tables_with_rows`: how many of those hold at least one row in `staging`. Row presence only; not load progress. |
| `_stub_intent` (not a DMS field) | - | The last Start/Stop command received, its time and start type. No reported field is computed from it. |

`Filters` named `replication-task-arn` or `replication-task-id` are honoured; a
filter that does not match the configured task returns `ResourceNotFoundFault`.
Any other filter name returns `InvalidParameterValueException`.

### `DescribeTableStatistics`

Requires `ReplicationTaskArn`. Lists the tables in the connector's
`table.include.list` except `replication_heartbeat`: the heartbeat is not a
business table, and the stub's role `nfl_stub_reader` is not granted SELECT on
`staging.replication_heartbeat`, so the stub has nothing to report for it. Its
landing is judged by the shim's `/checks/ingestion/freshness`. If the list cannot be read, `TableStatistics` is empty
and `_stub_tables_enumerated` is `false`: the empty list then means "unknown",
not "no tables".

| Field | Provenance | Truth |
|---|---|---|
| `FullLoadRows` | **measured** | Exact `COUNT(*)` from Postgres: total rows present now, full-load and CDC together. DMS reports those separately; this stub cannot. `null` when Postgres is unreadable, or when the table is not among the `staging` tables the stub's role can see (it does not exist or the role has no privilege on it). |
| `TableName` | measured | From the connector config. |
| `TableState`, `ValidationState`, `ValidationPendingRecords`, `ValidationFailedRecords`, `FullLoadErrorRows`, `FullLoadCondtnlChkFailedRows` | **unknown -> null** | No per-table load state exists locally and no validation is performed. |
| `Inserts`, `Updates`, `Deletes`, `Ddls` | **unknown -> null** | Would need per-operation CDC counters; the Connect REST API does not expose them. |

### `DescribeReplicationInstances` / `DescribeEndpoints`

`DescribeReplicationInstances` honours a `Filters` entry named
`replication-instance-arn`, `DescribeEndpoints` one named `endpoint-arn`
(only the matching endpoints are returned). A filter whose values match
nothing returns `ResourceNotFoundFault` (HTTP 400); any other filter name
returns `InvalidParameterValueException`.

`ReplicationInstanceStatus` is `available` (derived) when Kafka Connect
answered, otherwise `null`. Class, storage, engine version, AZ, Multi-AZ and
public accessibility are `null`: nothing local corresponds to them. The target
endpoint's `Status` is `active` (derived) when the stub read the staging
schema, otherwise `null`. The source endpoint's `Status` is always `null`: the
stub holds no MySQL credential and does not test the source.

### `StartReplicationTask` / `StopReplicationTask`

Both require `ReplicationTaskArn`; Start also requires
`StartReplicationTaskType` (`start-replication`, `resume-processing` or
`reload-target`). They return the AWS shape, `{"ReplicationTask": {...}}`, a
single object.

They start and stop nothing. The command is recorded in `_stub_intent` and
described in `_stub_action`; `Status` in the response is the Debezium state
read at that moment. Stop against a running connector therefore answers
`running`, and Start against a paused or failed connector answers `stopped` or
`failed`. Real AWS DMS would answer `stopping` / `starting`; this is a known
difference, stated in `_stub_action`.

To actually start or stop replication, pause or resume the Debezium connector
through Kafka Connect on 8097.

### Errors

AWS shape: HTTP 400, body `{"__type": ..., "message": ...}`, header
`x-amzn-ErrorType`. They carry `_stub` and `X-NFL-Stub` like every response.

| Condition | `__type` | HTTP |
|---|---|---|
| `ReplicationTaskArn` (or a filter) names a task other than the configured one | `ResourceNotFoundFault` | 400 |
| `ReplicationTaskArn` or `StartReplicationTaskType` missing | `ValidationException` | 400 |
| Invalid start type, unsupported filter, missing `X-Amz-Target` | `InvalidParameterValueException` | 400 |
| Operation the stub does not implement | `UnknownOperationException` | 404 |
| The stub itself failed | `InternalFailure` | 500 |

---

## Degraded behaviour

When an upstream is unreachable, affected fields return **`null` with
provenance `unknown`** and the connection error verbatim. They are **never**
backfilled with zeros or plausible values.

This matters because **a zero and an unreachable source are different facts.**
`FullLoadRows: 0` would be a claim that the table is empty;
`FullLoadRows: null, provenance: unknown` is the truth that we could not look.
`GET /health` returns 503 with `degraded: true` whenever either upstream is down.

---

## Repointing at real AWS DMS

The only consumer is the Bruno collection, which addresses DMS through its environment variable **`dms_endpoint`** (`bruno/environments/local.bru` and `aws.bru`). No service reads an env var for this. The request bodies and
`X-Amz-Target` headers do not change.

| | Local | Real AWS |
|---|---|---|
| `dms_endpoint` (Bruno) | `http://localhost:8098` | `https://dms.<region>.amazonaws.com` |
| Credentials | ignored by the stub | real SigV4 credentials required |
| `_stub` / `X-NFL-Stub` | present | **absent** |

**Make this the assertion that protects you:** have Bruno assert on
`X-NFL-Stub`. In the local environment assert it is present; in the AWS
environment assert it is **absent**. That single assertion makes it impossible
to mistake a stub response for a real one, in either direction.

Caveats when you switch:
1. Real DMS returns genuine progress, table-state, validation and CDC counter
   values; the stub's `null`s become real values. Assertions must tolerate both
   or be environment-scoped. Real DMS also answers Start/Stop with `starting` /
   `stopping` and then changes state; the stub's `Status` never moves on a
   command.
2. Real DMS has no `_provenance` maps. Do not depend on them outside local runs.
3. Real DMS implements many operations this stub does not. A missing operation
   here is a **stub limitation, not an AWS one** — the 404 says so explicitly
   and lists what is supported. Of the `DescribeReplicationTasks` filters only
   `replication-task-arn` and `replication-task-id` are implemented.
4. SigV4 is unverified by the stub. Signing problems will surface only against
   real AWS.

---

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `NFL_CONNECT_URL` | `http://localhost:8097` | Kafka Connect / Debezium REST |
| `NFL_CONNECT_CONNECTOR` | `nfl-hawks-mysql-source` | Connector name to inspect |
| `NFL_PG_HOST` / `NFL_PG_PORT` | `localhost` / `5440` | Postgres target |
| `NFL_PG_DB` / `NFL_PG_USER` / `NFL_PG_PASSWORD` | `nfl` / `nfl_stub_reader` / — | Postgres credentials |
| `NFL_PG_STAGING_SCHEMA` | `staging` | Schema counted for landed rows |
| `NFL_DMS_REPLICATION_TASK_ARN` | — | Task ARN to answer for; set from `infra/nfl.env`. Wins over `NFL_DMS_TASK_ID` |
| `NFL_DMS_TASK_ID` / `NFL_DMS_INSTANCE_ID` | `NFL-HAWKS-STAGING` / `nfl-replication-instance` | Synthetic identities |
| `NFL_STUB_PROBE_TIMEOUT` | `2.0` | Upstream probe timeout (seconds) |

### Least privilege

Consistent with the project-wide rule (`docs/DECISIONS.md` D-004), the Postgres
user should be a **dedicated read-only role** — default name
`nfl_stub_reader` — holding `CONNECT`, `USAGE` on the staging schema, and
`SELECT` on its tables, and nothing more. The stub only ever counts rows:

The role is created by `scripts/up.sh` from
`infra/sql/06-check-identities.sql.tmpl` (password `PG_STUB_PASSWORD` in
`infra/nfl.env`):

```sql
CREATE ROLE nfl_stub_reader LOGIN PASSWORD '<secret>';
GRANT CONNECT ON DATABASE nfl TO nfl_stub_reader;
GRANT USAGE ON SCHEMA staging TO nfl_stub_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO nfl_stub_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging
  GRANT SELECT ON TABLES TO nfl_stub_reader;
```

In AWS this lifts to a task role whose only data permission is read access to
the replication target. The stub needs no write access anywhere, and no S3,
Secrets Manager, or KMS access at all.

The full declaration is **`stub/role.yaml`**, written in the same format as
`shim/roles/*.yaml` so every identity in the project is declared uniformly. It
names the identity, its grants, the rationale, and the equivalent AWS IAM
policy. Note that the IAM policy deliberately omits
`dms:StartReplicationTask` / `dms:StopReplicationTask`: the stub implements
both, but they only record the command, and granting their real equivalents to a
monitoring caller would let a read surface halt production replication.

---

## Tests

`selftest.py` ships in the image and drives the app in-process with both
upstream probes replaced by fixed readings, so it covers states that cannot be
induced on the live stack without breaking it (FAILED connector, Kafka Connect
down, Postgres down):

```bash
docker exec nfl-dms-stub python selftest.py   # exit 0 only if every case passed
```

## Verification performed (2026-10-04, live compose stack, uncommitted tree)

- `selftest.py` in the running container: 66 cases, 0 failures. Ten mutations
  of `app.py` (FAILED guard off, intent overriding the measurement, Stop
  returning a list, ARN guard off, filter guard off, `/health` always 200,
  `healthy` hard-coded, `TablesErrored` 0, an invented `TableState`, crash
  handler off) each turned it red; the unmutated file was green again after.
- Live, connector RUNNING: `Status: running` (derived). After
  `StopReplicationTask`: response key `ReplicationTask` (object), `Status`
  still `running`, `StopReason: null`, command visible in `_stub_intent`.
- Live, source connector paused through Kafka Connect: `Status: stopped`,
  raw state `PAUSED`; `StartReplicationTask` while paused still answered
  `stopped`. Connector resumed and confirmed `RUNNING` / task `RUNNING`, stub
  back to `running`.
- Live: `DescribeTableStatistics`, `StartReplicationTask`,
  `StopReplicationTask` with a foreign ARN and `DescribeReplicationTasks` with
  a non-matching filter: HTTP 400 `ResourceNotFoundFault`, with `_stub: true`
  and `X-NFL-Stub: true`.
- Live: `FullLoadRows` 203 / 122 / 156 for `account_transaction` /
  `patient_account` / `payment` (measured); `TableState`, `ValidationState`,
  `TablesErrored`, `FullLoadProgressPercent`, `TablesLoaded` all `null` /
  `unknown`.
- Degraded `/health` over real HTTP: the image run in a throwaway container
  with no network returned 503, `healthy: false`, `degraded: true`, and the
  compose healthcheck command exited 1. On the live stack: 200, container
  `healthy`.

### Fixed 2026-10-05 (live compose stack, uncommitted tree)
- `DescribeTableStatistics` used to list a fourth table, `replication_heartbeat`, with
  `FullLoadRows: null` and the false reason "captured by Debezium but absent from
  Postgres" (the table exists in `staging`; `nfl_stub_reader` has no grant on it, by
  design). The heartbeat is now left out of the table statistics and of
  `_stub_landed.captured_tables`. Live: three rows, 203 / 122 / 152, no
  `replication_heartbeat` row, the text "absent from Postgres" nowhere in the response.
- `DescribeReplicationInstances` and `DescribeEndpoints` used to ignore `Filters` and
  answer HTTP 200 for an ARN that does not exist. Live before: 200 and 200. Live after:
  HTTP 400 `ResourceNotFoundFault` for an unknown `replication-instance-arn` /
  `endpoint-arn`; 200 for a matching value and for no filter.
- `selftest.py` in the rebuilt container: 78 cases, 0 failures. Three mutations of
  `app.py` on a scratch copy (instance filter guard off, endpoint guard off, heartbeat
  exclusion off) each turned it red.

### Not established
- A FAILED connector and an unreachable Kafka Connect or Postgres were not
  induced on the live stack; those paths are proven by `selftest.py` with
  substituted probe results, and `/health` 503 by the isolated container.
- The error `__type` names and messages follow the AWS DMS API reference from
  memory; they were not compared against a response from real AWS DMS.
- SigV4 signature verification is not implemented and not tested; the stub
  ignores credentials.
- No AWS SDK client was pointed at it; requests were made with `curl` and Bruno.
