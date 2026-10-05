# Decision Ledger — NFL Data Platform local test harness

Every entry records context, options considered, the decision, and the
consequence we accepted. Evidence is quoted verbatim where it was obtained by
live probing.

D-001 to D-006 date from the first build. Where a later entry changed one of
them, the earlier entry says so and points to it. D-007 onward were added on
2026-10-04 after the fix round, from the running stack.

---

## D-001 — floci (local AWS emulator) does not implement DMS

**Date:** 2026-10-04
**Status:** Settled, proven by live probe

### Context
The production pipeline's first stage is AWS DMS replicating Hawks's AWS
MySQL into the PostgreSQL `staging` schema. The local emulator in use (floci,
LocalStack-compatible API on `http://localhost:4566`, always-free edition) was
assumed to cover this.

### Evidence
Live request against the running emulator, AWS JSON 1.1 protocol:

```
POST http://localhost:4566/
X-Amz-Target: AmazonDMSv20160101.DescribeReplicationTasks

HTTP 404
{"__type":"UnknownOperationException",
 "message":"Unknown operation: AmazonDMSv20160101.DescribeReplicationTasks"}
```

The emulator's own health endpoint lists ~100 running services. `dms` is not
among them. Services it reports as running include `s3`, `monitoring`
(CloudWatch), `logs`, `secretsmanager`, `kms` and `iam` (re-read 2026-10-04:
100 services listed, `dms` absent). Of these the harness uses **`s3` only**.
The first build's list also named `glue`, `athena`, `elasticmapreduce`, `mwaa`,
`transfer`, `kafka`, `kinesis` and `states`; those were not re-checked.

### Options considered
1. Enable a DMS plugin in floci — no such service exists in the emulator.
2. Pay for LocalStack Pro — see D-002.
3. Replace DMS locally with a real CDC engine — see D-003.

### Decision
Accept that DMS cannot run locally. Do not spend further time attempting to
emulate the DMS data plane.

### Consequence
Stage 1 (Replication) cannot be tested as DMS. Everything downstream of the
`staging` schema is unaffected, because those stages only ever read Postgres —
they never call DMS. The untestable surface is therefore narrow and precisely
bounded: DMS's own configuration, IAM, and failure semantics.

---

## D-002 — Paying for LocalStack would not have helped

**Date:** 2026-10-04
**Status:** Settled

### Context
Before accepting D-001, the obvious question is whether money solves it.

### Evidence
LocalStack's free edition has no DMS at all. The paid (Pro) edition's DMS
support targets **Kinesis** as the replication target. It does not implement
MySQL-source-to-PostgreSQL-target replication, which is exactly and only the
shape this pipeline uses.

> **Flagged as inherited, not re-measured this session:** this is a statement
> about LocalStack Pro's documented capability, not a live probe — we hold no
> Pro licence to test against. The free-edition absence *is* directly proven by
> D-001's probe. If a Pro licence is ever obtained, re-verify before relying on
> this entry.

### Options considered
1. Buy LocalStack Pro and use its DMS — does not support the Postgres target,
   so it would not replicate a single row for us.
2. Do not buy.

### Decision
Do not purchase. The spend would not produce a working MySQL -> Postgres
replication path, so it buys nothing this project needs.

### Consequence
Zero licence cost. The local harness stays on the always-free emulator.

---

## D-003 — Debezium performs the real CDC; the DMS control plane is stubbed behind an env var

**Date:** 2026-10-04
**Status:** Settled, implemented

### Context
Two distinct needs were being conflated:
- **Data plane** — rows must actually move MySQL -> Postgres, or no downstream
  stage has anything to read, and the whole harness tests nothing.
- **Control plane** — Airflow and the Bruno collection must issue DMS-shaped
  requests (`DescribeReplicationTasks`, `DescribeTableStatistics`) so the same
  artefacts work against real AWS later.

Nothing requires these to be solved by the same component.

### Options considered
1. **Skip stage 1 entirely.** Hand-load the staging schema with fixtures.
   Rejected: downstream gates would then be tested against data that never
   passed through a replication process, and replication-shaped failure modes
   (lag, partial load, a stalled task) could not be injected at all.
2. **Mock the whole thing in Bruno.** Rejected: proves only that the mock
   returns what the mock was told to return.
3. **Real CDC engine + stubbed control plane.** Chosen.

### Decision
- **Debezium / Kafka Connect** (REST on 8097, broker on 9099) performs genuine
  MySQL binlog CDC into Postgres. This is real replication with real lag, real
  snapshot behaviour, and real failure modes.
- **`stub/`** serves a DMS-shaped control plane on **8098**, speaking the AWS
  JSON 1.1 wire protocol (`POST /` + `X-Amz-Target`).
- The Bruno collection addresses DMS through its environment variable
  `dms_endpoint`: `http://localhost:8098` in `local`, the real DMS endpoint in
  `aws`. **No request body changes.**
- As built (2026-10-04): the Bruno collection is the stub's only consumer. No
  Airflow task calls DMS or the stub. `nfl_ingestion.pre_check` reads the
  Kafka Connect connector states directly.

### The honesty constraint attached to this decision
A stub that returns plausible-looking progress is worse than no stub, because a
green check would then prove nothing while looking identical to a real pass.
The implementation is therefore bound by rules enforced in code, not convention:
- every response carries `"_stub": true` and an `X-NFL-Stub: true` header;
- every field carries a provenance tag — `measured`, `derived`, `static`, or
  `unknown`;
- numbers are read from live Debezium and live Postgres where a truthful
  equivalent exists;
- where no equivalent exists the field is **`null` with provenance `unknown`**
  and a stated reason. Nothing is invented to look real;
- `StartReplicationTask` / `StopReplicationTask` record *intent only* and say
  so. `Status` is **derived from the measured Debezium connector state and
  nothing else**: RUNNING with every task RUNNING reads `running`, PAUSED or
  STOPPED reads `stopped`, any FAILED reads `failed`, unreachable reads `null`.
  A Stop while the connector runs still reads `running`; the command appears
  only in `_stub_intent`. (Corrected 2026-10-04. The first build let a Stop
  command set `Status: stopped` while Debezium was running. Bruno
  `90-chaos` scenario E now asserts the corrected behaviour.)

### Consequence
Row movement is genuinely exercised. The DMS request *shape* is exercised. What
remains unproven is DMS itself — its configuration, IAM, table mappings, and
failure vocabulary. `docs/COVERAGE.md` records this against each REP
requirement rather than letting it hide.

---

## D-004 — An HTTP shim with per-endpoint roles, not a custom GUI

**Date:** 2026-10-04
**Status:** Settled

### Context
Several pipeline gates are not HTTP by nature: "Is Failed" after ingestion, the
"Is UTM" check with retries, the S3 `_SUCCESS` marker pre-check, report
checksum verification, SFTP delivery verification. Bruno can only assert
against HTTP.

### Options considered
1. **A custom GUI / test runner.** Rejected: builds a bespoke artefact that
   has to be maintained, and nothing in it transports to production.
2. **Shell scripts invoked outside Bruno.** Rejected: splits the test evidence
   across two places and loses a single collection run as the record.
3. **One HTTP shim exposing each check as an endpoint.** Chosen.

### Decision
A shim on **8096** exposes every non-HTTP check as its own HTTP endpoint,
subject to a hard constraint from the owner:

> "whatever decision that can be transported over to enterprise scale
> infrastructure on least privilege roles"

Therefore **each endpoint is backed by its own separately-declared
credential/role** — never one service holding broad god credentials. Each
endpoint declares the exact permissions it requires, so it lifts directly to an
AWS task role or IAM role with precisely that scope.

What the shim is, as built: 12 read-only checks under `/checks/*`
(replication lag and connector status, ingestion reconcile, quality and
freshness, extraction marker, manifest and Parquet row count, reporting file,
delivery, lineage, events). The pipeline's **gates are not shim endpoints**:
"Is Failed", "Is UTM", the marker pre-check and "Is Process Failed" are
Airflow tasks (D-009). The shim re-measures the same facts from outside. How
the per-endpoint identities are realised is D-010.

### Consequence
More credentials to declare and wire than a single-service design. Accepted
deliberately: the per-endpoint declaration *is* the deliverable that transports
to enterprise infrastructure. A shim with one broad credential would test the
pipeline while teaching the wrong deployment shape.

---

## D-005 — An isolated NFL stack on dedicated ports; the paused avapuck stack is untouched

**Date:** 2026-10-04
**Status:** Settled. Port binding tightened by D-011.

### Context
A separate project (avapuck) has a **paused** Docker stack on this machine:
`pl-airflow:8085`, `pl-postgres:5433`, `pl-spark-master:7077/8080`,
`pl-kafka:29092`, `pl-hiveserver2:10000`, `pl-hive-metastore:9083`,
`pl-kafka-ui:8090`. It already contains Airflow, Spark, Postgres and Kafka —
most of what this pipeline needs.

### Options considered
1. **Unpause and reuse avapuck.** Rejected: it belongs to another project. Its
   schemas, DAGs and connections would be polluted by this work; unpausing has
   side effects the owner did not ask for; and the two projects' lifecycles
   would become coupled, so a teardown here could destroy work there.
2. **Reuse its ports with our own containers.** Rejected: guarantees a conflict
   the moment avapuck is unpaused.
3. **A fully isolated stack on dedicated, verified-free ports.** Chosen.

### Decision
Stand up our own stack. Ports were **confirmed free by live probe** before
allocation:

| Port | Component |
|------|-----------|
| 8095 | NFL Airflow webserver / API |
| 5440 | NFL Postgres (`staging` + `utm` schemas) |
| 3307 | NFL MySQL (source, binlog enabled) |
| 8096 | HTTP shim |
| 8097 | Kafka Connect / Debezium REST |
| 9099 | Kafka broker for Debezium |
| 8098 | DMS control-plane stub |
| 2225 | SFTP server |
| 4566 | floci — already running, not started by us |

Confirmed busy at the time and therefore forbidden: 3000, 3100, 4566, 5432,
5433, 5434, 7077, 8080, 8085, 8090, 8180, 8443, 8444, 8446, 8447, 8448, 9083,
10000, 15432, 18000, 19000, 19092, 29092. (Inherited from the first build, not
re-probed on 2026-10-04. The current standing list also includes 19001.)

**Standing rule: do not touch, unpause, or reuse the avapuck stack or its
ports.** The same rule now covers every other project on the machine:
containers `sso-*`, `claimsdemo-*`, `slate-*`, `local-cloudify-*` and `floci`,
and volumes `avapuck_*` and `local-cloudify_*`. The scripts act only on the
compose project `nfl`, selected by its compose file or its project label,
never by a name pattern and never with a prune.

### Consequence
More containers and more RAM on a single machine than reuse would have cost.
Accepted: the two projects stay independently startable and destroyable, and
nothing this project does can damage avapuck.

---

## D-006 — The stub runs only as a compose service (supersedes the host virtualenv)

**Date:** 2026-10-04
**Status:** Settled. The original decision was reversed the same day.

### What was first decided
The stub needs `fastapi`, `uvicorn`, `httpx`, `psycopg`. System Python is a
Homebrew interpreter that refuses `pip install` (PEP 668), so the first build
added a launcher, `stub/run-local.sh`, that created and used `stub/.venv`,
alongside the Docker image.

### What is true now
`stub/run-local.sh` was deleted, and `stub/.venv` and `shim/.venv` no longer
exist (checked 2026-10-04: all three paths are absent). The compose service
`dms-stub` (image built from `stub/Dockerfile`, non-root uid 10001) is the
only way the stub runs. See D-008.

### Why it was reversed
A host launcher needed its own copies of the credentials and its own network
addresses, so it could drift from what the stack runs. One launch path means
the thing tested is the thing started by `scripts/up.sh`.

---

## D-007 — The env file is `infra/nfl.env`, not `.env`

**Date:** 2026-10-04
**Status:** Settled, in use

### Context
Compose reads `.env` by default. On this machine the Claude Code tool
permissions deny every path whose basename starts with `.env`, so no agent can
create, read or check such a file, and a script that depended on one could not
be verified by the sessions that maintain it.

### Decision
The env file is `infra/nfl.env`, generated by `scripts/init-env.sh` from
`infra/env.example.txt` with every placeholder replaced by a random value.
Every script passes `--env-file infra/nfl.env` explicitly. `.gitignore` ignores
`*.env` and `.env*` by pattern and keeps only the two placeholder templates.

### Evidence
- `scripts/up.sh` stops with "missing infra/nfl.env" when the file is absent.
- A bare `docker compose -f infra/docker-compose.yml config` fails on the
  first required variable, because nothing loads the file implicitly.
- Infra fix round: the 220 files git would add contain none of the 20
  generated secret values.

### Consequence
A manual compose command must name the env file. That is why the runbook
never uses a bare `docker compose up`.

---

## D-008 — The shim and the stub run as compose services

**Date:** 2026-10-04
**Status:** Settled, in use

### Decision
`nfl-shim` (8096) and `nfl-dms-stub` (8098) are services in
`infra/docker-compose.yml`, built from `shim/` and `stub/`, both running as
uid 10001. `scripts/up.sh` builds and starts them with everything else and
then calls them to prove they reach their backing stores. There is no host
launcher for either.

### Evidence (2026-10-04)
- `docker ps`: `nfl-shim` and `nfl-dms-stub` healthy, ports bound to
  127.0.0.1.
- `up.sh` calls nine shim checks, the shim's `/health` and the stub's `/health`; any answer other
  than HTTP 200 fails the bring-up.
- The stub's `/health` returns 503 when it cannot reach Kafka Connect or
  Postgres, so its container healthcheck cannot pass on a broken stub.
  For that reason `up.sh` waits for the stub only after the connectors are
  registered.

### Consequence
Changing shim or stub code needs an image rebuild (`scripts/up.sh` does it).

---

## D-009 — The pipeline engines are stand-ins inside Airflow tasks

**Date:** 2026-10-04
**Status:** Settled. This is the largest limit of the harness.

### Context
Production runs Apache Hop for ingestion and Spark for extraction and
reporting. Neither is in this stack.

### Decision
The three stages are implemented as Python and SQL inside Airflow tasks:
- `nfl_ingestion`: SQL run as the Postgres role `nfl_hop`.
- `nfl_extraction`: psycopg2 and pyarrow, reading as `nfl_spark`.
- `nfl_reporting`: Python, reading S3 and writing SFTP.

The gates of the design ("Is Failed", "Is UTM", the S3 marker pre-check, "Is
Process Failed") are Airflow tasks in those DAGs. Observability is a Postgres
table, `observability.pipeline_event`; an alert is a `failed` row with
`metrics.alert=true`. There is no CloudWatch use, no dashboard and no alert
channel.

### Evidence (2026-10-04)
- `docker ps` shows nine `nfl-*` containers; none runs Hop or Spark.
- The DAG docstrings state it: "Local stand-in for Apache Hop", "Local
  stand-in for the Spark job ... psycopg2 + pyarrow".
- The shim's `openapi.json` has 12 check routes and no gate route.

### Consequence
The harness proves the contract between stages: inputs, outputs, markers,
checksums, gates, events. It proves nothing about Hop or Spark themselves.
Engine fidelity is none. `docs/COVERAGE.md` marks every such row STAND-IN.
An earlier `docs/ARCHITECTURE.md` claimed the real engines ran locally at high
fidelity; that was false and has been removed.

---

## D-010 — One Postgres role per check

**Date:** 2026-10-04
**Status:** Settled, in use. Replaces the shared `nfl_check` role.

### Context
D-004 requires one credential per shim endpoint. The first build gave each
Postgres check its own variable name but the same login, `nfl_check`, and the
startup guard compared variable names. Four checks therefore shared one
identity while appearing separate: the lineage check's credential could read
every staging and utm table.

### Decision
Five roles, one per Postgres check: `nfl_check_reconcile`,
`nfl_check_quality`, `nfl_check_freshness`, `nfl_check_lineage`,
`nfl_check_events`. Each has its own password and DSN, SELECT on only the
columns that check reads, and read-only transactions by default. The grant
list is the `grants` list in the matching `shim/roles/*.yaml`. `nfl_check` is
dropped on every start. The shim's guard now compares the principal inside
each credential with the declared identity and exits 78 on a mismatch or an
undeclared shared principal.

The writer roles were tightened in the same round: `nfl_replication` INSERT
only, `nfl_events` INSERT only, `nfl_spark` without the reject table.

### Evidence (2026-10-04, live database)
- The live column grants of all five roles equal the YAML `grants` lists
  exactly (5, 8, 15 and 37 column grants, and one whole-table SELECT for
  events; nothing extra, nothing missing, no privilege other than SELECT).
- Probes as each role: `nfl_check_lineage` can read `stage` and is refused
  `message`; `nfl_check_reconcile` is refused `payment_ref`;
  `nfl_check_quality` is refused `raw_row`; `nfl_events` cannot read the event
  table; `nfl_hop` cannot write staging; `nfl_replication` cannot read staging.
  Login as `nfl_check` fails: the role does not exist.
- Infra fix round: 32 of 32 forbidden statements refused, 11 of 11 required
  ones allowed; after grants were widened by hand, `up.sh` converged them back.

### Consequence and limit
More secrets to generate. S3 is the exception: the five S3-using checks
share one floci key and declare it (`shared_with`), because floci accepts any
key and five keys would look like separation while enforcing none. S3 scope
is not enforced locally at all.

---

## D-011 — Published ports bind 127.0.0.1 only

**Date:** 2026-10-04
**Status:** Settled, in use

### Context
The first build published ports on all interfaces. Kafka Connect's REST API
has no authentication, and Airflow's config endpoint returned the Fernet key,
the metadata database password and the session secret.

### Decision
Every published port in `infra/docker-compose.yml` is bound to `127.0.0.1`.
Airflow runs with `AIRFLOW__WEBSERVER__EXPOSE_CONFIG=non-sensitive-only`.

### Evidence (2026-10-04)
- `docker ps`: all eight published ports show `127.0.0.1:<port>`;
  `nfl-airflow-db` publishes none.
- `GET /api/v1/config` with the admin login returns 200 and contains none of
  the Fernet key, secret key, metadata password or admin password (compared
  by value). Without a login the API answers 401.
- Infra fix round: connections to the LAN address are refused.

### Consequence
The stack is reachable from this machine only. This is local hygiene, not
network design; it says nothing about subnets or security groups in AWS.

---

## D-012 — Replication freshness is judged by a heartbeat

**Date:** 2026-10-04
**Status:** Settled, in use

### Context
Two earlier signals both misled. The shim's lag check compares the MySQL
binlog position with the source connector's committed offset, which covers
MySQL to Kafka only: with the JDBC sink dead it still read "lag 0, within
budget". The age of the newest staging row goes stale on an idle source even
when replication is healthy.

### Decision
Debezium writes one row to MySQL `hawks.replication_heartbeat` every 20 s
(`heartbeat.action.query`). The table is in the connector's include list, so
each beat travels MySQL, Kafka, the JDBC sink and lands as a new row in
`staging.replication_heartbeat`. End-to-end freshness is the age of the
newest beat:
- `/checks/ingestion/freshness` fails when it is older than 300 s, and returns
  503 when the table is empty.
- `nfl_ingestion.pre_check` fails the run, with an alert, on the same limit
  (`NFL_REPLICATION_HEARTBEAT_REQUIRED=true`).
- `/checks/replication/lag` keeps its path but is labelled "source capture
  lag (MySQL to Kafka only, NOT end-to-end)".

### Evidence
- Live, 2026-10-04: freshness `ok: true` with a heartbeat tens of seconds old;
  Bruno `20-ingestion/14` asserts it; `90-chaos/74` shows source lag staying
  green with the sink paused, and `90-chaos/77` shows a beat written after the
  resume landing.
- Infra fix round, by hand: sink paused, freshness red at 313 s and green 14 s
  after resume; beats stopped with both connectors RUNNING, freshness red at
  327 s and an ingestion run failed at `pre_check` with an alert.
- Bruno fix round, by hand: sink paused 320 s, `20-ingestion/14` red.

### Consequence and limits
`debezium` gained INSERT and UPDATE on that one MySQL table. The staging
heartbeat table is not pruned and grows by one row per beat. On an idle
source a beat lands about every 48 s, not every 20 s (measured; the cause was
not established). The staleness limit is fixed at 300 s; there is no
end-to-end lag figure in seconds.
