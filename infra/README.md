# NFL local infrastructure

Isolated local stack for the NFL Data Platform reporting pipeline. Compose
project name is `nfl`; nothing here shares a port, container, volume or network
with the paused avapuck `pl-*` stack on this machine, and floci (`:4566`) is
treated as an external, externally-managed dependency.

## Quick start

Prerequisites: Docker with compose v2, `python3`, `curl`, `lsof`, `ssh-keygen`, and
floci already answering on `:4566` (nothing here starts it):

```bash
curl -fsS http://localhost:4566/_localstack/health   # must answer; floci is external
./scripts/init-env.sh    # once: generates infra/nfl.env and the SFTP keys. No editing.
./scripts/up.sh          # every time: ensures the S3 bucket, builds, starts, waits for
                         # health, converges schemas/roles/grants, registers the
                         # connectors, proves each check identity. Exit 0 = all of it.
./scripts/down.sh        # stop; named volumes (data) kept
./scripts/reset.sh -y    # wipe the nfl volumes AND the pipeline's S3 prefixes
```

`up.sh` exits non-zero, and never prints its closing "Stack is up" line, when
floci is unreachable, a service is unhealthy, a connector is not RUNNING, or a
check could not run. On a cold start it waits up to 120s for two checks that
cannot run immediately: the source-lag check (Kafka Connect commits its first
offset on a 60s timer) and the freshness check (the first heartbeat has to
land). It waits for them to be able to run; it does not relax their verdict.

`reset.sh` removes only volumes of the `nfl` compose project and, through the
floci endpoint, only keys under `df_*/`, `reports/`, `_tmp/` and `_fixtures/`
in the one nfl bucket (`scripts/s3_bucket.py`, which refuses any non-loopback
endpoint). A reset therefore cannot leave an old `_SUCCESS` marker or report
behind for the next stack's checks to pass on.

`init-env.sh` copies `infra/env.example.txt` to `infra/nfl.env` and replaces
every `CHANGEME_*` with a generated value (32-character alphanumeric passwords,
a valid Fernet key, a 64-hex secret key). It refuses to overwrite an existing
file without `--force` and never prints a value. It also generates, under
`infra/keys/sftp/`, the SFTP server's host keys, the read-only checker's key
pair and the matching `known_hosts`. Both `infra/nfl.env` and `infra/keys/` are
gitignored.

The env file is `nfl.env`, not `.env`, because this machine's Claude Code
permissions deny all tool access to `.env*` paths. Every script passes
`--env-file infra/nfl.env` explicitly; for a manual compose command do the same.

## Ports

| Port | Service | Container |
|------|---------|-----------|
| 8095 | Airflow webserver + REST API | `nfl-airflow` |
| 5440 | Postgres (`staging`, `utm`, `observability`) | `nfl-postgres` |
| 3307 | MySQL source (binlog on) | `nfl-mysql` |
| 8097 | Kafka Connect / Debezium REST | `nfl-kafka-connect` |
| 9099 | Kafka broker (KRaft) | `nfl-kafka` |
| 2225 | SFTP delivery target | `nfl-sftp` |
| 8096 | Check shim (`shim/README.md`) | `nfl-shim` |
| 8098 | DMS control-plane stub (`stub/README.md`) | `nfl-dms-stub` |
| 4566 | floci — **external, not started here** | `floci` |

Every port is published on `127.0.0.1` only. Kafka Connect's REST API has no
authentication, so nothing here is reachable from the LAN.

Airflow's metadata database (`nfl-airflow-db`) deliberately has **no** host
port. It is a separate Postgres container from the pipeline database so the
orchestrator's state and the data it orchestrates never share a credential or a
blast radius — mirroring the two separate RDS instances this would be in AWS.

## The DMS question

**Stage 1 of the design is Hawks MySQL → AWS DMS → Postgres `staging`. DMS
cannot be emulated locally. Verified, not assumed:**

- floci has no DMS at all. Live probe this session:
  ```
  aws --endpoint-url=http://localhost:4566 dms describe-replication-tasks
  → UnknownOperationException: AmazonDMSv20160101.DescribeReplicationTasks
  aws --endpoint-url=http://localhost:4566 dms describe-endpoints
  → UnknownOperationException: AmazonDMSv20160101.DescribeEndpoints
  ```
- LocalStack free has no DMS either, and even paid LocalStack's DMS only
  supports **Kinesis** targets — never PostgreSQL. So the one thing stage 1
  needs (a Postgres target) is the one thing that emulation cannot provide at
  any price tier.

So DMS is split into its two halves and each is handled honestly:

| Half of DMS | Locally | In AWS |
|---|---|---|
| **Data plane** — actually moving MySQL rows into Postgres `staging` | Debezium (real CDC off a real binlog) | DMS replication task |
| **Control plane** — "is the replication task running, what's its lag" | `nfl-dms-stub` on `:8098`, speaking the DMS awsjson1.1 shape | Real `dms:DescribeReplicationTasks` |

The data plane is *real* locally — a real binlog, a real CDC connector, real
rows landing in Postgres. Only the control-plane *API surface* is stubbed, and
every stub response is marked (`"_stub": true`, header `X-NFL-Stub: true`), so
the same requests repoint at real AWS DMS by changing the endpoint and nothing
pretends to be DMS that isn't.

## Least privilege

The owner's requirement is that every credential lifts directly to an AWS task
or IAM role with exactly its own scope. At the database layer that is:

**MySQL** (`infra/mysql/init/02-debezium-user.sql.tmpl`, `03-lag-check-user.sql.tmpl`, `04-replication-heartbeat.sql`)

| User | Grants | Lifts to |
|---|---|---|
| `debezium` | `REPLICATION SLAVE`, `REPLICATION CLIENT`, `RELOAD` on `*.*`; `SELECT` on `hawks.*`; `INSERT`, `UPDATE` on `hawks.replication_heartbeat` only | DMS source endpoint user |
| `hawks_app` | DML on `hawks.*` only, no replication rights | application role |
| `nfl_check_replication_lag` | `REPLICATION CLIENT` on `*.*` only; no table access | shim lag-check role |

`RELOAD` is global-only in MySQL (it cannot be scoped to a database), so it is
granted at `*.*` while `SELECT` stays scoped. CDC never runs as root. The one
write grant exists so Debezium can stamp the heartbeat row (see below).

**Postgres** (`infra/sql/03-roles.sql.tmpl`, `06-check-identities.sql.tmpl`)

| Role | `staging` | `utm` | `observability` | Lifts to |
|---|---|---|---|---|
| `nfl_replication` | INSERT on the 4 landed tables; no SELECT/UPDATE/DELETE, no CREATE | none | none | DMS target endpoint user |
| `nfl_hop` | SELECT on the 4 landed tables | SELECT/INSERT/UPDATE on batch, dim, facts; SELECT/INSERT/DELETE on `ingestion_reject` | none | Hop ECS task role |
| `nfl_spark` | none | SELECT on batch, dim, facts (not `ingestion_reject`) | none | EMR/Glue job role |
| `nfl_stub_reader` | SELECT on the 3 replicated tables | none | none | DMS stub |
| `nfl_events` | none | none | INSERT on `pipeline_event` (cannot read it) | event emitter in each DAG |
| `nfl_check_reconcile` | key + time columns | event-time column | none | `/checks/ingestion/reconcile` |
| `nfl_check_quality` | key + time columns | key + time columns, batch row, reject keys (not `raw_row`) | none | `/checks/ingestion/quality` |
| `nfl_check_freshness` | `dms_loaded_at`, heartbeat | `utm_loaded_at` | none | `/checks/ingestion/freshness` |
| `nfl_check_lineage` | none | none | 5 columns of `pipeline_event` | `/checks/lineage` |
| `nfl_check_events` | none | none | SELECT on `pipeline_event` | `/checks/events` |

The grant list of each `nfl_check_*` role is the `grants` list in the matching
`shim/roles/*.yaml`; the two are changed together. There is no shared check
role any more: `nfl_check` is dropped on every start if it exists.

No role owns or can create an object, nothing is granted through `ALL TABLES`
or default privileges, and every read-only role carries
`default_transaction_read_only = on`. `PUBLIC` holds no `CONNECT` on `nfl`,
`postgres` or `template1` and nothing on schema `public`. `utm.v_reconciliation`
was removed: nothing read it, and as an owner-rights view it leaked staging
counts to any role with SELECT on utm. The database name `nfl` is fixed, not a
tunable.

Both templates begin by revoking everything from their roles and then grant
only what is listed, so a volume created with wider grants converges. `up.sh`
applies, on every start: `01-staging-schema.sql`, `02-utm-schema.sql`,
`05-observability.sql`, `03-roles.sql.tmpl` (through `03-render-roles.sh`
inside the container, the same path the postgres entrypoint takes on an empty
volume), `06-check-identities.sql.tmpl` (rendered in memory), and the two MySQL
files `03-lag-check-user.sql.tmpl` and `04-replication-heartbeat.sql`. Either
render path fails hard if a placeholder survives. No credential is in git.

**SFTP** (`infra/sftp/sshd_config`): `nfldelivery` uploads with a password into
its own `outbound` directory. The directory is a named volume, which Docker
creates root-owned; `infra/sftp/10-outbound-owner.sh`, mounted into
`/etc/sftp.d` and run by the image's entrypoint on every start, hands it to the
delivery user. `nfl_check_delivery_sftp` is key-only, forced to
`internal-sftp -R`, and sees the same directory through a read-only mount. The
server's host keys come from `infra/keys/sftp/` so clients pin them through
`infra/keys/sftp/known_hosts`.

Connector passwords are read through Kafka Connect's `FileConfigProvider` from
`infra/secrets/connect-secrets.properties` (generated by `up.sh`, mode 0600,
gitignored) rather than inlined in the connector JSON.

## Seed data and the failure paths

`infra/sql/04-seed.sql` (MySQL syntax — it targets the source, unlike the
Postgres files) seeds **122 / 203 / 152** rows across `patient_account`,
`account_transaction` and `payment`, including seven deliberately bad rows so
the quality gates are exercisable rather than theoretical:

| Tag | Table | Defect |
|---|---|---|
| BAD-1 | `patient_account` | null business key |
| BAD-2 | `patient_account` | duplicate business key |
| BAD-3 | `account_transaction` | null business key |
| BAD-4 | `account_transaction` | duplicate business key |
| BAD-5 | `account_transaction` | orphan `account_number` |
| BAD-6 | `payment` | null business key |
| BAD-7 | `payment` | duplicate business key |

Business keys are nullable in `staging` on purpose — staging must be able to
land a bad source row so the gate has something to catch. `utm` declares them
`NOT NULL UNIQUE`, so these rows cannot be promoted and land in
`utm.ingestion_reject` instead, driving `rows_rejected > 0` and failing the
"Is Failed" gate. **A run that reports zero rejects means the gate is not
wired, not that the data is clean.**

## Replication heartbeat (end-to-end freshness)

"No new source rows" must be distinguishable from "CDC is dead". The source
connector runs `heartbeat.action.query` every `heartbeat.interval.ms` (20s):
an upsert of the single row in MySQL `hawks.replication_heartbeat` with
`UTC_TIMESTAMP(3)`. That change is captured from the binlog like any data
change, routed to topic `staging.replication_heartbeat`, and inserted by the
sink as a NEW row in Postgres `staging.replication_heartbeat`
(`source_name`, `beat_at`, `dms_op`). `now() - max(beat_at)` is therefore the
age of the newest change that made it through MySQL -> Kafka -> Postgres.

- On an idle source the measured cadence is one beat per 48s, not 20s:
  Debezium only evaluates the heartbeat when a binlog event arrives, and an
  idle MySQL sends one keep-alive event per 48s. With traffic it is 20s.
- Readers: the shim's `/checks/ingestion/freshness` and the ingestion DAG's
  `pre_check`, both with a 300s limit. Compose sets
  `NFL_REPLICATION_HEARTBEAT_REQUIRED=true`, so a missing heartbeat fails the
  DAG instead of being reported as "not enforced".
- The staging table has no primary key (the sink is at-least-once) and is not
  pruned: it grows by at most 4,320 rows a day (one per 20s).

## Staging is append-only, and that drives the sink config

`staging.*` tables deliberately have **no primary key and no unique
constraint** — a duplicate business key has to be able to land so the gate can
catch it. That decides two JDBC-sink settings, and getting them wrong fails
loudly in different ways (both reproduced here before settling):

| Setting | Value | Why not the alternative |
|---|---|---|
| `insert.mode` | `insert` | `upsert` emits `ON CONFLICT (pk)` → `ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification` |
| `primary.key.mode` | `none` | `record_key` makes the sink expect a PK that staging does not have |

Two more sink/source details that are easy to get silently wrong:

- The sink's default table-naming strategy **flattens dots**, so a topic named
  `staging.payment` becomes a table literally called `staging_payment`. The
  schema is selected with `?currentSchema=staging` on the JDBC URL and the
  topic prefix is stripped by a `RegexRouter`, so `${topic}` resolves to the
  bare table name.
- The source's `ExtractNewRecordState` transform adds metadata fields with a
  `__` prefix by default (`__dms_op`, `__deleted`). Those columns do not exist
  in staging, and with `schema.evolution: none` the sink fails with
  `Cannot alter table ... because schema evolution is disabled`. So the source
  sets `add.fields.prefix` to empty and `delete.handling.mode: drop`, yielding
  exactly the `dms_op` column staging declares.

## What was proven on a live run, and what was not

Everything below was executed on 2026-10-04 (UTC) against the uncommitted tree.

**Cold start, from nothing, twice, both green on the first try:**

- Pass 1 in this directory: `reset.sh -y`, generated env/keys/secrets removed,
  `init-env.sh`, `up.sh` (104s, exit 0).
- Pass 2 from a clean copy holding exactly the 220 files `git add .` would
  take (no env file, no keys), with the S3 bucket deleted first so the
  bootstrap had to create it: `reset.sh -y` 6s, `init-env.sh` 1s, `up.sh` 103s,
  exit 0. Of the 103s, 60s is the wait for Kafka Connect's first offset commit.
- After each: 9 containers healthy, both connectors RUNNING, staging
  122 / 203 / 152 rows equal to MySQL, heartbeat landing; August
  (`nfl_ingestion`, `nfl_extraction`, `nfl_reporting` via REST, 27s) all
  success on try 1, 8 objects in S3, the report delivered to SFTP under its
  final name, owned 1001:1001 on the fresh volume; every shim check for August
  green; September failed at `is_failed_gate` with its alert in
  `/checks/events`; a `{}`-conf run recorded a NULL-interval alert visible
  through `/checks/events?unscoped=true`.

**Warm restart:** `down.sh` 6s, `up.sh` 37-38s, exit 0; event, utm and batch
counts identical before and after; named volumes kept; no anonymous volume
left behind.

**Mutations (each restored and re-verified):**

- Sink paused: freshness went red at 313s, green 14s after resume.
- Debezium's write grant on the heartbeat table revoked (both connectors still
  RUNNING): freshness red at 327s, and an ingestion run failed in `pre_check`
  with "replication is stale ... 330s ago" and an alert row; green after the
  grant was restored.
- Grants widened by hand (replication ALL + CREATE, spark ALL TABLES, the old
  `nfl_check` role, the old view, PUBLIC CONNECT on `postgres`, ...): 14 probes
  that must be refused succeeded; after `up.sh` all 32 were refused again.
- Upload dir set to root:root (the state of a new volume): uid 1001 could not
  write; after a container restart the mounted script had repaired it.
- Host-side emulator endpoint pointed at a dead port: `up.sh` exit 1,
  `floci:unreachable`, no closing "Stack is up" line.

**Also measured:** `GET /api/v1/config` returned the Fernet key, the
metadata-DB password and the session secret before the change and none of them
after (compared by value); every published port listens on 127.0.0.1 only and
the LAN address refuses connections; the 220 files git would add contain none
of the 20 generated secret values and no private key.

**Not proven / known gaps:**

- A literal `git clone` was not possible (nothing is committed); pass 2 used a
  copy of the files git would add.
- The infra fix round did not run the Bruno collection. It was run afterwards on
  this stack (2026-10-04 UTC): normal run 100/100 requests and 309/309 tests,
  `90-chaos` 57/57 and 160/160, normal run again 100/100 and 309/309.
- Convergence of a pre-heartbeat volume (connector already streaming when the
  MySQL heartbeat table is first created) was not exercised; only fresh
  volumes and a hand-widened one were.
- The 48s idle cadence is measured; its cause (the binlog keep-alive interval)
  is from Debezium's behaviour as recalled, not from a controlled experiment.
- A failed or crashed sink task, as opposed to a paused one, was not induced.
- The privilege probes log in over the container's local socket (trust
  authentication); passwords are exercised by the shim, the DAGs, the sink and
  the stub, not by the probes.
- `atmoz/sftp:alpine` is **amd64-only** and runs under emulation on this
  arm64 machine.
- Apache Hop and Spark are not in this stack; the Airflow DAGs perform
  ingestion and extraction in Python.
- `shellcheck` is not installed here; the scripts have had `bash -n` only.
- floci enforces no IAM, so no S3 scope is enforced locally: the S3 checks
  prove reachability. All DAG credentials sit in one Airflow container, and
  the single Airflow API user is an Admin.
