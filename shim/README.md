# NFL check shim

Turns every non-HTTP check in the NFL Data Platform pipeline into an HTTP GET, so each
one is a Bruno request with a Send button. FastAPI, port **8096**, compose service
`shim` (container `nfl-shim`).

## Running it

It starts with the stack. There is no separate step and nothing to edit:

```bash
scripts/init-env.sh     # once
scripts/up.sh           # builds and starts nfl-shim, then proves each identity authenticates
```

After changing shim code:

```bash
docker compose --project-directory infra -f infra/docker-compose.yml \
  --env-file infra/nfl.env up -d --build --no-deps shim
```

`http://localhost:8096/checks` lists every endpoint with its parameters and role.
`http://localhost:8096/docs` is the OpenAPI UI.

The compose service must supply one DSN per Postgres check, each logging in as that
check's own role: `SHIM_INGESTION_RECONCILE_PG_DSN` (`nfl_check_reconcile`),
`SHIM_INGESTION_QUALITY_PG_DSN` (`nfl_check_quality`), `SHIM_INGESTION_FRESHNESS_PG_DSN`
(`nfl_check_freshness`), `SHIM_LINEAGE_PG_DSN` (`nfl_check_lineage`) and
`SHIM_EVENTS_PG_DSN` (`nfl_check_events`). A compose file that still passes the shared
`nfl_check` login in those variables makes the container exit 78 on purpose.

The compose service is the only supported way to run it. `shim/env.example` is a
reference for the variables, not a file to copy.

## It refuses to start on a missing, mismatched or silently shared credential

Every endpoint's credential variable is declared in `shim/roles/<name>.yaml`. At startup
(`shim/roles.py`) the process checks, and exits **78** naming each problem, when:

- a declared variable is missing or empty;
- two endpoints claim the same variable;
- a Postgres or MySQL DSN logs in as a different user than the role file's
  `identity.name` (the declaration must describe the credential actually supplied);
- two endpoints resolve to the same principal (Postgres user, MySQL user, S3 access key,
  Connect basic-auth user, SFTP user) and do not BOTH list each other under
  `shared_with` with a `shared_rationale`.

Variable names are not compared as a stand-in for identities: five variables all carrying
`nfl_check` is a refusal, not a pass. The only declared sharing today is the floci S3 key
(see Identities). There is no fallback to a wider credential, and an endpoint cannot read
a variable it did not declare (`Role.secret()` raises).

What the guard does NOT establish: that the database holds no grant beyond the ones a
role file lists. It compares who logs in, not what that login was granted.

## Response contract

"Could not run" never looks like "passed".

| HTTP | `state` | Meaning |
|------|---------|---------|
| 200 | `pass` | The check ran and holds (`ok: true`) |
| 200 | `fail` | The check ran and is violated (`ok: false`) |
| 422 | `unverifiable` | Bad or missing parameters; the check never ran |
| 501 | `unverifiable` | Disabled here, explicitly NOT verified |
| 503 | `unverifiable` | Could not run: dependency down, credential rejected, or a value the verdict needs was unavailable |

Every 200 carries `ok`, `state`, `check`, `role`, `measured`, `threshold`, `summary`,
`params`. `measured` holds the numbers the verdict came from.

## Endpoints

Intervals are inclusive calendar days, `YYYYMMDD` or `YYYY-MM-DD`.

| Endpoint | Params | Passes when |
|---|---|---|
| `GET /health` | — | every dependency has a TCP listener (503 otherwise). Proves nothing about auth. |
| `GET /checks` | — | always; self-describing index |
| `GET /checks/replication/lag` | — | **source capture lag, MySQL to Kafka only** (`check: replication.source_capture_lag`): Debezium's committed binlog position equals MySQL's write position, or trails it by no more than `SHIM_REPLICATION_LAG_THRESHOLD_SECONDS` (300). Not end-to-end |
| `GET /checks/replication/connector-status` | — | BOTH the Debezium source (`SHIM_DEBEZIUM_CONNECTOR`) and the JDBC sink (`SHIM_SINK_CONNECTOR`) are RUNNING, each with at least one task and every task RUNNING |
| `GET /checks/ingestion/reconcile` | `interval_start`, `interval_end` | for ALL three table pairs, source rows in staging = rows in utm for the interval, and not every pair is empty |
| `GET /checks/ingestion/quality` | `interval_start`, `interval_end`, `layer=utm\|staging` (default `utm`) | all three tables of the layer hold no null/blank and no duplicate business key, at least one row exists, AND the ingestion batch for exactly this window is `SUCCEEDED` with zero rejects |
| `GET /checks/ingestion/freshness` | — | the replication heartbeat's newest beat (stalest source) is younger than `SHIM_HEARTBEAT_MAX_AGE_SECONDS` (300) and the newest `utm_loaded_at` is younger than `SHIM_FRESHNESS_THRESHOLD_SECONDS` (900). **503** while the heartbeat table is empty |
| `GET /checks/extraction/s3-marker` | `interval_start`, `interval_end` | `_SUCCESS` exists under the interval prefix |
| `GET /checks/extraction/manifest` | `interval_start`, `interval_end` | `manifest.json` parses, declares the requested interval, lists files that all carry a checksum, and declares more than zero rows |
| `GET /checks/extraction/parquet-rowcount` | `interval_start`, `interval_end`, `layer=silver\|gold` | rows read from the Parquet footers equal the manifest's `layers.<layer>.row_count` |
| `GET /checks/reporting/file` | `interval_start`, `interval_end`, `report` | the report exists, is non-empty, and its sha256 equals the published one |
| `GET /checks/delivery/sftp` | `interval_start`, `interval_end`, `report` | the file is on SFTP under its final name, no temp name left, size and sha256 equal the S3 report |
| `GET /checks/lineage` | `interval_start`, `interval_end` | all five stages have events for EXACTLY this interval (`interval_start` = start AND `interval_end` = end) and each stage's latest terminal event is `success` |
| `GET /checks/events` | `interval_start` + `interval_end`, or `unscoped=true`; optional `status`, `stage`, `alerts_only`, `limit` | at least one event matches. A listing, not a health verdict: use `lineage` for that |

The three table pairs (`shim/config.py` `TABLE_PAIRS`, mirroring `infra/sql/01`, `02` and
the ingestion DAG):

| Pair | Staging table | utm table | Source PK | Business key | Interval column |
|---|---|---|---|---|---|
| `patient_account` | `staging.patient_account` | `utm.dim_patient_account` | `account_id` | `account_number` | `opened_at` |
| `account_transaction` | `staging.account_transaction` | `utm.fact_account_transaction` | `transaction_id` | `transaction_ref` | `posted_at` |
| `payment` | `staging.payment` | `utm.fact_payment` | `payment_id` | `payment_ref` | `received_at` |

An interval `S..E` selects `[S 00:00 UTC, E+1 day 00:00 UTC)` on the interval column, the
same half-open window the ingestion DAG uses.

### Behaviour worth knowing

- **`replication/lag`** measures ONE hop: MySQL binlog write position
  (`SHOW MASTER STATUS`) against the offset the Debezium source connector has committed
  (`GET /connectors/<name>/offsets`). It says nothing about the JDBC sink or staging, and
  its response carries `end_to_end: false` and a `scope` sentence saying so. End-to-end
  staleness is `ingestion/freshness` (heartbeat); sink health is `connector-status`.
  Kafka Connect commits offsets on a timer, so the committed position trails real
  consumption between flushes (roughly two minutes was observed here after an uncaptured
  binlog event). One "behind" sample therefore cannot tell a stall from a pending flush.
  The check answers: equal positions → pass, lag 0; behind but the last committed event
  is within budget → pass with that upper bound; behind and over budget → **503** until
  the shim itself has watched the position stay put for longer than the budget, then
  **fail** ("STALLED"). The watch is in-process memory and restarts with the container.
- **`replication/connector-status`** fails when either connector is not RUNNING, has no
  task, or has any task that is not RUNNING (PAUSED and FAILED both count). A connector
  that is not registered is **503**. `measured.connectors.source` / `.sink` carry each
  connector's own numbers; the top-level `tasks_running` etc. are sums over both, and
  top-level `connector_state` is RUNNING only when both are.
- **`ingestion/reconcile`** counts, per pair, the newest CDC version of each source
  primary key whose event time is in the window (staging is an append-only CDC log: an
  UPDATE at the source lands as a second staging row; raw rows are reported alongside).
  The verdict fails if ANY pair differs by more than `SHIM_RECONCILE_TOLERANCE_ROWS`, or
  if all pairs are empty. A single pair with no rows on either side is listed under
  `table_pairs_empty` and does not fail the check by itself. Top-level `staging_rows` /
  `utm_rows` are totals; `measured.tables.<pair>` holds each pair.
- **`ingestion/quality`** inspects business keys in all three tables of the chosen layer
  and then judges the ingestion batch in `utm.ingestion_batch` whose
  `window_start`/`window_end` equal the window exactly: no batch, a batch that is not
  `SUCCEEDED`, or any reject (`utm.ingestion_reject`, or `rows_rejected` on the batch)
  fails the check for either layer. utm cannot hold a null or duplicate key by
  constraint, so without the batch a failed load whose bad rows were rejected would read
  as clean. Rejects are reported by table and reason with a capped sample of rejected
  business keys; `raw_row` is never read. Zero rows in the interval is a fail.
- **`ingestion/freshness`** judges two clocks. Replication: the heartbeat (contract
  below). The newest staging DATA row is reported but is not part of the verdict while a
  heartbeat is configured, because an idle source ages it without anything being wrong.
  utm: newest `utm_loaded_at` over the three utm tables. If the heartbeat table is
  configured and empty the answer is **503**, never a pass. With `SHIM_HEARTBEAT_TABLE`
  set to an empty string the check falls back to the newest staging data row and says
  in its summary that idle and dead then look the same.
- **`events`** returns rows of `observability.pipeline_event` newest first, capped at
  `limit` (default 50, never above `SHIM_EVENTS_ROW_CAP`, 200), with `counts` computed
  over every match (`matched`, `alerts`, `by_status`, `by_stage`, `returned`) and
  `truncated`. Each row carries `alert` (status `failed` with `metrics.alert` = true),
  `severity`, `error_summary` and the full `metrics`. With an interval it matches that
  interval exactly; with `unscoped=true` it lists rows whose interval bounds are both
  NULL, which is where an alert lands when a run's conf yields no valid interval.
  `ok` is true when at least one row matched and false when none did.
- **`reporting/file`** needs a published checksum (object metadata `sha256` or a
  `<key>.sha256` sidecar). Without one it is 503: presence alone is not integrity.
- **`delivery/sftp`** returns 503 if the file is on SFTP but the S3 source cannot be
  read, because presence is proven while integrity is not.
- **`lineage`** matches the interval exactly. A sub-range, a super-range or a range that
  straddles two recorded intervals matches nothing and fails; events of other intervals
  that overlap the request are listed for orientation and never counted. It uses each
  stage's LATEST terminal event, so a failed attempt followed by
  a successful re-run for the same interval reads as succeeded, and the reverse reads as
  failed. A stage with only `started`/`retry` events is "not finished" and fails.

## Artifact layout the checks assume

This is the contract between the shim and whatever produces the artifacts (the Airflow
DAGs in `infra/airflow/dags/`). `<prefix>` is `df_<start>_<end>` with both bounds as
inclusive `YYYYMMDD` days, e.g. `df_20260801_20260831`
(`SHIM_INTERVAL_PREFIX_TEMPLATE`).

**S3** (bucket `nfl-data-platform`, `SHIM_S3_BUCKET`):

```
<prefix>/_SUCCESS                         written last; any size
<prefix>/manifest.json
<prefix>/silver/**/*.parquet              any depth below silver/
<prefix>/gold/**/*.parquet
reports/<prefix>/<report>                 `report` is a bare file name
reports/<prefix>/<report>.sha256          sidecar: sha256 hex as the first token
                                          (or object metadata sha256=<hex> on the report)
```

`manifest.json` must be a JSON object with:

```json
{
  "schema_version": "1.0.0",
  "interval": {"start": "20260801", "end": "20260831"},
  "layers": {
    "silver": {"row_count": 460, "files": [{"path": "silver/x/part-0.parquet", "checksum": "<sha256>"}]},
    "gold":   {"row_count": 68,  "files": [{"path": "gold/y/part-0.parquet",   "checksum": "<sha256>"}]}
  }
}
```

`interval.start`/`end` must equal the requested bounds (dashes ignored). Every file needs
`checksum` (or `sha256`/`md5`). `layers.<layer>.row_count` must be an integer equal to
the summed Parquet footer row counts under that layer's prefix.

**SFTP** (`nfl-sftp`): the delivered file is `/outbound/<report>` as seen by the SFTP
client. An upload in progress is `/outbound/<report>.tmp` (`SHIM_DELIVERY_SFTP_TEMP_SUFFIX`)
and must be renamed away. The bytes must equal `reports/<prefix>/<report>` in S3.

**Postgres** `observability.pipeline_event` (`infra/sql/05-observability.sql`): one row
per step state change with `stage` in `replication, ingestion, extraction, reporting,
delivery`, `status` in `started, success, retry, failed, skipped`, and
`interval_start`/`interval_end` as dates (`interval_end` is the last day covered).
`failed` is terminal for an attempt; a step that will be retried writes `retry`. Written
as role `nfl_events` (INSERT + SELECT on that table only). An alert is a `failed` row
whose `metrics` has `alert: true`, `severity` and `error_summary`. An alert for a run
with no valid interval has NULL `interval_start` and `interval_end` (both), and is only
visible through `GET /checks/events?unscoped=true`.

**Postgres replication heartbeat** (what `ingestion/freshness` reads; defaults shown):

| Setting | Default | Contract |
|---|---|---|
| `SHIM_HEARTBEAT_TABLE` | `staging.replication_heartbeat` | one row per beat, landed by the same MySQL → Debezium → Kafka → JDBC sink path as data |
| `SHIM_HEARTBEAT_COLUMN` | `beat_at` | `timestamptz`, the time the beat was WRITTEN AT THE SOURCE (not the landing time), comparable with Postgres `now()` |
| `SHIM_HEARTBEAT_SOURCE_COLUMN` | `source_name` | text naming the source; each source's newest beat is judged and the stalest decides |
| `SHIM_HEARTBEAT_SOURCE` | empty | when set, only that `source_name` is judged |
| `SHIM_HEARTBEAT_MAX_AGE_SECONDS` | `300` | the beat interval at the source must be well below this (60s or less) |

The query is `SELECT source_name, MAX(beat_at), COUNT(*) FROM <table> WHERE beat_at IS
NOT NULL GROUP BY source_name`. The check role needs `SELECT (source_name, beat_at)` on
the table and nothing else. No rows → 503. Rows older than the limit → fail. The table
may be append-only or upserted; only the newest beat per source matters. The ingestion
DAG's pre-check reads the same table and columns with the same 300s default.

**Postgres ingestion batch** (what `ingestion/quality` reads): `utm.ingestion_batch` has
one row per interval with `window_start`/`window_end` equal to the half-open UTC window,
`status` in `RUNNING, SUCCEEDED, FAILED`, and `rows_read/rows_loaded/rows_rejected`;
`utm.ingestion_reject` has one row per rejected source row keyed by `utm_batch_id` with
`source_table`, `reject_reason`, `business_key`.

## Identities

One credential variable and one principal per endpoint, declared in `shim/roles/`.

| Endpoint | Variable | Principal | Enforced by the local backing store |
|---|---|---|---|
| `ingestion/reconcile` | `SHIM_INGESTION_RECONCILE_PG_DSN` | Postgres `nfl_check_reconcile` | column SELECT on PK, event-time and CDC-version columns of the 3 staging tables and the event-time column of the 3 utm tables; read-only |
| `ingestion/quality` | `SHIM_INGESTION_QUALITY_PG_DSN` | Postgres `nfl_check_quality` | column SELECT on business key + event time (both layers), the batch row, and reject table without `raw_row`; read-only |
| `ingestion/freshness` | `SHIM_INGESTION_FRESHNESS_PG_DSN` | Postgres `nfl_check_freshness` | column SELECT on `dms_loaded_at`, `utm_loaded_at`, heartbeat `source_name, beat_at`; read-only |
| `lineage` | `SHIM_LINEAGE_PG_DSN` | Postgres `nfl_check_lineage` | column SELECT on `stage, status, event_time, interval_start, interval_end` of `observability.pipeline_event`; read-only |
| `events` | `SHIM_EVENTS_PG_DSN` | Postgres `nfl_check_events` | SELECT on `observability.pipeline_event`; read-only |
| `trace` | `SHIM_TRACE_PG_DSN` | Postgres `nfl_check_trace` | SELECT on `observability.pipeline_event` and `observability.alert_delivery`; read-only |
| `alerts/delivery` | `SHIM_ALERT_DELIVERY_PG_DSN` | Postgres `nfl_check_alert_delivery` | column SELECT on eight columns of `pipeline_event`, SELECT on `alert_delivery`; read-only |
| `monitor` | `SHIM_MONITOR_PG_DSN` | Postgres `nfl_check_monitor` | SELECT on `observability.monitor_sample`; read-only |
| `replication/lag` | `SHIM_REPLICATION_LAG_MYSQL_DSN` | MySQL `nfl_check_replication_lag` | `REPLICATION CLIENT` only; table SELECT and binlog reads are refused |
| `replication/*` (Connect side) | `*_CONNECT_URL` | none: the local Connect REST API has no auth | nothing; the code issues GETs only |
| `extraction/*`, `reporting/file`, S3 side of `delivery/sftp` | five `*_AWS_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` pairs | ONE shared floci key, declared under `shared_with` in each of the five role files | **nothing. Reachability only.** floci accepts any key for any action and evaluates no IAM policy, so a passing S3 check proves the endpoint answered and the object is there; it proves no IAM identity and no S3 scope |
| `delivery/sftp` | `SHIM_DELIVERY_SFTP_*` | SFTP user `nfl_check_delivery_sftp`, ed25519 key | key-only login, `internal-sftp -R`, read-only mount, pinned host keys (`RejectPolicy`) |
| `/health`, `/checks` | none | none | hold no credential |

The exact DDL for each Postgres role is the `grants` list of its role file, and
`GET /checks` returns it. The five roles are created by the infra layer (`infra/sql`),
not by the shim; until compose supplies a DSN per role, the shim exits 78. The per-check
S3 policies and Connect ACLs in `aws_iam_policy` / `grants` are the production target;
each role file's `local_enforcement` field, also returned by `GET /checks`, states the
difference.

## Configuration

Every setting is an environment variable; `shim/env.example` lists them with defaults.
Table and column names are validated as SQL identifiers and composed with `psycopg.sql`.
The three table pairs are code (`TABLE_PAIRS` in `shim/config.py`), not environment
variables, because they must match the schema and the ingestion DAG together.

## Proven on the live stack (2026-10-04)

Two rounds. The second (fix round) ran the current code in a one-off container next to
the stack, logged in as five per-check Postgres roles created from the role files'
`grants` lists exactly as written, with the pipeline's real data for 20260801..20260831
(clean) and 20260901..20260930 (failed batch).

Fix round, current code:

- **lineage, exact interval.** Before: 20260801..20260802, 20260801..20260930 and
  20260715..20260815 all returned pass. After: all three fail ("no event is recorded for
  exactly this interval"), 20260801..20260831 passes, September fails. Mutation: putting
  the range predicate back made 20260801..20260802 pass again.
- **reconcile, three pairs.** August pass with 118/118, 196/196, 146/146 (460 = 460).
  September fail in 3 of 3 pairs (4 vs 1, 7 vs 3, 6 vs 3). Mutation: ignoring the
  per-pair failures made September pass.
- **quality, batch and rejects.** Before: September with the default layer returned
  pass ("3 rows clean"). After: fail, batch `df_20260901_20260930` FAILED, 10 rejects
  (6 duplicate, 3 null, 1 orphan) listed by table. August pass in both layers. A window
  with no batch fails. Mutation: dropping the batch from the verdict made September pass.
- **connector-status, source and sink.** Live: both RUNNING, pass. Against a mock
  Connect API the previous code passed with the sink PAUSED, with its task FAILED, with
  zero tasks, with one of two tasks paused, and with the sink unregistered; the current
  code fails the first four and answers 503 for the fifth. Mutation: removing the sink
  from the required list made the paused-sink case pass. The live sink connector was not
  paused, so other work on the stack was not disturbed.
- **freshness, heartbeat.** Live table empty: 503. On a scratch copy of the heartbeat
  table: a 20s-old beat passed, a 301s-old beat failed, a second source 2000s stale
  failed. Mutation: taking the heartbeat out of the verdict made the stale case pass.
- **events.** August: 57 matched, counts by status and stage, alerts flagged with
  severity and error summary; `status`, `stage`, `alerts_only`, `limit` filters; a
  straddling range matched nothing; missing parameters, `unscoped` with an interval and
  an unknown status gave 422. `unscoped=true` against a scratch copy of the event table
  holding NULL-interval rows returned exactly those rows.
- **Startup guard.** Current compose environment (four variables carrying `nfl_check`,
  no events DSN): exit 78 naming each mismatch. Per-check DSNs: starts. One login reused
  by two endpoints: exit 78. S3 role file without `shared_with`: exit 78. Mutation:
  disabling the principal comparison let five variables carrying `nfl_check` start.
- **Column grants.** As each per-check role: the columns the check reads were readable;
  `message`/`metrics` (lineage), `payment_ref`/names/amounts (reconcile), `raw_row`
  (quality), business columns (freshness) and other schemas were refused; writes were
  refused, also with read-write forced.

First round (earlier code, same day; not re-run in the fix round):

- s3-marker pass, manifest pass (2 layers, 4 files, 528 declared rows), parquet-rowcount
  pass (silver 460 = 460, gold 68 = 68), reporting/file pass (3736 bytes, sha256 matches
  metadata), delivery/sftp pass (3736 bytes, sha256 matches source), lag pass.
- Mutation, s3-marker: on a fixture interval, deleting `_SUCCESS` turned pass into fail
  and re-creating it turned it back.
- Mutation, delivery: nothing uploaded → fail; `.tmp` only → fail ("never renamed");
  renamed → pass; one byte changed → fail (checksum); restored → pass.
- Lag branches: with a 1s budget and an uncaptured binlog event pending, first call 503,
  second call 4s later fail ("STALLED"); with the normal budget the same situation
  resolved to pass with lag 0 once the offset committed.
- Role boundaries: `nfl_check_replication_lag` SELECT refused (1142) and `SHOW BINLOG
  EVENTS` refused (1227); SFTP checker write and mkdir refused, password login refused;
  a wrong pinned host key refused (`BadHostKeyException`).

## Not established

Four items below were true when the shim fix round ended and have since been closed on
the running compose service (2026-10-04 UTC); they are kept, marked, so the record of what
that round did and did not prove stays intact.

- **Closed: per-check roles.** The roles the infra layer creates were compared with the
  role files: the live column grants of all five `nfl_check_*` roles equal the `grants`
  lists exactly, with no privilege other than SELECT.
- **Closed: live heartbeat.** `/checks/ingestion/freshness` answers 200 `ok: true` on the
  heartbeat produced by the replication path; it went red with the live sink paused for
  over 300 s (infra and Bruno fix rounds).
- **Closed: `unscoped=true` on live rows.** Proven 2026-10-04 on the live event table by
  Bruno `90-chaos/63`: a `{}`-conf run wrote a NULL-interval alert row and
  `/checks/events?unscoped=true` returned it. This records that proof; it says nothing
  about what the table holds now (a `reset.sh` empties it).
- **Closed: paused sink on the live connector.** Bruno `90-chaos/72` to `/76` pause the live
  JDBC sink and `connector-status` goes red naming it. A FAILED sink task is still only
  proven against the mock.

As recorded at the end of the shim fix round:


- The five per-check Postgres roles used for the fix-round proof were temporary and were
  dropped afterwards. Whether the roles the infra layer creates carry exactly the
  declared grants is not verified by the shim.
- The heartbeat pass/fail branches were proven on a scratch table, not on a heartbeat
  produced by the replication path; when the fix round ran, the live table was empty.
- `unscoped=true` was proven on a scratch table; the live event table held no
  NULL-interval row at the time.
- A paused or failed sink was simulated with a mock Connect API, not produced on the
  live connector.
- No S3 identity or S3 scope is proven locally: floci accepts any key. Passing S3 checks
  prove reachability and content only. No Kafka Connect scope is enforced locally either.
- The `aws_iam_policy` documents have not been applied to an AWS account.
- A genuinely stalled source connector was not produced; the STALLED verdict was reached
  by lowering the budget to 1s while an offset commit was pending.
- There is no end-to-end lag measurement in seconds. `replication/lag` is the MySQL to
  Kafka hop only; end-to-end is judged as heartbeat staleness, which is bounded below by
  the beat interval.
- Nothing was run against real Hawks MySQL, real DMS, real S3 or AWS Transfer Family.
