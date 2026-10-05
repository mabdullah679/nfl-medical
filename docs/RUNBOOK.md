# Runbook

How to start the NFL local harness, run the Bruno collection, read a failure, rerun an interval, and
inject each failure. Every command here was run on 2026-10-04 (UTC) against this tree unless a line says
otherwise.

All commands run from the repository root unless they start with `cd`.

## 1. Before you start

| Need | Notes |
|---|---|
| Docker with `docker compose` v2 or later | measured with Docker 29.8.0, compose v5.5.1 |
| `python3`, `curl`, `lsof`, `ssh-keygen` | `init-env.sh` and `up.sh` stop with a named error if one is missing |
| floci answering on `http://localhost:4566` | the local AWS emulator. It is **not** part of this project. Nothing here starts or stops it. |
| `bru` CLI, for command-line runs | measured with 4.2.0 |

Other projects share this machine. Do not touch containers named `sso-*`, `claimsdemo-*`, `slate-*`,
`local-cloudify-*`, `pl-*` or `floci`, or volumes named `avapuck_*` or `local-cloudify_*`. Never run
`docker volume prune` or `docker system prune`. The scripts below act on the compose project `nfl` only.

The env file is `infra/nfl.env`. Never print it. To use a value in a shell, load the file without
echoing it:

```bash
set -a; . infra/nfl.env; set +a
```

## 2. Start from nothing

```bash
curl -fsS http://localhost:4566/_localstack/health
scripts/init-env.sh
scripts/up.sh
```

1. The `curl` confirms floci answers. If it does not, stop: start floci first, outside this project.
2. `scripts/init-env.sh` writes `infra/nfl.env` with freshly generated secrets (never printed) and the
   SFTP host keys and checker key pair under `infra/keys/sftp/`. Run it once. It refuses to overwrite an
   existing env file.
3. `scripts/up.sh` creates the S3 bucket if absent, builds and starts the nine containers, waits for
   health, applies schemas, roles and grants, registers both connectors, and calls every check once to
   prove each identity can log in. **Exit 0 and the closing line "Stack is up" mean all of that
   happened.** Anything else is a failure; see section 5.

Do not start the stack with a bare `docker compose up`. It skips the env file, the bucket, the grants
and the connectors.

Measured for this document: `reset.sh -y` 5 s, cold `up.sh` 104 s and exit 0 (about 60 s of that is
waiting for Kafka Connect's first offset commit), `down.sh` 6 s, warm `up.sh` 38 s and exit 0. The
normal Bruno run on that cold stack was 100 of 100 requests and 309 of 309 tests, and the August checks
were still green after the stop and start. `init-env.sh` was not re-run for this document (the existing
env file was kept); the infra fix round measured it at 1 s on a tree with no env file.

After `up.sh`, the pipeline has not run yet: staging holds the seeded rows, utm and S3 are empty. Run
the collection (section 3) or trigger the DAGs yourself (section 6).

### Stop

```bash
scripts/down.sh
```

Stops and removes the containers. The six named volumes stay, so data survives. `scripts/up.sh` brings
it back.

### Reset to a true cold start

```bash
scripts/reset.sh -y
scripts/up.sh
```

`reset.sh` removes the `nfl` volumes (MySQL, Postgres, Kafka, Airflow metadata and logs, SFTP uploads)
and deletes the pipeline's objects from the bucket: keys under `df_*/`, `reports/`, `_tmp/` and
`_fixtures/` only. Without `-y` it asks you to type `reset`. `scripts/reset.sh -y --up` does both steps.
It does not touch floci itself, `infra/nfl.env` or `infra/keys/`. If floci is unreachable, `reset.sh`
fails and says the S3 objects were not deleted.

To generate new credentials as well, reset first, then:

```bash
scripts/init-env.sh --force
scripts/up.sh
```

`--force` on a stack that still has its volumes makes every database reject the new passwords. That
flag was not run in the fix round; deleting the env file and running plain `init-env.sh` was.

## 3. Run the Bruno collection

The collection is the folder `bruno/`. Folders run in this order; `90-chaos` is separate.

```
00-preflight  10-replication  20-ingestion  30-extraction  40-reporting  50-delivery  60-observability
```

### Command line

```bash
cd bruno
set -a; . ../infra/nfl.env; set +a

# normal run: everything except chaos
bru run -r --exclude-tags chaos --env local \
  --env-var "airflow_password=${AIRFLOW_ADMIN_PASSWORD}" \
  --env-var "aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

# chaos, on its own, after a green normal run (about 2 minutes)
bru run 90-chaos --env local \
  --env-var "airflow_password=${AIRFLOW_ADMIN_PASSWORD}" \
  --env-var "aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

# one folder
bru run 20-ingestion --env local \
  --env-var "airflow_password=${AIRFLOW_ADMIN_PASSWORD}" \
  --env-var "aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"
```

Use bash for these. Do not add `--reporter-json`, `--reporter-html` or `--reporter-junit` unless you
delete the output afterwards: Bruno writes the Airflow `Authorization` header into those files.

### Bruno desktop app

1. Open Collection, choose the `bruno/` folder.
2. Pick the environment `local`.
3. In the environment settings fill the two secrets: `airflow_password` (the value of
   `AIRFLOW_ADMIN_PASSWORD` in `infra/nfl.env`) and `aws_secret_access_key` (`test`).
4. Run a folder, or Send requests top to bottom inside one.

The desktop app was not exercised; every measured run used the CLI, which executes the same files.

### What a normal run does to the stack

It triggers `nfl_ingestion`, `nfl_extraction` and `nfl_reporting` for the clean interval (August 2026),
then triggers `nfl_reporting` once more with `force_redeliver: true`, which re-uploads the same bytes to
SFTP. Reruns are idempotent: same row counts, same Parquet, same report checksum, one file on SFTP.

### Last measured result

2026-10-04 UTC, bru 4.2.0, re-run for this document, in this order:

| Run | Requests | Tests | Seconds |
|---|---|---|---|
| normal (seven folders named) | 100 of 100 | 309 of 309 | 51 |
| `90-chaos` | 57 of 57 | 160 of 160 | 127 |
| normal (`-r --exclude-tags chaos`) | 100 of 100 | 309 of 309 | 49 |

After chaos, checked outside Bruno: both connectors and their tasks RUNNING, the August `_SUCCESS`
marker present with 0 bytes, nine containers healthy.

Then `reset.sh -y`, `up.sh` and the normal run once more on the cold stack: 100 of 100, 309 of 309.

Since then one test that could not fail was removed from `60-observability/02` on 2026-10-05; the normal run measured that day, chaos not re-run, is 100 of 100 requests and 308 of 308 tests.

## 4. Reading a result

**Shim requests** (`/checks/...` on port 8096) answer in one of four ways:

| Answer | Meaning |
|---|---|
| HTTP 200, `ok: true` | The check ran and passed. |
| HTTP 200, `ok: false` | The check ran and failed. This is a verdict: read `summary`, `measured`, `threshold`. |
| HTTP 503 | The check **could not run** (dependency down, credential refused, a needed value missing). Never a pass. Read `error`, then `docker logs nfl-shim`. |
| HTTP 422 | Bad parameters; the check did not run. |

On `/checks/events`, `ok` means only "matching rows exist". The pipeline verdict is `/checks/lineage`.

**DMS stub** (port 8098): every response carries `_stub: true` and the header `X-NFL-Stub: true`. `Status`
is derived from the Debezium connector's state. A field with no local source is `null` and tagged
`unknown`; that is deliberate. `/health` returns 503 when the stub cannot reach Kafka Connect or
Postgres.

**"... run state (waits until terminal)"** requests poll Airflow until the run finishes. If the run is
still going after `poll_timeout_seconds` (240 locally) the tests fail with "still running after N ms".

| Symptom | What it means | First action |
|---|---|---|
| `00-preflight` red | The environment is wrong, not the pipeline | `docker ps --filter name=nfl-`; re-run `scripts/up.sh` and read its output |
| `10-replication/06` to `/08` or `/10` red | A connector or task is not RUNNING | `curl -s localhost:8097/connectors/<name>/status`; read the task `trace` |
| `10-replication/09` red | The Debezium source is not keeping up with the binlog, or is paused | same |
| `20-ingestion/14` red, "heartbeat ... STALE" | Nothing has landed in staging through the sink for over 300 s | check both connectors; the source can be healthy while the sink is down |
| Ingestion run failed at `pre_check` | A connector is not RUNNING, the heartbeat is stale, or utm is unreachable | read the task log; fix replication before rerunning |
| Ingestion run failed at `is_failed_gate` | Rows were rejected or counts do not reconcile | `/checks/ingestion/quality` for the interval lists rejects by table and reason. This is the gate doing its job. |
| Extraction run failed at `pre_check_ingestion` | The interval's ingestion batch is missing or FAILED | run or fix ingestion for that interval first |
| Extraction `is_utm` retrying | The batch is not SUCCEEDED yet, or utm does not hold the rows it loaded | wait, or check the ingestion run |
| Reporting failed at `wait_for_success_marker` after 4 tries | `_SUCCESS` or `manifest.json` is absent for the interval | run extraction for that interval |
| Reporting failed at `is_process_failed` | The stored report is empty, does not match its checksum, or its trailer disagrees with the manifest | treat as a data defect; do not rerun blindly |
| Reporting failed at `deliver_sftp`, "already delivered with different bytes" | The file on SFTP differs from the report | find out why before using `force_redeliver` |
| `50-delivery` reports a `.tmp` file present | Verification failed before the rename | compare size and checksum; the guard worked |
| Any run failed at `validate_conf` | The conf has no valid interval | both bounds must be UTC midnights, end after start |

Airflow task logs: the UI at `http://localhost:8095` (user from `AIRFLOW_ADMIN_USER`), or
`docker logs nfl-airflow`. There are no Hop or Spark logs; those engines are not in the stack.

**A check that could not run is not a check that passed.** A timeout, a 503 or a refused request leaves
the requirement unverified.

## 5. When `up.sh` fails

`up.sh` exits non-zero and ends with `stack is up but these need attention: ...` followed by labels.

| Label or message | Meaning | Action |
|---|---|---|
| `missing infra/nfl.env` | not initialised | `scripts/init-env.sh` |
| `infra/nfl.env is missing: <NAMES>` | the env file predates variables added later | `scripts/reset.sh -y`, `scripts/init-env.sh --force`, `scripts/up.sh` |
| `port N (...) is in use by something else` | another project holds one of our ports | free it there, or set the matching `NFL_*_PORT` in `infra/nfl.env`; never take a port from the forbidden list in `docs/ARCHITECTURE.md` |
| `floci:unreachable` | the emulator does not answer on 4566 | start floci outside this project, then `scripts/up.sh` again. The S3 probes are reported as NOT RUN, not as passed. |
| `could not ensure bucket` | floci answered but the bucket could not be created | check floci |
| `these services did not become healthy: ...` | a container never reached healthy; the last 30 log lines are printed | read them; `docker logs nfl-<name>` |
| `connector:<name>` | the connector did not reach RUNNING | `curl -s localhost:8097/connectors/<name>/status` |
| `shim:<label>` ... `COULD NOT RUN (HTTP 503 ...)` | that check identity could not log in or reach its store; the response body is printed | fix what the body names. On a cold start the lag and freshness probes wait up to 120 s before giving up. |
| `dms-stub`, `dms-stub:upstreams` | the stub cannot reach Kafka Connect or Postgres | `curl -s localhost:8098/health` |

Databases rejecting every login after `init-env.sh --force`: the volumes still hold the old passwords.
`scripts/reset.sh -y`, then `scripts/up.sh`.

## 6. Run or rerun an interval by hand

A rerun is a new run with the same conf. It reuses the interval's batch, rewrites identical S3 objects
and skips delivery if the file on SFTP already matches.

```bash
set -a; . infra/nfl.env; set +a
AF=http://localhost:8095/api/v1
CONF='{"interval_start":"2026-08-01T00:00:00Z","interval_end":"2026-09-01T00:00:00Z"}'
RUN="manual_$(date -u +%Y%m%dT%H%M%SZ)"

curl -s -u "${AIRFLOW_ADMIN_USER}:${AIRFLOW_ADMIN_PASSWORD}" -X POST "$AF/dags/nfl_ingestion/dagRuns" \
  -H 'Content-Type: application/json' -d "{\"dag_run_id\":\"$RUN\",\"conf\":$CONF}"

curl -s -u "${AIRFLOW_ADMIN_USER}:${AIRFLOW_ADMIN_PASSWORD}" "$AF/dags/nfl_ingestion/dagRuns/$RUN"
curl -s -u "${AIRFLOW_ADMIN_USER}:${AIRFLOW_ADMIN_PASSWORD}" "$AF/dags/nfl_ingestion/dagRuns/$RUN/taskInstances"
```

Wait for `"state": "success"` before triggering the next DAG, in this order: `nfl_ingestion`,
`nfl_extraction`, `nfl_reporting`. The DAGs are not chained; each one checks that the stage before it
succeeded for the same interval and fails if it did not.

- The window is half-open: `interval_end` is the first midnight after the last day. August 2026 is
  `2026-08-01T00:00:00Z` to `2026-09-01T00:00:00Z`. The shim takes the same interval as
  `interval_start=20260801&interval_end=20260831`.
- To send a report again when it is already on SFTP, add `"force_redeliver": true` to the
  `nfl_reporting` conf. Without it the delivery step skips, and fails if the delivered bytes differ.
- September 2026 is seeded with bad rows and is meant to fail at `is_failed_gate`. Rerunning it does not
  fix it.
- Before rerunning a data failure, find out why it failed. The same input fails the same way.

Check an interval afterwards:

```bash
curl -s "localhost:8096/checks/lineage?interval_start=20260801&interval_end=20260831"
curl -s "localhost:8096/checks/events?interval_start=20260801&interval_end=20260831&status=failed"
```

## 7. Failure injection

`90-chaos` holds seven scenarios. Each request that breaks something has `INJECT` in its name and is
followed by one with `RESTORE`. A folder run always reaches the restores, because the CLI does not stop
at a failed test. Run it only after a green normal run.

"Alert" below means a row in `observability.pipeline_event` with `status='failed'` and
`metrics.alert=true`, visible through `/checks/events?...&status=failed`. **No scenario proves that a
person is notified**; no alert channel exists (ALR-03 in `docs/COVERAGE.md`).

| Scenario | Inject | What goes red | Expected alert | Restore |
|---|---|---|---|---|
| A. Source connector stops | `03` pauses the Debezium source connector | `04` connector-status names the source; `05` source capture lag; `06` the DMS stub reads `stopped` with no stop reason | none: no pipeline run is involved | `07` resumes; `08`, `09` green |
| B. Bad data in the interval | `10` triggers `nfl_ingestion` for September | `11` run failed; `12` only `is_failed_gate` failed and nothing after it ran; `17`, `19` quality red; `18` lineage red | one alert, stage `ingestion`, step `is_failed_gate`, severity `critical`, error summary naming the rejects (`13`, `20`) | none needed; September is meant to stay failed |
| C. Client error | `25` asks for a run that does not exist | 404 | none | none |
| D. Extraction output missing | `35` deletes the August `_SUCCESS` marker, `37` triggers `nfl_reporting` | `36` marker check; `38` run failed; `39` `wait_for_success_marker` failed after every retry, nothing downstream ran | three `retry` events, then one alert, stage `reporting`, step `wait_for_success_marker`, naming the marker (`40`) | `41` puts the marker back, `44` triggers a clean reporting run; `42`, `43`, `45`, `46` green |
| E. Operator sends DMS Stop | `51` StopReplicationTask on the stub | nothing, by design: `51`, `52` show `Status` stays `running` because the connector is still RUNNING; the command is recorded as intent only | none | `53` StartReplicationTask; `54` |
| F. Invalid conf | `60` triggers `nfl_ingestion` with conf `{}` | `61` run failed; `62` only `validate_conf` failed | one alert with NULL interval bounds, step `validate_conf`, found with `/checks/events?unscoped=true` (`63`) | none needed |
| G. JDBC sink stops | `72` pauses the JDBC sink connector | `73` connector-status names the sink; `74` source capture lag stays green, because it is not end-to-end | none within the scenario | `75` resumes; `76` green; `77` a beat written after the resume has landed |

Numbers are the request sequence in `bruno/90-chaos/`. `55` and `56` check that the stub rejects an
unknown operation (404) and an unknown task ARN (400).

Two guards on scenario D: `35` and `41` refuse to send unless the marker key was captured from a real
extraction run, and `35` also requires that `34` just proved the marker check green for that key.

Not in the collection, measured by hand in the infra fix round: with the sink paused for over 300 s,
`/checks/ingestion/freshness` goes red; with beats stopped while both connectors stay RUNNING, an
ingestion run fails at `pre_check` with "replication is stale" and writes an alert.

Not injected by anything: a connector task in state FAILED, an extraction failure, an
`is_process_failed` failure, an SFTP failure mid-transfer.

### What chaos leaves behind

Failed runs in the Airflow history, and alert rows: for September (B), for no interval (F), and for
August (the one failed `bruno_chaos_marker_*` run of D). Events are append-only. `60-observability/12`
tolerates the August alerts by that run id prefix only, and requires the alert count to equal the number
of such runs' alerts; an alert from any other run, the restore run `bruno_chaos_restore_*` included, fails it. A `reset.sh` clears them.

### If a chaos run is interrupted

Restore by hand, then run the normal run:

```bash
curl -s -X PUT localhost:8097/connectors/nfl-hawks-mysql-source/resume
curl -s -X PUT localhost:8097/connectors/nfl-staging-jdbc-sink/resume
```

For the marker, send `90-chaos` requests `30` to `34` and then `41` in the Bruno app, or simply rerun
extraction for August (section 6): `publish` writes the marker again.

Confirm nothing is left broken:

```bash
curl -s localhost:8097/connectors/nfl-hawks-mysql-source/status
curl -s localhost:8097/connectors/nfl-staging-jdbc-sink/status
curl -s localhost:8096/checks/replication/connector-status
curl -s "localhost:8096/checks/extraction/s3-marker?interval_start=20260801&interval_end=20260831"
docker ps --filter name=nfl- --format '{{.Names}} {{.Status}}'
```

Both connectors and their tasks RUNNING, both checks `ok: true`, nine containers healthy.

## 8. Shut down

```bash
scripts/down.sh
```

Leave floci running. `down.sh` lists the remaining `nfl` containers (expect none) and the volumes kept.
