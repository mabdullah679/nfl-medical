# NFL Data Platform: Bruno collection

Every observable fact about the pipeline is one request you can Send. Requests that need a value from an
earlier response get it from that response: the earlier request captures it in its post-response script and
the later request uses it as `{{variable}}`. Nothing is pasted by hand.

Checks that are not HTTP by nature (SQL counts, MySQL binlog position, Parquet row counts, SFTP file
checksum, the Operations alert rows) go through the check shim on port 8096, so they are Send buttons too.

## Open it in Bruno desktop

1. Bruno > Open Collection > choose this `bruno/` folder.
2. Top right, pick the environment `local`.
3. Environments > Configure > `local`: fill the two secrets. They are declared in `vars:secret`, so Bruno keeps
   the values in its own encrypted store and never writes them into this folder.
   - `airflow_password`: the value of `AIRFLOW_ADMIN_PASSWORD` in `infra/nfl.env`
   - `aws_secret_access_key`: `test` for floci (floci accepts any value)
4. Collection settings > Scripts: the default "Safe Mode" sandbox is enough. Scripts use only `bru.*`, `btoa`
   and the two local files `lib/poll.js` and `lib/guards.js`.
5. Send requests top to bottom inside a folder, or right-click a folder > Run.

Each request has a Docs tab: what it proves, what it captures, what it consumes, what to look at when it fails.

## Environments

| Variable | `local` | `aws` |
|---|---|---|
| `shim_url` | `http://localhost:8096` | placeholder: where the shim is deployed |
| `airflow_url`, `airflow_user` | `http://localhost:8095`, `nfladmin` | placeholder |
| `dms_endpoint` | `http://localhost:8098` (the stub) | `https://dms.us-east-1.amazonaws.com` (real DMS) |
| `dms_is_stub` | `true` | `false` |
| `dms_task_identifier` | `NFL-HAWKS-STAGING` | placeholder |
| `s3_endpoint`, `s3_bucket` | floci on 4566, `nfl-data-platform` | `https://s3.us-east-1.amazonaws.com`, placeholder |
| `aws_region`, `aws_access_key_id` | `us-east-1`, `test` | placeholder |
| `connect_url`, `source_connector_name`, `sink_connector_name`, `floci_health_url` | local values | not applicable |
| `interval_start_ts`, `interval_end_ts` | `2026-08-01T00:00:00Z`, `2026-09-01T00:00:00Z` (the clean interval) | same |
| `chaos_interval_start_ts`, `chaos_interval_end_ts` | `2026-09-01T00:00:00Z`, `2026-10-01T00:00:00Z` (seeded bad rows) | same |
| `poll_timeout_seconds`, `poll_interval_ms` | `240`, `2000` | `1800`, `10000` |
| `logs_endpoint`, `log_group_prefix`, `log_ship_wait_ms` | floci on 4566, `/nfl`, `8000` | `https://logs.us-east-1.amazonaws.com`, `/nfl`, `8000` |
| `mail_url` | `http://localhost:8094` (the local Operations inbox) | not applicable |
| secrets | `airflow_password`, `aws_secret_access_key` | same names |

Both files define exactly the same variable names, so the same requests repoint by switching environment.
The DMS requests are identical in both: `POST {{dms_endpoint}}/` with `X-Amz-Target:
AmazonDMSv20160101.<Operation>`, `Content-Type: application/x-amz-json-1.1` and SigV4 signing for service
`dms`. Locally the stub ignores the signature; on AWS it is required.

To run another interval, change the two `interval_*_ts` values. They must be UTC midnights; the window is
half-open, `[start, end)`. The `YYYYMMDD` forms the shim takes are not typed anywhere: they are captured
from the DAG run's own `validate_conf` XComs.

### Tags

| Tag | Requests | Meaning |
|---|---|---|
| `chaos` | 86 | every request in `90-chaos`; breaks things on purpose |
| `local-only` | 44 | exists only on the local stack: Kafka Connect, floci health, DMS stub health, the Operations inbox, chaos scenarios A, E, G and H, and the inbox requests of scenario B |
| `airflow-basic-auth` | 91 | every request that calls the Airflow REST API with HTTP basic auth |

### What the `aws` environment assumes, and what it does not cover

The `aws` values are placeholders. The `aws` environment has NOT been run against a real account; nothing
below is a measured result.

- It assumes the Airflow REST API at `airflow_url` accepts HTTP basic auth (self-managed Airflow, or a
  gateway in front of it). Amazon MWAA does not: it takes a web login token or IAM-signed calls, and no
  request in this collection does that.
- Against an Airflow with basic auth: `--env aws --exclude-tags chaos,local-only`.
- Against MWAA: `--env aws --exclude-tags chaos,local-only,airflow-basic-auth`. That selects 38 requests.
  Not run against MWAA. Run with that selection against the local stack (`--env local`, measured
  2026-10-07) 16 of the 38 pass (11 of the original 31 on 2026-10-05, plus five of the seven added on 2026-10-07: alert delivery, the watcher, and three log requests of `60-observability`). The original 11 are: the shim, S3 and unauthenticated Airflow health requests of `00-preflight`
  (4), the DMS and shim requests of `10-replication` (6), and `20-ingestion/14` (freshness). Freshness is
  the conditional one: its utm assertion passes only while an ingestion run made outside this selection is
  recent, so on a stack where nobody triggered ingestion the count is 10. The other 20, in folders `20` to `60`, cannot work in that
  mode: each of their chains starts with an Airflow request (the trigger, the XComs that carry the interval
  dates and S3 keys), so the shim and S3 requests that follow have nothing to consume and fail. That is a
  gap, not a pass: on MWAA the pipeline folders are not covered by this collection until an MWAA-capable
  auth is added.
- The shim S3 checks and the raw S3 requests prove reachability and content locally, not an IAM identity:
  floci accepts any access key and evaluates no policy.

## Run order

| Folder | Requests | What it covers | Changes state? |
|---|---|---|---|
| `00-preflight` | 9 | every dependency answers: shim (health, check index incl. `/checks/events`), floci, Airflow (health, auth, DAG import errors), Kafka Connect, DMS stub, S3 bucket | no |
| `10-replication` | 10 | DMS task -> ARN -> table statistics; connectors -> status -> task; shim source capture lag; shim connector status for BOTH connectors (source and sink) | no |
| `20-ingestion` | 14 | the canonical Airflow chain for `nfl_ingestion`, then shim reconcile and quality (all three table pairs, plus the batch row), and freshness, which asserts a green end-to-end heartbeat | triggers a run (idempotent rerun) |
| `30-extraction` | 20 | `nfl_extraction` chain, raw S3 list / HEAD / GET, shim marker, manifest, Parquet row counts | triggers a run (rewrites identical output) |
| `40-reporting` | 15 | `nfl_reporting` chain, raw S3 report object and checksum sidecar, shim report file check | triggers a run (delivers only if the file is not there yet) |
| `50-delivery` | 19 | the file AT REST on SFTP (01 to 06), then a forced redelivery that proves the upload path (07 to 19) | triggers a run with `force_redeliver: true`: re-uploads the same bytes, the file's modification time changes |
| `60-observability` | 22 | run history, failed task instances, shim lineage (and two negative lineage tests), shim events and alerts for the clean interval, Airflow audit log; then one attempt as a single thread (`14`), alert delivery (`15`), the watcher (`16`), the Operations inbox (`17`) and the shipped logs in CloudWatch Logs (`18` to `22`) | no (`19` writes one line to the shim's log) |
| `90-chaos` | 86 | deliberate failures, each followed by its restore | yes, see below |

`30` needs a successful `20` for the interval, `40` needs `30`, `50` and `60` need `40`. Each folder is
otherwise self-contained: it captures everything it uses, so any single folder can be run on its own.

### The "wait until terminal" requests

`... run state (waits until terminal)` has a pre-request script that polls the same Airflow endpoint every
`poll_interval_ms` until the run is `success` or `failed`, then sends the real request. In the desktop app the
Send button just takes longer. If `poll_timeout_seconds` passes first, the request is sent anyway and its
tests FAIL with "still running after N ms". A run that has not finished is never reported as a pass.

### No request passes on a red verdict

Every shim request in folders `00` to `60` asserts the verdict it expects. The three that expect
`ok: false` say so in their names and docs:

- `60-observability/09` and `10`: lineage for a sub-interval, and for a range spanning the clean and the
  chaos interval, must be `ok: false` with zero events. These are regression tests for a real shim defect
  (a range match that reported "all stages succeeded" for intervals that never ran).
- `60-observability/12`: alerts for the clean interval. On `/checks/events`, `ok` means only "rows
  matched", so an empty alert list answers `ok: false`, which is the good answer there. The assertion is:
  zero alert rows other than those of `bruno_chaos_marker_*` runs, the run `90-chaos` scenario D fails on
  purpose; the matched alert count must equal the injected count. The restore run `bruno_chaos_restore_*`
  is not excused.

`20-ingestion/14 Shim ingestion freshness` asserts `ok: true`. The verdict rests on the replication
heartbeat (a row Debezium writes in MySQL on a timer, landed in `staging.replication_heartbeat` through
Kafka and the JDBC sink) and on the utm load time. Measured: with the JDBC sink paused for 320 s the
request goes red ("heartbeat ... STALE"); after the resume it is green again.

### 50-delivery: at rest, then the upload

A plain `nfl_reporting` rerun on a warm stack answers `skipped_already_delivered`: the file is already on
SFTP with the right bytes, so nothing is uploaded. Requests 01 to 06 look at that state and are labelled
AT REST: they prove the file that is there matches the S3 source, not that an upload works.

Requests 07 to 19 trigger `nfl_reporting` with `"force_redeliver": true` and assert, for that run:
`delivery_action` is `delivered` or `redelivered_forced` (a skip fails), `delivered` is `true`, the task log
has the upload line, exactly one copy exists, and the shim finds the file under its final name with the
temp name absent, hashing to what the DAG read back and to the S3 source, with a modification time inside
the `deliver_sftp` task's own start and end and newer than the at-rest file.

## 90-chaos

Ten scenarios. Every request that breaks something has `INJECT` in its name and is followed by a request
with `RESTORE` in its name. If any request in between fails, still send the RESTORE. The CLI does not stop
at a failed test, so a folder run always reaches the restores.

| Scenario | Inject | What goes red | Restore | Proof of restore |
|---|---|---|---|---|
| A source connector stops (`local-only`) | `A3` pause the Debezium source | `A4` shim connector-status (source named, sink still healthy), `A5` shim source capture lag, `A6` DMS task reads `stopped` with `StopReason` null | `A7` resume | `A8`, `A9` green |
| B bad data in the interval | `B1` trigger `nfl_ingestion` for the chaos interval | `B2` run `failed`, `B3` only `is_failed_gate` failed, `B4` log shows `OPERATIONS ALERT recorded`, `B8` staging quality red, `B9` lineage red, `B10` quality red in the default (utm) layer because the batch FAILED, `B11` the alert row itself: `alert`, `severity`, `error_summary` | none needed: the chaos interval is designed to be FAILED, the clean interval is untouched | n/a |
| C client error | `C1` GET a run that does not exist | 404 problem document | none needed | n/a |
| D extraction output missing | `D6` delete the S3 `_SUCCESS` marker of the clean interval, then `D8` trigger `nfl_reporting` | `D7` shim marker check, `D9` run `failed`, `D10` `wait_for_success_marker` failed after using every retry and nothing downstream ran, `D11` one `retry` event per retry and one alert row naming the marker | `D12` PUT the empty marker back, `D15` trigger a clean `nfl_reporting` run | `D13` marker green, `D14` raw HEAD 200, `D16` run `success`, `D17` lineage green |
| E operator sends DMS Stop (`local-only`) | `E2` StopReplicationTask | nothing, and that is the point: `E2` and `E3` assert `Status` stays `running` because the connector is still RUNNING; the command shows only in `_stub_intent` | `E4` StartReplicationTask `resume-processing` (resets the recorded intent) | `E5` reads `running`, last command Start. `E6` unknown operation 404, `E7` unknown task ARN 400 |
| F invalid conf | `F1` trigger `nfl_ingestion` with conf `{}` | `F2` run `failed`, `F3` only `validate_conf` failed, `F4` the alert row with NULL interval bounds through `/checks/events?unscoped=true` | none needed: nothing was read or loaded | n/a |
| G JDBC sink stops (`local-only`) | `G3` pause the JDBC sink | `G4` shim connector-status (sink named, source still healthy). `G5` shows source capture lag stays GREEN: it is not end-to-end, and says so | `G6` resume | `G7` green, `G8` a heartbeat written after the resume has landed in staging |
| B, continued: the alert itself | none (uses the failed run of `B1`) | nothing: these prove the alert is usable. `B12` severity, log link and correlation id on the row; `B13` follows the link and gets the failed task's log; `B14` delivered within 60 s; `B15`, `B16` (`local-only`) the email in the inbox, with run id, stage, interval, error summary, severity and link | none needed | n/a |
| J extraction for a failed interval | `J1` trigger `nfl_extraction` for the chaos interval | `J2` run `failed`, only `pre_check_ingestion` failed, nothing downstream ran; `J3` one alert at stage extraction, delivered | none needed: nothing was read or written | n/a |
| H a connector stops and nobody triggers anything (`local-only`) | `H2` pause the Debezium source | `H3` the watcher's own sample is `critical` within a minute; `H4` its alert, delivered; `H5` the email | `H6` resume | `H7` watcher green, `H8` the incident thread is alert then recovery, both delivered; `H9` the watcher's log lines in CloudWatch Logs |
| K the report disagrees with the manifest | `K2` overwrite the manifest's gold row count, then `K3` trigger `nfl_reporting` | `K4` run `failed`, only `is_process_failed` failed, `deliver_sftp` never ran; `K5` the alert names both row counts; `K6` delivered | `K7` trigger `nfl_extraction` (republishes the true manifest), `K10` trigger `nfl_reporting` | `K9` the manifest equals the one captured before, `K11` run `success`, `K12` lineage green |

Guards on scenario K: `K2` (put) has a pre-request script that throws, so nothing is sent, unless `K1`
captured the manifest stored under exactly that `<prefix>/manifest.json` key. The restore does not write the
manifest back from this collection; it reruns extraction, whose output is deterministic, and `K9` compares.

Scenarios A and G pause a connector for a few seconds, which is shorter than one watcher tick (15 s), so
they normally raise no watcher alert. Scenario H holds the pause until the watcher has seen it.

Guards on scenario D: `D6` (delete) and `D12` (put) each have a pre-request script that throws, so nothing
is sent, unless `chaos_marker_key` is a captured `<prefix>/_SUCCESS` key. `D6` also requires that `D5`
proved the marker check green for that same key in this pass. Measured: sent on their own, both are refused
and the bucket is unchanged; with the guard removed, the delete goes to the literal text
`{{chaos_marker_key}}` and S3 answers 204.

What chaos leaves behind on purpose: failed runs in the Airflow history, and alert rows in
`observability.pipeline_event` for the chaos interval, for no interval (F), and for the clean interval (the
failed `bruno_chaos_marker_*` and `bruno_chaos_gate_*` reporting runs of scenarios D and K, which
`60-observability/12` recognises by their run ids), the watcher's alert and recovery rows of scenario H, and one
email per alert in the Operations inbox. Events and emails are append-only; no request deletes them.

On the stub, scenario E changes only the stub's recorded intent; the stub says so in `_stub_action`. Against
real AWS, `E2` would really stop replication, which is why it is `local-only`.

## CLI

Run from this folder. The secrets are read from `infra/nfl.env` into the shell and passed with `--env-var`;
they are never written into the collection.

```bash
cd bruno
set -a; . ../infra/nfl.env; set +a

# normal run: everything except chaos
bru run -r --exclude-tags chaos --env local \
  --env-var "airflow_password=${AIRFLOW_ADMIN_PASSWORD}" \
  --env-var "aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

# the same, naming the folders
bru run 00-preflight 10-replication 20-ingestion 30-extraction 40-reporting 50-delivery 60-observability \
  --env local \
  --env-var "airflow_password=${AIRFLOW_ADMIN_PASSWORD}" \
  --env-var "aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

# chaos, on its own, after a green normal run (about 2 minutes: scenario D waits out the gate's retries,
# scenario G waits for a heartbeat)
bru run 90-chaos --env local \
  --env-var "airflow_password=${AIRFLOW_ADMIN_PASSWORD}" \
  --env-var "aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

# one folder or one request
bru run 20-ingestion --env local --env-var "airflow_password=${AIRFLOW_ADMIN_PASSWORD}" --env-var "aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"
```

After a chaos run, confirm outside Bruno that nothing was left broken:

```bash
curl -s localhost:8097/connectors/nfl-hawks-mysql-source/status   # connector and task RUNNING
curl -s localhost:8097/connectors/nfl-staging-jdbc-sink/status      # connector and task RUNNING
curl -sI --aws-sigv4 "aws:amz:us-east-1:s3" --user test:test \
  http://localhost:4566/nfl-data-platform/df_20260801_20260831/_SUCCESS   # HTTP 200, content-length 0
docker ps --filter name=nfl- --format '{{.Names}} {{.Status}}'      # 12 containers, all healthy
```

Do not use `--reporter-json` / `--reporter-html` / `--reporter-junit` output files without deleting them
afterwards, and never commit them: Bruno writes the request headers into them, including the Airflow
`Authorization` header.

## Last measured result

Measured 2026-10-04 UTC with bru 4.2.0 against the local stack, freshly cold-started with rotated
credentials. The three runs were made in this order, back to back.

| Run | Command | Requests | Tests |
|---|---|---|---|
| normal, before chaos | `bru run 00-preflight ... 60-observability --env local` | 100 of 100 passed | 309 of 309 |
| chaos | `bru run 90-chaos --env local` | 57 of 57 passed | 160 of 160 |
| normal, after chaos | `bru run -r --exclude-tags chaos --env local` | 100 of 100 passed | 309 of 309 |
| normal, 2026-10-05, after one test was removed from `60-observability/02`; chaos not yet run on this volume | `bru run -r --exclude-tags chaos --env local` | 100 of 100 passed | 308 of 308 |
| **2026-10-07, after `reset.sh -y` and a cold `up.sh`** | | | |
| normal, before chaos | `bru run -r --exclude-tags chaos --env local` | 109 of 109 passed | 344 of 344 |
| chaos | `bru run 90-chaos --env local` | 86 of 86 passed | 233 of 233 |
| normal, after chaos | `bru run -r --exclude-tags chaos --env local` | 109 of 109 passed | 344 of 344 |

The 2026-10-04 and 2026-10-05 rows are history: they were measured before the observability requests
existed. After the 2026-10-07 chaos run, checked outside Bruno: both connectors RUNNING, the `_SUCCESS`
marker HEAD 200, `/checks/monitor` and `/checks/alerts/delivery` both `ok: true`, slowest alert delivery
11.5 s.

After the chaos run of 2026-10-04, checked outside Bruno: both connectors RUNNING with their task RUNNING; the
`_SUCCESS` marker answers HEAD 200 with 0 bytes and the bucket holds the same 8 keys; all 9 `nfl-*`
containers healthy; the DMS stub reads `running`.

Proofs that the new assertions can fail (each run against the live stack; mutations were made in a scratch
copy of this folder, never here):

| What was changed | Result |
|---|---|
| `D6` and `D12` sent alone, no marker key captured | both refused in the pre-request script, bucket unchanged |
| guard removed from `D6` | DELETE sent to the literal placeholder key, S3 answered 204 |
| `D5` baseline made red (asked about another interval) | `D6` refused: "D5 did not prove the marker check green" |
| `force_redeliver` removed from `50-delivery/07` | 6 tests red: conf, log line, `delivery_action`, `delivered`, `force_redeliver` XCom, file modification time |
| chaos run-id prefix changed in `60-observability/12` | red: the alert of the injected run is reported as unexplained |
| JDBC sink paused for 320 s, then `20-ingestion/14` | red: heartbeat 336 s old against a 300 s limit; green again 3 s after the resume |
| **2026-10-07** | |
| `record_event` made to write no correlation id | `60-observability/14` red: verdict and "joins a run of each of the three DAGs" |
| alert log path given the wrong attempt number | `90-chaos/21` and `22` red; Airflow answered 200 with a "not found" text, the content tests caught it |
| `nfl-watch` made to record `sent` without sending | `90-chaos/23` stayed green (the row says sent), `90-chaos/24` red: no message in the inbox |
| `pre_check_ingestion` made to accept a FAILED batch | `90-chaos/28`, `29` red |
| `is_process_failed` made to never fail | `90-chaos/90`, `91`, `92` red |
| mail server stopped, then a failure triggered (by hand) | `/checks/alerts/delivery` pending, then `undelivered` and red; `sent` on the retry after the server came back |
| `nfl-watch` stopped (by hand) | `/checks/monitor` red with `watcher_alive: false` after 45 s; green 25 s after restart |
| `nfl-airflow` frozen with `docker pause` for 70 s (by hand) | orchestrator probe critical after three unanswered ticks, emailed; recovery emailed after unpause |
| watcher run with a 25 s and a 10 s heartbeat limit (by hand) | ok to warning, warning to critical, back to ok; each change emailed |

The code mutations of 2026-10-07 were made in the working tree, reversed by the exact opposite edit, and
`git diff` was empty afterwards.

Not established:

- The requests added on 2026-10-07 against real CloudWatch Logs. On floci a filter pattern is a plain
  substring match and a missing log group answers 200 with no events; AWS does neither.

- The `aws` environment against a real account. Locally, the MWAA tag selection
  (`--exclude-tags chaos,local-only,airflow-basic-auth`) picks 31 requests: 11 pass (4 in `00-preflight`,
  6 in `10-replication`, the freshness check) and 20 fail for lack of captured values, as described above
  (re-measured 2026-10-05: 31 requests, 11 passed, 20 failed).
- The Bruno desktop app. The proof was run with the CLI, which executes the same files and scripts.
- Scenario G does not wait for the heartbeat to go stale with the sink paused (that takes over 300 s); the
  320 s proof above was run by hand, not by a request in the collection.
- A FAILED connector task (as opposed to a paused connector) is not injected by any scenario.
- The DMS error type names asserted in `E6` and `E7` match the stub; they were not compared with real AWS.

## Chaining map

Request -> variables it captures -> the requests (by number, same folder) that consume them. Generated from
the request files (every `bru.setVar` against every `{{variable}}` and `bru.getVar`). Variables are never
shared across folders, so each folder runs alone. The same pass found 0 variable references without an
earlier producer in the same folder or a definition in both environments.

### 00-preflight (9 requests, 23 tests)

| Request | Captures | Consumed by |
|---|---|---|
| 02 Shim check index | `shim_check_count` | informational, not consumed |
| 05 Airflow auth and version | `airflow_version` | informational, not consumed |

### 10-replication (10 requests, 34 tests)

| Request | Captures | Consumed by |
|---|---|---|
| 01 DMS DescribeReplicationTasks | `dms_task_arn`<br>`dms_task_status` | 02<br>informational, not consumed |
| 02 DMS DescribeTableStatistics | `dms_tables_landed` | informational, not consumed |
| 05 Kafka Connect list connectors | `connector_name`<br>`sink_connector_name_live` | 06, 07<br>08 |
| 06 Kafka Connect source status | `connector_task_id` | 07 |

### 20-ingestion (14 requests, 48 tests)

| Request | Captures | Consumed by |
|---|---|---|
| 01 Airflow version | `airflow_version` | informational, not consumed |
| 02 List DAGs | `ingestion_dag_id` | this request |
| 03 Trigger nfl_ingestion | `new_dag_run_id`<br>`correlation_id`<br>`ingestion_dag_run_id` | this request<br>this request<br>04, 05, 06, 07, 08, 09, 10, 11 |
| 04 nfl_ingestion run state (waits until terminal) | `ingestion_poll_count`<br>`ingestion_poll_waited_ms`<br>`ingestion_run_state` | informational, not consumed<br>this request<br>informational, not consumed |
| 05 nfl_ingestion task instances | `ingestion_task_id`<br>`ingestion_try_number` | 06, 07<br>06, 07 |
| 08 XCom keys of load_staging_to_utm | `ingestion_xcom_key` | 09 |
| 09 XCom load_staging_to_utm.load_by_table | `ingestion_load_by_table` | 12 |
| 10 XCom validate_conf.interval_start_date | `interval_start_date` | 12, 13 |
| 11 XCom validate_conf.interval_end_date | `interval_end_date` | 12, 13 |
| 14 Shim ingestion freshness | `freshness_ok`<br>`freshness_heartbeat_age_seconds` | informational, not consumed<br>informational, not consumed |

### 30-extraction (20 requests, 62 tests)

| Request | Captures | Consumed by |
|---|---|---|
| 01 Trigger nfl_extraction | `new_dag_run_id`<br>`correlation_id`<br>`extraction_dag_run_id` | this request<br>this request<br>02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12 |
| 02 nfl_extraction run state (waits until terminal) | `extraction_poll_count`<br>`extraction_poll_waited_ms`<br>`extraction_run_state` | informational, not consumed<br>this request<br>informational, not consumed |
| 03 nfl_extraction task instances | `extraction_task_id`<br>`extraction_try_number` | 04<br>04 |
| 05 XCom keys of publish | `extraction_xcom_key` | 06 |
| 06 XCom publish.marker_key | `s3_marker_key` | 13, 15, 17 |
| 07 XCom publish.manifest_key | `s3_manifest_key` | 13, 16 |
| 08 XCom publish.interval_prefix | `interval_prefix` | 13, 14, 16 |
| 09 XCom publish.silver_row_count | `silver_row_count` | 16, 18, 19 |
| 10 XCom publish.gold_row_count | `gold_row_count` | 16, 18, 20 |
| 11 XCom validate_conf.interval_start_date | `interval_start_date` | 16, 17, 18, 19, 20 |
| 12 XCom validate_conf.interval_end_date | `interval_end_date` | 16, 17, 18, 19, 20 |
| 13 S3 list interval prefix | `s3_keys`<br>`s3_first_parquet_key` | 16<br>informational, not consumed |
| 16 S3 GET manifest | `manifest` | this request |

### 40-reporting (15 requests, 41 tests)

| Request | Captures | Consumed by |
|---|---|---|
| 01 Trigger nfl_reporting | `new_dag_run_id`<br>`correlation_id`<br>`reporting_dag_run_id` | this request<br>this request<br>02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12 |
| 02 nfl_reporting run state (waits until terminal) | `reporting_poll_count`<br>`reporting_poll_waited_ms`<br>`reporting_run_state` | informational, not consumed<br>this request<br>informational, not consumed |
| 03 nfl_reporting task instances | `reporting_task_id`<br>`reporting_try_number` | 04<br>04 |
| 05 XCom keys of build_report | `reporting_xcom_key` | 06 |
| 06 XCom build_report.report_key | `report_key` | 13, 15 |
| 07 XCom build_report.report_name | `report_name` | 14, 15 |
| 08 XCom build_report.report_sha256 | `report_sha256` | 13, 14, 15 |
| 09 XCom build_report.report_size_bytes | `report_size_bytes` | 13, 15 |
| 10 XCom build_report.report_checksum_key | `report_checksum_key` | 14 |
| 11 XCom validate_conf.interval_start_date | `interval_start_date` | 15 |
| 12 XCom validate_conf.interval_end_date | `interval_end_date` | 15 |

### 50-delivery (19 requests, 58 tests)

| Request | Captures | Consumed by |
|---|---|---|
| 01 AT REST latest successful nfl_reporting run for the interval | `at_rest_dag_run_id`<br>`at_rest_dag_run_id_raw` | 02, 03, 04, 05<br>informational, not consumed |
| 02 AT REST XCom validate_conf.report_name | `delivery_report_name` | 06, 19 |
| 03 AT REST XCom validate_conf.interval_start_date | `interval_start_date` | 06, 19 |
| 04 AT REST XCom validate_conf.interval_end_date | `interval_end_date` | 06, 19 |
| 05 AT REST XCom deliver_sftp.delivery_action | `at_rest_delivery_action` | this request |
| 06 AT REST Shim delivery SFTP | `at_rest_mtime`<br>`at_rest_sha256` | 19<br>informational, not consumed |
| 07 Trigger nfl_reporting with force_redeliver | `new_dag_run_id`<br>`correlation_id`<br>`delivery_dag_run_id` | this request<br>this request<br>08, 09, 10, 11, 12, 13, 14, 15, 16, 17, 18 |
| 08 nfl_reporting run state (waits until terminal) | `delivery_poll_count`<br>`delivery_poll_waited_ms`<br>`delivery_run_state` | informational, not consumed<br>this request<br>informational, not consumed |
| 09 deliver_sftp task instance | `delivery_task_id`<br>`delivery_try_number`<br>`delivery_task_started_epoch`<br>`delivery_task_ended_epoch` | 10<br>10<br>19<br>19 |
| 11 XCom keys of deliver_sftp | `delivery_xcom_key` | 12 |
| 12 XCom deliver_sftp.delivery_action | `delivery_action` | this request |
| 13 XCom deliver_sftp.delivered | `delivery_delivered` | this request |
| 14 XCom validate_conf.force_redeliver | `delivery_force_redeliver` | this request |
| 15 XCom deliver_sftp.sftp_final_path | `sftp_final_path` | 19 |
| 16 XCom deliver_sftp.delivered_sha256 | `delivered_sha256` | 19 |
| 17 XCom deliver_sftp.delivered_size_bytes | `delivered_size_bytes` | 19 |
| 18 XCom deliver_sftp.copies_on_sftp | `copies_on_sftp` | this request |

### 60-observability (22 requests, 78 tests)

| Request | Captures | Consumed by |
|---|---|---|
| 01 DAG run history across the pipeline | `run_history_counts` | this request |
| 02 Latest nfl_ingestion run for the clean interval | `ingestion_clean_latest_state`<br>`obs_correlation_id` | this request<br>14, 21 |
| 03 Latest successful nfl_reporting run for the interval | `obs_dag_run_id`<br>`obs_dag_run_id_raw` | 06, 07<br>04, 05, 11 |
| 04 Failed task instances across the pipeline | `failed_task_count` | informational, not consumed |
| 06 XCom validate_conf.interval_start_date | `interval_start_date` | 08, 09, 10, 11, 12 |
| 07 XCom validate_conf.interval_end_date | `interval_end_date` | 08, 09, 10, 11, 12 |
| 09 Shim lineage rejects a sub-interval | `lineage_sub_interval_end` | this request |
| 10 Shim lineage rejects a range spanning two intervals | `lineage_span_end` | this request |
| 11 Shim events for the clean interval | `clean_interval_event_count`<br>`clean_interval_alert_count` | informational, not consumed<br>informational, not consumed |
| 12 Shim alerts for the clean interval | `clean_interval_injected_alert_count` | this request |
| 15 Shim alert delivery, last 24 hours | `alerts_notifiable_24h` | informational, not consumed |
| 17 Operations inbox is reachable | `inbox_total` | informational, not consumed |
| 18 Logs DescribeLogGroups | `log_groups` | informational, not consumed |
| 19 Shim request that leaves a marked log line | `log_probe_token`<br>`log_probe_sent_ms` | 20<br>20 |
| 22 Logs: Kafka Connect, recent events | `log_window_start_ms` | this request |

### 90-chaos (86 requests, 233 tests)

| Request | Captures | Consumed by |
|---|---|---|
| 01 A1 Kafka Connect list connectors | `chaos_connector_name` | 03, 04, 07, 08 |
| 04 A4 Connector status check goes red | `chaos_connector_state_seen` | informational, not consumed |
| 08 A8 Connector status check is green again | `chaos_connector_state_seen` | informational, not consumed |
| 10 B1 Trigger nfl_ingestion | `new_dag_run_id`<br>`chaos_dag_run_id` | this request, 27, 37, 44, 60, 89, 93, 96<br>11, 12, 13, 14, 15, 16, 20, 21, 22, 23, 24, 26 |
| 11 B2 nfl_ingestion run state (waits until terminal) | `chaos_poll_count`<br>`chaos_poll_waited_ms`<br>`chaos_run_state` | informational, not consumed<br>this request<br>informational, not consumed |
| 12 B3 nfl_ingestion task instances | `chaos_task_id`<br>`chaos_try_number` | 13<br>13 |
| 14 B5 XCom load_staging_to_utm.rows_rejected | `chaos_rows_rejected` | 19, 20 |
| 15 B6 XCom validate_conf.interval_start_date | `interval_start_date_chaos` | 17, 18, 19, 20, 21 |
| 16 B7 XCom validate_conf.interval_end_date | `interval_end_date_chaos` | 17, 18, 19, 20, 21 |
| 21 B12 The alert row carries its thread and a link to its log | `chaos_alert_log_api_path`<br>`chaos_alert_event_id`<br>`chaos_alert_correlation_id` | 22, 26<br>23, 26<br>22, 26 |
| 23 B14 The alert was delivered to the Operations channel | `chaos_delivery_waited_ms` | informational, not consumed |
| 24 B15 The alert email is in the Operations inbox | `chaos_alert_mail_id` | 26 |
| 27 J1 INJECT trigger nfl_extraction for the failed interval | `new_dag_run_id`<br>`chaos_extract_dag_run_id` | this request, 37, 44, 60, 89, 93, 96<br>28, 29 |
| 28 J2 nfl_extraction refuses (waits until terminal) | `chaos_extract_run_state` | this request |
| 30 D1 Latest successful nfl_extraction run for the interval | `chaos_marker_key`<br>`chaos_marker_baseline_ok`<br>`chaos_marker_baseline_key`<br>`chaos_extraction_dag_run_id`<br>`chaos_extraction_dag_run_id_raw` | 31, 34, 35, 36, 40, 41, 42, 43, 87<br>informational, not consumed<br>informational, not consumed<br>31, 32, 33<br>informational, not consumed |
| 31 D2 XCom publish.marker_key | `chaos_marker_key` | 34, 35, 36, 40, 41, 42, 43, 87 |
| 32 D3 XCom validate_conf.interval_start_date | `interval_start_date_marker` | 34, 36, 40, 42, 46, 91, 98 |
| 33 D4 XCom validate_conf.interval_end_date | `interval_end_date_marker` | 34, 36, 40, 42, 46, 91, 98 |
| 34 D5 Baseline marker check is green | `chaos_marker_baseline_ok`<br>`chaos_marker_baseline_key` | informational, not consumed<br>informational, not consumed |
| 37 D8 Trigger nfl_reporting while the marker is missing | `new_dag_run_id`<br>`chaos_marker_dag_run_id` | this request, 44, 60, 89, 93, 96<br>38, 39, 40 |
| 38 D9 nfl_reporting run state (waits until terminal) | `chaos_marker_poll_count`<br>`chaos_marker_poll_waited_ms`<br>`chaos_marker_run_state` | informational, not consumed<br>this request<br>informational, not consumed |
| 39 D10 nfl_reporting task instances | `chaos_marker_try_number`<br>`chaos_marker_max_tries` | informational, not consumed<br>40 |
| 44 D15 RESTORE trigger nfl_reporting with the marker back | `new_dag_run_id`<br>`chaos_restore_dag_run_id` | this request, 60, 89, 93, 96<br>45 |
| 45 D16 nfl_reporting restore run state (waits until terminal) | `chaos_restore_poll_count`<br>`chaos_restore_poll_waited_ms`<br>`chaos_restore_run_state` | informational, not consumed<br>this request<br>informational, not consumed |
| 50 E1 DMS DescribeReplicationTasks | `chaos_dms_task_arn` | 51, 53, 56 |
| 60 F1 INJECT trigger nfl_ingestion with an empty conf | `new_dag_run_id`<br>`chaos_badconf_dag_run_id` | this request, 89, 93, 96<br>61, 62, 63 |
| 61 F2 nfl_ingestion run state (waits until terminal) | `chaos_badconf_poll_count`<br>`chaos_badconf_poll_waited_ms`<br>`chaos_badconf_run_state` | informational, not consumed<br>this request<br>informational, not consumed |
| 70 G1 Kafka Connect list connectors | `chaos_sink_connector_name` | 72, 73, 75, 76 |
| 73 G4 Connector status check goes red for the sink | `chaos_sink_state_seen` | informational, not consumed |
| 75 G6 RESTORE resume the JDBC sink connector | `chaos_sink_resumed_at_ms` | 77 |
| 76 G7 Connector status check is green again | `chaos_sink_state_seen` | informational, not consumed |
| 77 G8 Heartbeat is landing again after the resume | `chaos_heartbeat_waited_ms` | this request |
| 78 H1 Baseline: the watcher is alive and sees replication healthy | `chaos_watch_baseline_ok`<br>`chaos_watch_incident` | 79<br>80, 81, 82, 85, 86 |
| 79 H2 INJECT pause the Debezium connector, and trigger nothing | `chaos_watch_injected_ms` | 80 |
| 80 H3 The watcher noticed on its own | `chaos_watch_noticed_ms`<br>`chaos_watch_incident` | this request<br>81, 82, 85, 86 |
| 81 H4 The watcher's alert, delivered | `chaos_watch_alert_event_id` | 85 |
| 87 K1 Baseline: S3 GET manifest | `chaos_manifest_original`<br>`chaos_manifest_baseline_key`<br>`chaos_manifest_key` | 91, 95<br>this request<br>88, 95 |
| 88 K2 INJECT overwrite the manifest's gold row count | `chaos_manifest_tampered_rows` | 91, 95 |
| 89 K3 Trigger nfl_reporting against the inconsistent manifest | `new_dag_run_id`<br>`chaos_gate_dag_run_id` | this request, 93, 96<br>90, 91, 92 |
| 90 K4 Is Process Failed stops the run and delivers nothing | `chaos_gate_run_state` | this request |
| 93 K7 RESTORE trigger nfl_extraction to republish the manifest | `new_dag_run_id`<br>`chaos_rebuild_dag_run_id` | this request, 96<br>94 |
| 96 K10 RESTORE trigger nfl_reporting with the manifest back | `new_dag_run_id`<br>`chaos_rereport_dag_run_id` | this request<br>97 |
