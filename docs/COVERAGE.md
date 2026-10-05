# Requirement coverage

What each requirement of the NFL requirements specification is exercised by in this local harness, and
what it is not. Written 2026-10-04 (UTC) from the running stack and the files in this working tree
(nothing is committed, so there is no SHA to cite).

## How to read this

**Requirement text.** The ID, priority and wording come from the specification "NFL Data Platform:
Reporting Pipeline Requirements Specification" (Shakeel Ahmad, 2026-10-01), read on 2026-10-04. The
wording in the tables is abridged; the IDs and priorities are exact. The specification has 61 IDs:
REP-01..05, ORC-01..06, ING-01..06, EXT-01..07, RPT-01..07, ALR-01..03, DSH-01..02, NFR-01..13,
SEC-01..12. The earlier version of this file used reconstructed requirement text and had no SEC rows;
both are replaced here.

**"Exercised by"** names things that exist and were checked:

- `NN-folder/NN` is a request file in `bruno/` (for example `20-ingestion/12` is
  `bruno/20-ingestion/12-shim-ingestion-reconcile.bru`). Every one cited resolves to exactly one file.
- `/checks/...` is a route of the check shim on port 8096. Every one cited is in the shim's live
  `openapi.json`.
- `nfl_<dag>.<task>` is an Airflow task. Every one cited is in `airflow tasks list` for that DAG.
- Anything else (a SQL probe, a hand-run by a builder) is labelled as such and is **not** part of a
  Bruno run.

**Verdicts.**

| Verdict | Meaning |
|---|---|
| **COVERED** | A request in the collection exercises the requirement as written against the running local stack, and the run is green. |
| **STAND-IN** | A request exercises the contract, but through a local substitute: Python/SQL in an Airflow task instead of Apache Hop or Spark, Debezium instead of AWS DMS, floci instead of AWS, a Postgres event table instead of a dashboard. The production engine is not proven. |
| **PARTIAL** | Part of the requirement is exercised by a request; a named part is not. The last column says which. |
| **NOT COVERED** | No request exercises it. The reason is given. |

A green Bruno run therefore proves the COVERED rows, the contract half of the STAND-IN rows, and the
named half of the PARTIAL rows. It proves nothing about the NOT COVERED rows.

**Measured state behind this file (2026-10-04 UTC, bru 4.2.0, this stack):** normal run 100 of 100
requests and 309 of 309 tests; `90-chaos` 57 of 57 and 160 of 160; normal run again 100 of 100 and
309 of 309. All three were re-run for this document, in that order, and the normal run once more after
a `reset.sh -y` and a cold `up.sh` (100 of 100, 309 of 309). Since then one test that could not fail was removed from `60-observability/02` on 2026-10-05; the normal run measured that day, chaos not re-run, is 100 of 100 requests and 308 of 308 tests.

## Summary

<!-- summary:begin -->
Requirement IDs in the specification: **61**. Bruno requests cited: **91** distinct files.

| Verdict | All 61 IDs | Of the 32 functional IDs with priority Must |
|---|---|---|
| **COVERED** | 9 | 7 |
| **STAND-IN** | 11 | 8 |
| **PARTIAL** | 17 | 13 |
| **NOT COVERED** | 24 | 4 |

| Area | COVERED | STAND-IN | PARTIAL | NOT COVERED |
|---|---|---|---|---|
| REP | 0 | 2 | 2 | 1 |
| ORC | 1 | 0 | 4 | 1 |
| ING | 1 | 4 | 0 | 1 |
| EXT | 2 | 1 | 3 | 1 |
| RPT | 4 | 1 | 2 | 0 |
| ALR | 0 | 0 | 2 | 1 |
| DSH | 0 | 1 | 0 | 1 |
| NFR | 1 | 1 | 2 | 9 |
| SEC | 0 | 1 | 2 | 9 |
<!-- summary:end -->

The counts above are produced by a script from the tables below (see "Keeping this file honest").

## REP: replication

| ID | Pri | Requirement | Exercised by | Verdict | Not proven |
|---|---|---|---|---|---|
| REP-01 | Must | DMS full load, then continuous change capture, from MySQL into Staging | `10-replication/01`, `10-replication/02` (task running; the three tables have rows in staging), `10-replication/05` to `10-replication/08` (source and sink connectors RUNNING), `10-replication/10` with `/checks/replication/connector-status`; change capture continuing: `90-chaos/77` (a beat written at the source after the sink resumed has landed) | **STAND-IN** | Debezium and a JDBC sink move the rows, not DMS. DMS task settings, its full-load to CDC cutover and its failure vocabulary are untested. The DMS-shaped API on 8098 is a stub that replicates nothing. |
| REP-02 | Must | Table and column mappings are configuration driven and version controlled | `10-replication/02` (exactly the three Hawks tables are replicated, by name). The mapping is `table.include.list` in `infra/debezium/mysql-source.json` and `topics` in `infra/debezium/jdbc-sink.json`. | **STAND-IN** | DMS table-mapping rules. Column-level mapping. No request shows that an unlisted table stays out. Nothing is committed yet, so "version controlled" is not true today. |
| REP-03 | Must | Lag is measured continuously and alerts above the threshold (assumed 5 minutes) | `10-replication/09` with `/checks/replication/lag` (source capture lag, MySQL to Kafka only); `20-ingestion/14` with `/checks/ingestion/freshness` (end-to-end heartbeat, 300 s limit); red cases `90-chaos/05` and `90-chaos/73`; `nfl_ingestion.pre_check` fails a run on a stale heartbeat | **PARTIAL** | Lag is measured when asked, not continuously. An alert row is written only when an ingestion run reaches `pre_check`; nothing alerts on its own. The stale-heartbeat alert was produced by hand in the infra fix round (beats stopped, run failed at 330 s), not by a request. |
| REP-04 | Should | Source schema changes are detected and reported, never silently dropping data | none | **NOT COVERED** | No request changes the source schema. |
| REP-05 | Must | Staging is written only by DMS | No Bruno request. Enforced by Postgres grants in `infra/sql/03-roles.sql.tmpl`: only `nfl_replication` holds INSERT on `staging`. SQL probe run for this document: `nfl_hop` INSERT into `staging.payment` refused. | **PARTIAL** | Proven by a SQL probe outside the collection. The superuser `nfl_admin` can still write. Local grants, not DMS or IAM. |

## ORC: orchestration

| ID | Pri | Requirement | Exercised by | Verdict | Not proven |
|---|---|---|---|---|---|
| ORC-01 | Must | Airflow owns scheduling and triggers the three pre-checks | `20-ingestion/02`, `20-ingestion/03`, `30-extraction/01`, `40-reporting/01`; the pre-checks are `nfl_ingestion.pre_check`, `nfl_extraction.pre_check_ingestion` and `nfl_reporting.wait_for_success_marker`, seen succeeded in `20-ingestion/05`, `30-extraction/03`, `40-reporting/03` | **PARTIAL** | Every DAG has `schedule=None`. Runs are started through the REST API; no schedule ever fires. The three DAGs are not chained: nothing starts the next stage automatically. |
| ORC-02 | Must | A stage starts only when the upstream stage reported success for the same interval | Reporting refuses without the marker: `90-chaos/37` to `90-chaos/40`. The ingestion gate stops its own downstream tasks: `90-chaos/12`. Extraction's refusal is `nfl_extraction.pre_check_ingestion`. | **PARTIAL** | No request triggers extraction for the failed interval. That refusal was seen only in the pipeline builder's hand-run, on an earlier volume. |
| ORC-03 | Must | Every run is parameterised by data interval; reruns and backfills need no code change | `20-ingestion/03`, `20-ingestion/10`, `20-ingestion/11`; a second interval with the same code: `90-chaos/10`; a run with no interval is refused: `90-chaos/60` to `90-chaos/63` | **COVERED** | A backfill over many intervals was not run. |
| ORC-04 | Must | Retries use a configurable count and exponential backoff | `90-chaos/39` (the gate used every retry), `90-chaos/40` (one retry event per retry). Count and delay are `NFL_GATE_RETRIES` and `NFL_GATE_RETRY_DELAY_SECONDS` on `nfl_reporting.wait_for_success_marker` and `nfl_extraction.is_utm`. | **PARTIAL** | The delay is fixed (15 s). There is no exponential backoff in the DAG code. Every other task has 0 retries. No request changes the count. |
| ORC-05 | Must | Each task is idempotent: a rerun gives the same result and no duplicates | Delivery rerun: `50-delivery` re-triggers reporting for the same interval and `50-delivery/18` finds exactly one copy on SFTP. Ingestion and extraction: one trigger each per run, end state checked by `20-ingestion/12` (staging equals utm per table) and `20-ingestion/13` (no duplicate keys) | **PARTIAL** | Only the delivery rerun is covered by requests (`50-delivery`). A single green run triggers ingestion and extraction once each, and no request compares outputs across runs; their idempotency was shown by repeating runs by hand and diffing, not by a request. Measured that way for this document: across two cold starts and every rerun in between the report is 3736 bytes with sha256 `31d92497...1f1d` and the manifest sha256 is `6d5ecc37...c94d`, the same values the pipeline builder recorded on an earlier volume. |
| ORC-06 | Should | Concurrency limits and SLA timeouts are configured per DAG | none | **NOT COVERED** | Each DAG file sets `max_active_runs=1`, which no request exercises. No SLA or timeout is configured anywhere. |

## ING: ingestion

| ID | Pri | Requirement | Exercised by | Verdict | Not proven |
|---|---|---|---|---|---|
| ING-01 | Must | A pre-check confirms DMS is healthy, Staging is fresh, UTM is reachable and Hop is available | `nfl_ingestion.pre_check` (both connectors RUNNING, heartbeat within 300 s, utm reachable); green in `20-ingestion/05`; its event counted in `60-observability/08` | **STAND-IN** | It reads Debezium connector state, not a DMS task. There is no Hop to check. The red path (stale heartbeat fails the run with an alert) was produced by hand, not by a request. |
| ING-02 | Must | Hop loads Staging into UTM incrementally with an upsert on business keys | `nfl_ingestion.load_staging_to_utm`; `20-ingestion/09`, `20-ingestion/12`, `20-ingestion/13` | **STAND-IN** | The load is SQL run as the `nfl_hop` Postgres role. No Hop pipeline exists here. Engine fidelity: none. |
| ING-03 | Must | Each load is transactional per batch; a failure leaves UTM unchanged for that batch | none | **NOT COVERED** | No request checks it, and the observed behaviour differs from the requirement: after `90-chaos` scenario B the September batch is FAILED, yet the 7 rows that passed validation stay in utm (measured 2026-10-04: `/checks/ingestion/reconcile` for September read utm 1, 3 and 3; batch row 17 read, 7 loaded, 10 rejected). The load commits before `is_failed_gate` decides. Extraction refuses a FAILED batch, so those rows are not published. |
| ING-04 | Must | The "Is Failed" check marks the run failed, blocks downstream and alerts Operations | `nfl_ingestion.is_failed_gate`; `90-chaos/10` to `90-chaos/13`, `90-chaos/18`, `90-chaos/20` | **STAND-IN** | The gate judges the SQL stand-in's result, not a Hop result. "Blocks downstream" is shown inside the DAG only (see ORC-02). The alert is a database row, not a message to a person (ALR-03). |
| ING-05 | Must | Row count reconciliation, null business keys and duplicates pass before the run is successful | `nfl_ingestion.data_quality`; `20-ingestion/12` with `/checks/ingestion/reconcile`; `20-ingestion/13` with `/checks/ingestion/quality`; red cases `90-chaos/17`, `90-chaos/19` | **COVERED** | Seeded data only: 460 rows in the clean interval, 17 in the bad one. |
| ING-06 | Should | Hop logs, row counts and durations are captured per run | `20-ingestion/06` (start and end time), `20-ingestion/07` (task log), `20-ingestion/09` (row counts per table) | **STAND-IN** | These are Airflow task logs and XComs of the stand-in. There are no Hop logs. |

## EXT: extraction

| ID | Pri | Requirement | Exercised by | Verdict | Not proven |
|---|---|---|---|---|---|
| EXT-01 | Must | A pre-check confirms ingestion completed for the interval | `nfl_extraction.pre_check_ingestion`; green in `30-extraction/03` | **PARTIAL** | Only the passing path is in the collection. The refusal for a failed interval was seen in the pipeline builder's hand-run only. |
| EXT-02 | Must | "Is UTM" confirms the data is present and complete, retries, then alerts | `nfl_extraction.is_utm`; green in `30-extraction/03` | **PARTIAL** | No request makes it retry or exhaust its retries. The builder's hand-run saw one retry then a pass. The same retry mechanism is exercised on the reporting gate (RPT-01). |
| EXT-03 | Must | A Spark job reads the interval from UTM with partitioned reads | `nfl_extraction.write_parquet_temp`; `30-extraction/02`, `30-extraction/03` | **STAND-IN** | The task is psycopg2 plus pyarrow. No Spark, no partitioned read, no cluster. Engine fidelity: none. |
| EXT-04 | Must | Parquet output on S3 under a path that carries the interval | `30-extraction/08`, `30-extraction/13`, `30-extraction/19`, `30-extraction/20` with `/checks/extraction/parquet-rowcount` | **COVERED** | The layout is `df_<start>_<end>/<layer>/<dataset>/`; the specification's example puts layer and dataset first. Dates are `YYYYMMDD`. The store is floci, not S3. |
| EXT-05 | Must | Writes are atomic: temporary prefix, then publish with marker and manifest | `nfl_extraction.publish`; `30-extraction/14` (temp prefix empty after publish), `30-extraction/15`, `30-extraction/16`, `30-extraction/17` with `/checks/extraction/s3-marker`, `30-extraction/18` with `/checks/extraction/manifest` | **PARTIAL** | Only the finished state is observed. No request interrupts a publish. S3 has no atomic rename; the marker written last is the commit point. |
| EXT-06 | Should | Silver holds cleansed data, Gold holds aggregates | `30-extraction/09`, `30-extraction/10`, `30-extraction/19`, `30-extraction/20` | **COVERED** | Row counts and checksums are checked, not whether the content is "cleansed" or the aggregates are right. |
| EXT-07 | Must | Any extraction failure alerts Operations with run details | none | **NOT COVERED** | No request fails an extraction run. The alert hook is the one shared by all three DAGs (`nfl_common.make_failure_hook`) and is exercised for ingestion and reporting. |

## RPT: reporting and delivery

| ID | Pri | Requirement | Exercised by | Verdict | Not proven |
|---|---|---|---|---|---|
| RPT-01 | Must | A pre-check confirms the S3 success marker; if absent it retries, then alerts | `nfl_reporting.wait_for_success_marker`; `90-chaos/34` to `90-chaos/40`; restore `90-chaos/41` to `90-chaos/46`; green in `40-reporting/03` | **COVERED** | The retry delay is fixed, not exponential (ORC-04). |
| RPT-02 | Must | The Reporting Engine applies report logic as Spark jobs | `nfl_reporting.build_report`; `40-reporting/02`, `40-reporting/03`, `40-reporting/15` | **STAND-IN** | The report is built in Python inside an Airflow task. No Spark. One report (`NFL_ACCT_ACTIVITY`), no report metadata store. |
| RPT-03 | Must | Report files have a deterministic name, a header and a checksum | `40-reporting/07`, `40-reporting/08`, `40-reporting/13`, `40-reporting/14`, `40-reporting/15` with `/checks/reporting/file` | **COVERED** | The header record is checked by `nfl_reporting.is_process_failed`, not asserted by a request. |
| RPT-04 | Must | "Is Process Failed" stops the run, delivers nothing and alerts | `nfl_reporting.is_process_failed`; green in `40-reporting/03`, `60-observability/05` | **PARTIAL** | No request makes this gate fail. |
| RPT-05 | Must | Delivery uploads under a temporary name, verifies size and checksum, then renames | `nfl_reporting.deliver_sftp`; `50-delivery/07` to `50-delivery/19` with `/checks/delivery/sftp` | **COVERED** | The recipient is a local SFTP container. A failure in the middle of a transfer is not injected. |
| RPT-06 | Must | Delivery retries transient errors and sends no duplicate unless a rerun is flagged | `50-delivery/05` (an unflagged rerun skips), `50-delivery/07`, `50-delivery/12`, `50-delivery/14`, `50-delivery/18` (a flagged rerun replaces, one copy) | **PARTIAL** | `deliver_sftp` has 0 retries, and no transient SFTP error is injected. |
| RPT-07 | Must | Events and metrics are captured per step | `nfl_reporting.capture_events`; `60-observability/08` with `/checks/lineage`; `60-observability/11` with `/checks/events`; `50-delivery/09` (task start and end) | **COVERED** | One event per stage outcome, carrying status, row count, file size and delivery action. Start and end times come from Airflow, not from the event row. |

## ALR and DSH: alerting and dashboard

| ID | Pri | Requirement | Exercised by | Verdict | Not proven |
|---|---|---|---|---|---|
| ALR-01 | Must | Alerts at four points: ingestion failure, extraction failure, reporting pre-check failure, report process failure | Ingestion failure: `90-chaos/13`, `90-chaos/20`. Reporting pre-check failure: `90-chaos/40`. Also an invalid-conf failure: `90-chaos/63`. | **PARTIAL** | Two of the four points. No request fails extraction or the report process. |
| ALR-02 | Must | Each alert carries run id, stage, interval, error summary, severity and a link to logs | `90-chaos/20`, `90-chaos/40`, `90-chaos/63` | **PARTIAL** | The alert row has run id, stage, interval, error summary and severity. It has no link to logs. Severity is the constant `critical`; the three severity levels of section 7 do not exist. |
| ALR-03 | Must | Alerts reach Operations and Production Support through the agreed channels | none | **NOT COVERED** | An alert here is a row in `observability.pipeline_event` and a log line. Nothing sends email, chat or a page. |
| DSH-01 | Must | The dashboard shows run status per stage, durations, volumes, delivery status and SLA compliance | `60-observability/01`, `60-observability/08` with `/checks/lineage`, `60-observability/11`, `60-observability/12` with `/checks/events` | **STAND-IN** | There is no dashboard. The data a dashboard would read is a Postgres table, read through the shim. SLA compliance is not computed. |
| DSH-02 | Must | Dashboard access is role based and read only for business users | none | **NOT COVERED** | No dashboard exists. |

## NFR: non-functional

| ID | Area | Requirement | Exercised by | Verdict | Not proven |
|---|---|---|---|---|---|
| NFR-01 | Freshness | Replication lag under 5 minutes (assumed) | `20-ingestion/14` (heartbeat within 300 s), `10-replication/09` | **STAND-IN** | Debezium on an idle seeded source. Says nothing about DMS lag under load. |
| NFR-02 | Timeliness | Reports delivered by the agreed cut-off | none | **NOT COVERED** | No cut-off is defined and no request measures run time against one. |
| NFR-03 | Performance | Extraction and reporting finish in the batch window and scale with volume | none | **NOT COVERED** | 460 rows, no Spark. Local timings predict nothing. |
| NFR-04 | Scalability | 5x volume without code change | none | **NOT COVERED** | No volume test. |
| NFR-05 | Reliability | Transient failures recover through retries; permanent failures alert within minutes | `90-chaos/39`, `90-chaos/40` | **PARTIAL** | Retries and the final alert row are shown. No request has a retry that then succeeds, and no request measures the time to the alert. |
| NFR-06 | Idempotency | Any stage can be rerun with identical results | as ORC-05: `50-delivery/18` for the delivery rerun; `20-ingestion/12`, `20-ingestion/13` for the end state of one run | **PARTIAL** | As ORC-05: the delivery rerun is covered by `50-delivery`; ingestion and extraction idempotency was shown by repeating runs by hand and diffing, not by a request. |
| NFR-07 | Availability | High availability inside the region | none | **NOT COVERED** | One container per component on one machine. |
| NFR-08 | Recoverability | RPO 15 minutes, RTO 4 hours (assumed) | none | **NOT COVERED** | No restore test. A stop and start keeps the volumes; that is not a recovery test. |
| NFR-09 | Data integrity | Checksum verified on every delivery | `50-delivery/06`, `50-delivery/16`, `50-delivery/19` | **COVERED** | Local SFTP only. |
| NFR-10 | Maintainability | Pipelines, DAGs and infrastructure are code, reviewed and versioned | none | **NOT COVERED** | Everything is a file in this tree, but the repository has no commits and no review history. |
| NFR-11 | Configurability | Schedules, thresholds, retries and mappings come from configuration | none | **NOT COVERED** | Thresholds and retries are environment variables in `infra/docker-compose.yml`; mappings are the connector JSON. No request changes one and observes the effect. |
| NFR-12 | Cost | Compute sized to workload | none | **NOT COVERED** | Not a local concern. |
| NFR-13 | Portability | Jobs depend on standard interfaces only | none | **NOT COVERED** | The claim is about the production Hop and Spark jobs, which are not here. |

## SEC: security and compliance

| ID | Area | Requirement | Exercised by | Verdict | Not proven |
|---|---|---|---|---|---|
| SEC-01 | Cross-account access | Dedicated least-privilege database user and private connectivity | none | **NOT COVERED** | One Docker network, no accounts. The local `debezium` MySQL user's grants are listed in `infra/README.md`; no request tests them. |
| SEC-02 | Identity and access | Each component uses its own role with only the permissions it needs | `00-preflight/02` (every check declares its role and what is enforced locally). Outside the collection, measured for this document: the live column grants of the five `nfl_check_*` roles equal the `grants` lists in `shim/roles/*.yaml` exactly, and 14 of 14 forbidden SQL statements across `nfl_hop`, `nfl_spark`, `nfl_replication`, `nfl_events`, `nfl_stub_reader` and four check roles were refused. | **STAND-IN** | Postgres and MySQL roles are real and enforced. No IAM: floci accepts any key, so the S3 checks share one key and prove reachability only. All DAG credentials sit in one Airflow container. The one Airflow API user is an Admin. |
| SEC-03 | Encryption in transit | TLS everywhere; SFTP with key-based authentication | `50-delivery/06`, `50-delivery/19` (the pinned SSH host key was verified) | **PARTIAL** | No TLS on MySQL, Postgres, Kafka, Airflow or floci. The delivery upload logs in with a password, not a key; only the read-only checker uses a key pair. |
| SEC-04 | Encryption at rest | KMS-managed keys with rotation | none | **NOT COVERED** | Plain Docker volumes. |
| SEC-05 | Secrets | Credentials live in a secrets manager, rotate, never appear in code or logs | none | **NOT COVERED** | Credentials are generated into `infra/nfl.env` (gitignored) and read from the environment; connector configs use Kafka's file config provider. No secrets manager is used and nothing rotates on a schedule. |
| SEC-06 | Network | Private subnets, security groups, controlled egress | none | **NOT COVERED** | Published ports bind 127.0.0.1 only. That is local hygiene, not network design. |
| SEC-07 | Data protection | Sensitive fields classified and masked by default | none | **NOT COVERED** | The delivered report holds per-facility totals only. The Silver Parquet for `patient_account` carries patient names and MRN unmasked. |
| SEC-08 | Auditability | Access, changes, runs and deliveries logged to immutable storage | `60-observability/13` (Airflow audit log of triggers), `60-observability/11` (event rows) | **PARTIAL** | The event writer role can only INSERT, but the table is ordinary Postgres storage and a superuser can change it. Data access is not logged. |
| SEC-09 | Segregation of duties | Developers cannot change production data or approve their own deployments | none | **NOT COVERED** | One operator, one Admin user. |
| SEC-10 | Dashboard access | Single sign-on with role-based access | none | **NOT COVERED** | No dashboard. |
| SEC-11 | Vulnerability management | Images and libraries scanned and patched | none | **NOT COVERED** | Images are pinned by tag. Nothing scans them. |
| SEC-12 | Compliance | Controls map to the applicable standards | none | **NOT COVERED** | Not addressed. |

## Things a green run could be mistaken for

- **"Hop and Spark work."** They are not in the stack. Ingestion, extraction and reporting are Python
  and SQL inside Airflow tasks. The contract (inputs, outputs, gates, markers, checksums) is proven;
  the engines are not.
- **"DMS works."** Debezium moves the rows. The DMS endpoint on 8098 is a stub whose `Status` is derived
  from the Debezium connector's state.
- **"Least privilege is enforced."** For Postgres, MySQL and SFTP it is. For S3 it is not: floci
  evaluates no IAM policy.
- **"Alerting works."** An alert row is written. Nobody is notified.
- **"It runs on AWS."** The `aws` Bruno environment has never been run against an account, and the
  Airflow requests use basic auth, which Amazon MWAA does not accept.
- **"The collection works in the Bruno app."** Every run was made with the `bru` CLI.

## Findings this mapping turned up

These are differences between the specification and what the harness does, found while mapping. They
belong to the DAG owner and the specification owner, not to this document.

1. **ING-03.** A failed batch leaves its accepted rows in utm (7 rows for September).
2. **ORC-04.** Retry delay is fixed; the specification asks for exponential backoff.
3. **RPT-06.** `deliver_sftp` does not retry.
4. **SEC-03.** The delivery upload authenticates with a password.
5. **ALR-02.** Alert rows carry no link to logs and a single severity.
6. **ALR-01.** Two of the four alert points have no failure-injection request.
7. **EXT-04.** The S3 path puts the interval before layer and dataset; the specification's example has
   it after.

## Keeping this file honest

`docs/check_coverage.py` reads the tables above and:

1. counts the verdicts and rewrites the Summary block;
2. fails if the number of rows is not 61 or an ID is missing or repeated;
3. fails if a cited `NN-folder/NN` does not resolve to exactly one file in `bruno/`;
4. with `--live`, fails if a cited `/checks/...` route is not in the shim's `openapi.json` or a cited
   `nfl_<dag>.<task>` is not in the DAG file.

```bash
python3 docs/check_coverage.py          # recount and check the Bruno references
python3 docs/check_coverage.py --live   # also check shim routes against the running shim
```

Change a verdict only after the request that justifies it has been seen to go red for the right reason
at least once. Do not edit the Summary by hand.
