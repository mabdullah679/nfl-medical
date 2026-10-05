# Local test architecture

What runs on this machine when the NFL harness is up, what each piece stands in for, and what each
stand-in cannot prove. Written 2026-10-04 (UTC) from the running stack and the files in this tree.

The harness proves the pipeline's **contract** end to end: rows move from MySQL to Postgres, a run for
one data interval loads them, publishes Parquet with a marker and a manifest, builds a report with a
checksum, and delivers it over SFTP, with gates that stop the run and record an alert. It does not prove
the production **engines**. Apache Hop, Spark, AWS DMS, AWS IAM and a dashboard are not in the stack.

## 1. The production pipeline (from the design)

```
Hawks AWS MySQL
      |  (1) AWS DMS: full load, then change capture
      v
PostgreSQL staging
      |  (2) Apache Hop        gate: "Is Failed"
      v
PostgreSQL utm
      |  (3) Spark             gates: pre-check, "Is UTM" (retries)
      v
S3  df_<start>_<end>/  Silver + Gold Parquet, _SUCCESS, manifest.json
      |  (4) Spark             gates: S3 marker pre-check (retries), "Is Process Failed"
      v
Report file + checksum
      |  (5) SFTP: upload under a temp name, verify, rename
      v
Recipient                      events and metrics -> dashboard; four alert points -> Operations

Airflow orchestrates every stage.
```

## 2. What runs locally

```
nfl-mysql ──binlog──> nfl-kafka-connect (Debezium source) ──> nfl-kafka ──> nfl-kafka-connect (JDBC sink) ──> nfl-postgres.staging
                                                                                                                    |
nfl-airflow  nfl_ingestion   SQL as role nfl_hop      staging -> utm                                    <──────────┘
             nfl_extraction  psycopg2 + pyarrow       utm -> floci S3 (Parquet, manifest.json, _SUCCESS)
             nfl_reporting   Python                   S3 -> report + .sha256 -> nfl-sftp
             every task      INSERT as nfl_events     -> nfl-postgres observability.pipeline_event

nfl-shim      read-only checks over HTTP, one identity per check   (reads Postgres, MySQL, Kafka Connect, S3, SFTP)
nfl-dms-stub  DMS-shaped API; Status derived from the Debezium connector state   (called by Bruno only)
```

Compose project `nfl`, file `infra/docker-compose.yml`, env file `infra/nfl.env`. Nine containers:

| Container | Image | Host port | What it is |
|---|---|---|---|
| `nfl-mysql` | `mysql:8.0.40` | 127.0.0.1:3307 | Source database `hawks`, binlog on. Seeded by `infra/sql/04-seed.sql`. |
| `nfl-kafka` | `apache/kafka:3.9.0` | 127.0.0.1:9099 | Broker for the change events. |
| `nfl-kafka-connect` | `debezium/connect:2.7.3.Final` | 127.0.0.1:8097 | Runs both connectors: `nfl-hawks-mysql-source` (Debezium) and `nfl-staging-jdbc-sink`. This is what replicates. |
| `nfl-postgres` | `postgres:16-alpine` | 127.0.0.1:5440 | Database `nfl`, schemas `staging`, `utm`, `observability`. |
| `nfl-airflow-db` | `postgres:16-alpine` | none | Airflow metadata. |
| `nfl-airflow` | `apache/airflow:2.10.4` | 127.0.0.1:8095 | DAGs `nfl_ingestion`, `nfl_extraction`, `nfl_reporting`; REST API with basic auth. |
| `nfl-sftp` | `atmoz/sftp:alpine` | 127.0.0.1:2225 | Delivery target. Upload user chrooted to `/outbound`; a second, read-only checker user. |
| `nfl-shim` | built from `shim/` | 127.0.0.1:8096 | Check shim, 12 checks under `/checks/*`. |
| `nfl-dms-stub` | built from `stub/` | 127.0.0.1:8098 | DMS control-plane stub (awsjson1.1). |

Every published port binds `127.0.0.1` only. `nfl-airflow-db` publishes nothing.

**floci** is the local AWS emulator on `http://localhost:4566` (`host.docker.internal:4566` from
containers). It is external: this project does not start, stop or configure it. The harness uses its S3
only, in the one bucket `nfl-data-platform`, which `scripts/up.sh` creates if absent. floci also reports
CloudWatch (`monitoring`), `logs`, `secretsmanager`, `kms` and `iam` as running; **nothing in this stack
uses them**.

**Not in the stack:** Apache Hop, Spark, AWS DMS, any dashboard, any alert channel. No container runs
them and no task calls them.

Ports this project must never use, because other projects on the machine hold them: 3000, 3100, 4566,
5432, 5433, 5434, 7077, 8080, 8085, 8090, 8180, 15432, 18000, 19000, 19001, 19092, 29092. `up.sh`
refuses to start if one of its own ports is taken by something else.

## 3. The three DAGs

All three have `schedule=None` and `max_active_runs=1`. A run is started through the Airflow REST API
with `conf = {"interval_start": "<UTC midnight>", "interval_end": "<UTC midnight>"}`; the window is
half-open. The DAGs are not chained: each is triggered separately, and each refuses to proceed unless
the stage before it left its success signal for the same interval.

| DAG | Tasks, in order | Stands in for |
|---|---|---|
| `nfl_ingestion` | `validate_conf` > `pre_check` > `load_staging_to_utm` > `data_quality` > `is_failed_gate` > `record_event` | Apache Hop |
| `nfl_extraction` | `validate_conf` > `pre_check_ingestion` > `is_utm` > `write_parquet_temp` > `publish` > `record_event` | Spark extraction job |
| `nfl_reporting` | `validate_conf` > `wait_for_success_marker` > `build_report` > `is_process_failed` > `deliver_sftp` > `capture_events` | Spark reporting framework and SFTP delivery |

**The gates of the design are Airflow tasks, not shim endpoints:**

| Gate in the design | Airflow task | Retries |
|---|---|---|
| Ingestion pre-check | `nfl_ingestion.pre_check` | 0 |
| "Is Failed" | `nfl_ingestion.is_failed_gate` | 0 |
| Extraction pre-check | `nfl_extraction.pre_check_ingestion` | 0 |
| "Is UTM" | `nfl_extraction.is_utm` | `NFL_GATE_RETRIES` (3), fixed delay `NFL_GATE_RETRY_DELAY_SECONDS` (15) |
| Reporting pre-check on the S3 marker | `nfl_reporting.wait_for_success_marker` | same |
| "Is Process Failed" | `nfl_reporting.is_process_failed` | 0 |

The shim does not implement the gates. It re-measures, from outside the DAGs and with its own
read-only identities, the facts the gates decide on, so a Bruno request can assert them.

**Events and alerts.** Each stage writes rows to `observability.pipeline_event` as the role
`nfl_events`, which can INSERT and cannot read. A task that fails for good writes a row with
`status='failed'` and `metrics.alert=true`, `severity`, `error_summary`; each retry writes a `retry` row.
A run whose conf yields no interval writes its alert with NULL interval bounds. That table is the whole
of local observability. Nothing is sent to CloudWatch and nobody is notified.

**Replication freshness.** Debezium writes one row to MySQL `hawks.replication_heartbeat` every 20 s.
It travels the same path as data and lands in `staging.replication_heartbeat` as a new row per beat.
`nfl_ingestion.pre_check` and `/checks/ingestion/freshness` both require the newest beat to be at most
300 s old.

## 4. Substitutions

Read the last column first.

### Stage 1: replication

| Production | Local | Fidelity | Cannot prove |
|---|---|---|---|
| AWS DMS data plane | Debezium MySQL source + JDBC sink on Kafka Connect | Contract only: rows really move by binlog change capture, through a different engine | Anything specific to DMS: table mappings, LOB handling, full-load to CDC cutover, task settings, its failure states. |
| AWS DMS control plane | `stub/` on 8098 | Request and response shape only | Anything about real DMS. `Status` is derived from the Debezium connector state; Start and Stop record intent and change nothing. Fields with no local source are `null`, tagged `unknown`. |
| Hawks MySQL in another AWS account | `nfl-mysql`, seeded | Engine same, data synthetic | Volume, schema drift, cross-account connectivity, Hawks-side access control. |
| DMS IAM role, VPC, endpoints | nothing | None | All of it. |

### Stage 2: ingestion

| Production | Local | Fidelity | Cannot prove |
|---|---|---|---|
| Apache Hop pipeline, Staging to UTM | SQL inside `nfl_ingestion.load_staging_to_utm`, run as Postgres role `nfl_hop` | **Engine fidelity: none. Contract only.** | That any Hop pipeline exists or works. Hop's own error handling, logging, performance. |
| "Is Failed" gate | Airflow task `is_failed_gate` | Contract | That it reads a Hop result; here it reads the SQL load's counts and rejects. |
| PostgreSQL | PostgreSQL 16, one container | Same engine | Sizing, tuning, Multi-AZ, contention. |

### Stage 3: extraction

| Production | Local | Fidelity | Cannot prove |
|---|---|---|---|
| Spark job, UTM to Parquet | psycopg2 + pyarrow inside `nfl_extraction` tasks, reading as role `nfl_spark` | **Engine fidelity: none. Contract only.** | Anything about Spark: partitioned reads, shuffle, executor loss, skew, cluster sizing. |
| S3 | floci S3 | API shape | IAM, bucket policy, KMS, lifecycle rules, consistency under load. floci accepts any access key. |
| `_SUCCESS` and `manifest.json` | Same objects, written last | High | Behaviour of an interrupted publish (not injected). |
| "Is UTM" with retries | Airflow task `is_utm` with Airflow retries | Contract | Exponential backoff: the delay is fixed. |

### Stage 4: reporting

| Production | Local | Fidelity | Cannot prove |
|---|---|---|---|
| Spark reporting engine | Python inside `nfl_reporting.build_report` | **Engine fidelity: none. Contract only.** | Anything about Spark, or about report definitions read from a metadata store (there is one hard-coded report). |
| Checksum | SHA-256 sidecar and object metadata | High | nothing further |
| "Is Process Failed" | Airflow task `is_process_failed` | Contract | Its failure path: no test makes it fail. |

### Stage 5: delivery

| Production | Local | Fidelity | Cannot prove |
|---|---|---|---|
| Recipient's SFTP server | `nfl-sftp` container | Protocol | The recipient's authentication, host-key policy, quotas, directory rules, what they do with the file. |
| Temp name, verify, rename | Same sequence in `deliver_sftp` | High | A network failure mid-transfer. Retries: the task has none. Key-based login: the upload uses a password. |

### Stage 6: observability and alerting

| Production | Local | Fidelity | Cannot prove |
|---|---|---|---|
| Events and metrics store | Postgres table `observability.pipeline_event` | Contract | Retention, immutability, any managed metrics service. |
| Dashboard | none; the table is readable through `/checks/events` and `/checks/lineage` | None | That any person can see pipeline state. |
| Alerts to Operations | A `failed` row with `metrics.alert=true`, and a log line | Trigger only | That anyone is notified. No channel exists. |

### Platform

| Production | Local | Fidelity | Cannot prove |
|---|---|---|---|
| Airflow (managed or HA) | Airflow 2.10.4, one container, basic auth, one Admin user | Same software | MWAA authentication (it does not accept basic auth), scheduling (every DAG is trigger-only), HA. |
| IAM roles per component | Postgres and MySQL roles per component and per check; one shared floci key for S3 | Database roles enforced; S3 not | Any IAM policy. All DAG credentials sit in the one Airflow container. |
| Secrets manager | `infra/nfl.env`, generated by `scripts/init-env.sh`, gitignored | None | Rotation, injection, audit. |
| TLS | none | None | Encryption in transit. |

## 5. Least privilege as built

Postgres roles, from `infra/sql/03-roles.sql.tmpl` and `06-check-identities.sql.tmpl`, applied on every
`up.sh`:

| Role | Used by | Can |
|---|---|---|
| `nfl_replication` | JDBC sink | INSERT on the four staging tables. Cannot read them. |
| `nfl_hop` | `nfl_ingestion` | SELECT on staging; read and write on utm. |
| `nfl_spark` | `nfl_extraction` | SELECT on utm batch, dim and fact tables; read-only transactions. |
| `nfl_events` | every DAG's event writer | INSERT on `observability.pipeline_event`. Cannot read it. |
| `nfl_stub_reader` | DMS stub | SELECT on the three replicated staging tables. |
| `nfl_check_reconcile`, `nfl_check_quality`, `nfl_check_freshness`, `nfl_check_lineage`, `nfl_check_events` | one shim check each | SELECT on the columns that check reads, nothing else; read-only transactions. |
| `nfl_admin` | the database container itself and `up.sh` | Superuser. Not held by Airflow, the shim, the stub or Kafka Connect. |

The grant list of each `nfl_check_*` role is the `grants` list in the matching `shim/roles/*.yaml`.
Measured 2026-10-04: the live column grants of all five equal those lists exactly. The shim refuses to
start (exit 78) if a check's credential logs in as a different user than its role file declares, or if
two checks share a principal without both declaring it.

MySQL: `debezium` (replication rights, SELECT on `hawks.*`, INSERT and UPDATE on the heartbeat table
only), `nfl_check_replication_lag` (REPLICATION CLIENT only). SFTP: the upload user (password, write,
chrooted) and `nfl_check_delivery_sftp` (key pair, read-only); server host keys are pinned.

**What is declared but not enforced locally:** S3 scope. The five S3-using checks and the two S3-using
DAG stages each have their own variable names, and the role files carry an `aws_iam_policy` for each,
but floci evaluates no policy, so every S3 call proves reachability and content only.

## 6. Where to read more

- `docs/RUNBOOK.md`: start, run, read a failure, rerun, inject failures.
- `docs/COVERAGE.md`: every requirement ID against what exercises it.
- `docs/DECISIONS.md`: why it is built this way.
- `infra/README.md`, `shim/README.md`, `stub/README.md`, `bruno/README.md`: each component in detail.
