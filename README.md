# NFL Data Platform: local test harness

A local, runnable stand-in for the NFL reporting pipeline (Hawks MySQL, replication into PostgreSQL,
ingestion into the UTM schema, Parquet extraction to S3, report generation, SFTP delivery, all
orchestrated by Airflow), with a Bruno collection that exercises it end to end and injects failures.

It exists to test the pipeline's contract against the requirements specification before anything is
built in AWS. It is not the production pipeline, and a green run here does not show that the AWS
deployment works. Read "What this does not prove" before relying on a result.

## Start from nothing

Needs Docker with compose v2 or later, `python3`, `curl`, `lsof`, `ssh-keygen`, and floci (the local AWS
emulator) already answering on port 4566. This project does not start floci.

```bash
curl -fsS http://localhost:4566/_localstack/health   # floci must answer
scripts/init-env.sh                                  # once: generates infra/nfl.env and the SFTP keys
scripts/up.sh                                        # builds, starts, converges, proves; exit 0 = up
```

`up.sh` ends with "Stack is up" only when all nine containers are healthy, both replication connectors
are RUNNING and every check identity could log in. Any other ending is a failure with a named cause.

```bash
scripts/down.sh          # stop; data volumes kept
scripts/reset.sh -y      # wipe this project's volumes and its S3 objects, for a true cold start
```

The scripts act on the compose project `nfl` only. They never touch another project's containers or
volumes, and never floci itself.

## Run the tests

The Bruno collection is in `bruno/`. With the `bru` CLI:

```bash
cd bruno
set -a; . ../infra/nfl.env; set +a

bru run -r --exclude-tags chaos --env local \
  --env-var "airflow_password=${AIRFLOW_ADMIN_PASSWORD}" \
  --env-var "aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

bru run 90-chaos --env local \
  --env-var "airflow_password=${AIRFLOW_ADMIN_PASSWORD}" \
  --env-var "aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"
```

The first command is the normal run: it drives the clean interval (August 2026) through all three DAGs
and checks every stage. The second injects seven failure scenarios and restores each one. In the Bruno
desktop app, open the `bruno/` folder and pick the environment `local`; see `bruno/README.md`.

Last measured, 2026-10-04 UTC, bru 4.2.0: normal run 100 of 100 requests, 309 of 309 tests; chaos 57
of 57 requests, 160 of 160 tests. Since then one test that could not fail was removed from `60-observability/02` on 2026-10-05; the normal run measured that day, chaos not re-run, is 100 of 100 requests and 308 of 308 tests.

## What is in here

| Path | What |
|---|---|
| `scripts/` | `init-env.sh`, `up.sh`, `down.sh`, `reset.sh` |
| `infra/` | compose file, schemas, roles and grants, seed data, connector configs, the three Airflow DAGs |
| `shim/` | read-only checks over HTTP on port 8096, one database role per check |
| `stub/` | DMS-shaped API on port 8098 for request-shape tests; replicates nothing |
| `bruno/` | the test collection: 157 requests, 100 in the normal run and 57 in `90-chaos` |
| `docs/ARCHITECTURE.md` | every container and port, and what each stand-in cannot prove |
| `docs/RUNBOOK.md` | start, stop, reset, run, read a failure, rerun an interval, inject failures |
| `docs/COVERAGE.md` | all 61 requirement IDs against what exercises them |
| `docs/DECISIONS.md` | why it is built this way |
| `design/architecture.png` | the High Level Design the specification was derived from; kept locally and gitignored, not in the repository |

Local addresses, all bound to 127.0.0.1: Airflow 8095, shim 8096, Kafka Connect 8097, DMS stub 8098,
MySQL 3307, Postgres 5440, Kafka 9099, SFTP 2225. Credentials are generated into `infra/nfl.env`, which
is gitignored and never printed.

## What this does not prove

- **Real AWS DMS.** Replication is done by Debezium and a JDBC sink. The DMS endpoint is a stub that
  answers in DMS's shape and reports the Debezium connector's state.
- **Real Apache Hop and Spark.** Neither runs here. Ingestion, extraction and reporting are Python and
  SQL inside Airflow tasks. The contract between stages is tested; the engines are not.
- **IAM enforcement.** floci accepts any access key and evaluates no policy. The S3 checks prove that
  objects are reachable and correct, not that any identity is scoped. Postgres, MySQL and SFTP roles
  are enforced.
- **MWAA authentication.** The collection calls Airflow with HTTP basic auth, which Amazon MWAA does
  not accept. Scheduling is not exercised either: every DAG is trigger-only.
- **The `aws` Bruno environment.** It holds placeholders and has never been run against an account.
- **The Bruno desktop app.** Every measured run used the `bru` CLI.
- **Alert delivery and a dashboard.** An alert is a row in a Postgres table. Nobody is notified, and
  there is no dashboard.
- **Volume, performance, availability, recovery, encryption, secrets management.** One container per
  component, a few hundred seeded rows, no TLS, no KMS, no secrets manager.
- **A literal `git clone`.** Nothing is committed yet. The cold start (`reset.sh -y`, `up.sh`, normal
  run) was proven in place with an existing env file, and by the infra fix round from a clean copy of
  the files git would add.

`docs/COVERAGE.md` gives the verdict for each requirement: 9 covered, 11 covered through a stand-in,
17 partly covered, 24 not covered.
