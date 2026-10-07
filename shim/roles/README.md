# Role declarations

One file per shim endpoint. Each file is the single source of truth for:

- `identity` — the local principal (Postgres role, Kafka Connect user, S3 access key, SFTP user)
- `credential_env` — the env var the endpoint reads. The shim refuses to start if it is missing.
- `grants` — the exact backing-store permissions the identity requires, as DDL/ACL statements
- `aws_iam_policy` — the production IAM policy document this identity maps to

The shim loads these at startup, validates that every declared `credential_env` is present,
and serves them verbatim from `GET /checks`. The declaration is not documentation about the
code — it *is* the code's configuration. An endpoint with no role file cannot be registered.

## Invariants enforced at startup (see `shim/roles.py`)

1. Every registered endpoint has exactly one role file.
2. Every role file's `credential_env` (and companions) is set in the environment.
3. No two endpoints claim the same `credential_env` variable.
4. For `identity.kind` `postgres_role` and `mysql_user`, the user inside the DSN equals
   `identity.name`. A role file cannot describe one login while the environment supplies
   another.
5. No two endpoints resolve to the same principal (Postgres user, MySQL user, S3 access
   key id, Connect basic-auth user, SFTP user) unless EVERY one of them lists the others
   under `shared_with` and gives a `shared_rationale`. Distinct variable names carrying
   one login are refused.
6. Every role declares `grants` and `aws_iam_policy`. Empty is not permitted; a role
   needing no backing store declares `grants: []` with a `grants_rationale`.

The guard compares who authenticates. It does not read the database's ACLs, so it does
not prove that a login holds only the grants its file lists.

## Postgres roles the shim expects

One login role per check, each read-only (`default_transaction_read_only = on`), with
the DDL in that file's `grants` list:

| Role file | Variable | Postgres role |
|---|---|---|
| `ingestion_reconcile.yaml` | `SHIM_INGESTION_RECONCILE_PG_DSN` | `nfl_check_reconcile` |
| `ingestion_quality.yaml` | `SHIM_INGESTION_QUALITY_PG_DSN` | `nfl_check_quality` |
| `ingestion_freshness.yaml` | `SHIM_INGESTION_FRESHNESS_PG_DSN` | `nfl_check_freshness` |
| `lineage.yaml` | `SHIM_LINEAGE_PG_DSN` | `nfl_check_lineage` |
| `events.yaml` | `SHIM_EVENTS_PG_DSN` | `nfl_check_events` |
| `trace.yaml` | `SHIM_TRACE_PG_DSN` | `nfl_check_trace` |
| `alert_delivery.yaml` | `SHIM_ALERT_DELIVERY_PG_DSN` | `nfl_check_alert_delivery` |
| `monitor.yaml` | `SHIM_MONITOR_PG_DSN` | `nfl_check_monitor` |

## Declared vs enforced

`grants` and `aws_iam_policy` describe the scope each identity is meant to have.
`local_enforcement` says which part of that the LOCAL backing store really enforces:

| Backing store | Enforced locally | Not enforced locally |
|---|---|---|
| Postgres | one role per check, column-level SELECT, read-only, when the infra layer creates the roles from the `grants` lists | nothing the shim can see; extra grants in the database would not be detected by the shim |
| MySQL | `nfl_check_replication_lag` has `REPLICATION CLIENT` only | — |
| SFTP | key-only login, `internal-sftp -R`, read-only mount, pinned host keys | — |
| S3 (floci) | nothing: floci accepts any key for every action. S3 checks prove reachability and object content only, never an IAM identity | every S3 grant; the five S3 role files share one key and declare it (`shared_with`) |
| Kafka Connect | nothing: no auth or ACL on the local REST API | every `connect.rest.acl` grant |

`GET /checks` returns `local_enforcement`, `shared_with` and `shared_rationale` for every
endpoint, so the gap is visible at the API and not only in this file.

## Lifting to production

`aws_iam_policy` is a complete IAM policy document. In production each endpoint becomes its
own ECS task / Lambda with that policy as its task role, and `credential_env` is sourced from
Secrets Manager rather than `infra/nfl.env`. The Postgres `grants` become the DDL run against
the real RDS instance for that role. Nothing in `grants` is wider than the endpoint's
queries need.
