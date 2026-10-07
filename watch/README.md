# nfl-watch

The part of the platform that looks without being asked, and the part that delivers alerts.

It is a separate container and not an Airflow DAG, because the orchestrator is one of the things it
watches. Everything is in `app.py`.

## What one tick does (every `NFL_WATCH_INTERVAL_SECONDS`, 15)

1. Runs three probes.

   | Probe | Reads | Levels |
   |---|---|---|
   | `replication_heartbeat` | newest beat per source in `staging.replication_heartbeat` | `ok` up to the limit (300 s), `warning` above it, `critical` above twice it or when no beat exists |
   | `connector_status` | Kafka Connect status of both connectors | `critical` when a connector or one of its tasks is not RUNNING |
   | `orchestrator` | Airflow `/health` | `critical` when the scheduler or the metadata database is not healthy |

2. Appends each reading to `observability.monitor_sample`.
3. When a probe's level gets worse, appends an alert to `observability.pipeline_event`; when it returns to
   `ok`, appends a recovery (severity `info`). Both carry the incident id `nfl_watch_<probe>_<epoch>` as
   `run_id` and `correlation_id`.
4. Emails every event whose metrics carry `notify` and that has no successful delivery, and appends the
   attempt to `observability.alert_delivery`.

A probe that cannot run keeps its previous level for `NFL_WATCH_UNKNOWN_TICKS` (3) ticks in a row and then
becomes `critical`.

A failed send is retried after 30 s, 60 s, 120 s and so on, capped at 600 s, for `NFL_ALERT_MAX_ATTEMPTS`
(10) attempts. Events older than `NFL_ALERT_DISPATCH_MAX_AGE_HOURS` (24) are not sent.

## Identities

| Role | Used for | Grants |
|---|---|---|
| `nfl_monitor` | probes and their events | SELECT `(source_name, beat_at)` on the heartbeat; SELECT, INSERT, DELETE on `monitor_sample`; INSERT on `pipeline_event` |
| `nfl_notifier` | alert dispatch | SELECT on `pipeline_event`; SELECT, INSERT on `alert_delivery` |

Both are created by `infra/sql/06-check-identities.sql.tmpl`. The container holds no pipeline data role,
no S3 key and no Airflow credential.

## Reading it

- `GET /checks/monitor` on the shim: is it alive, and what each probe last measured.
- `GET /checks/alerts/delivery` on the shim: every alert and its delivery state.
- `docker logs nfl-watch`, or log group `/nfl/nfl-watch`: state changes and delivery attempts. It logs
  nothing on a quiet tick.

## Proven (2026-10-07) and not

Proven by request: `bruno/90-chaos` scenario H (a paused connector is noticed, alerted, emailed and
recovered with nothing triggered) and scenarios B, J and K (DAG alerts delivered in 60 s or less).

Proven by hand: mail server stopped, watcher stopped, Airflow frozen, heartbeat limit lowered to 25 s and
10 s. Results are in `bruno/README.md`.

Not covered:

- An outage shorter than one tick is not seen.
- Nothing watches this service unless `/checks/monitor` is asked.
- A mail outage is not itself alerted.
- The recipient is the local inbox `nfl-mail`. Nobody is notified.
- The severity names and rules were chosen here, not taken from the specification.
