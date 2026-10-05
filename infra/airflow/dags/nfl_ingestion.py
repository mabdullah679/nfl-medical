from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

from nfl_common import (
    default_args,
    env,
    env_int,
    interval_from_context,
    log,
    pg,
    push_interval,
    record_event,
)

DOC_MD = """
### nfl_ingestion - stage 2: staging -> utm

**Local stand-in for Apache Hop.** In production this stage is a Hop pipeline run on
ECS; here the same contract is implemented in SQL executed as the `nfl_hop` Postgres
role (SELECT on `staging`, read/write on `utm`, nothing else).

Trigger with `conf = {"interval_start": "<ISO-8601 UTC midnight>", "interval_end":
"<ISO-8601 UTC midnight>"}`. The window is half-open `[interval_start, interval_end)`
and selects rows by business event time: `patient_account.opened_at`,
`account_transaction.posted_at`, `payment.received_at`.

Rules applied to every staging row in the window:

* staging is an append-only CDC log, so only the newest version of each source primary
  key is considered;
* a row is **rejected** to `utm.ingestion_reject` (never loaded) when its business key
  is null/blank (`null_business_key`), when the same business key is carried by more
  than one source row (`duplicate_business_key` - every carrier is rejected, because
  nothing says which one is right), when a NOT NULL utm column would be null
  (`missing_required_field`), or when a transaction/payment points at an account that
  does not exist anywhere in staging (`orphan_account`);
* every other row is upserted into utm on its business key, in one transaction.

`is_failed_gate` fails the run - and marks `utm.ingestion_batch` FAILED - when any row
was rejected or the counts do not reconcile. Extraction refuses to run for an interval
whose batch is not SUCCEEDED.

A rerun of the same interval reuses the same batch row, replaces its rejects and
upserts the same rows: identical counts, no duplicates.
"""

STAGE_BY_TASK = {"pre_check": "replication"}

TABLES: list[dict[str, Any]] = [
    {
        "name": "patient_account",
        "staging": "staging.patient_account",
        "target": "utm.dim_patient_account",
        "pk": "account_id",
        "bk": "account_number",
        "event": "opened_at",
        "target_event": "opened_at",
        "required": ["facility_code", "account_status", "balance_cents", "opened_at", "updated_at"],
        "columns": {
            "account_number": "w.account_number",
            "mrn": "w.mrn",
            "facility_code": "w.facility_code",
            "patient_last_name": "w.patient_last_name",
            "patient_first_name": "w.patient_first_name",
            "guarantor_name": "w.guarantor_name",
            "account_status": "w.account_status",
            "balance_cents": "w.balance_cents",
            "opened_at": "w.opened_at",
            "closed_at": "w.closed_at",
            "source_updated_at": "w.updated_at",
        },
        "orphan_check": False,
    },
    {
        "name": "account_transaction",
        "staging": "staging.account_transaction",
        "target": "utm.fact_account_transaction",
        "pk": "transaction_id",
        "bk": "transaction_ref",
        "event": "posted_at",
        "target_event": "posted_at",
        "required": ["account_number", "facility_code", "transaction_type", "posted_at", "amount_cents", "updated_at"],
        "columns": {
            "transaction_ref": "w.transaction_ref",
            "account_number": "w.account_number",
            "account_sk": "(SELECT d.account_sk FROM utm.dim_patient_account d WHERE d.account_number = w.account_number)",
            "facility_code": "w.facility_code",
            "transaction_type": "w.transaction_type",
            "cpt_code": "w.cpt_code",
            "service_date": "w.service_date",
            "posted_at": "w.posted_at",
            "amount_cents": "w.amount_cents",
            "payer_name": "w.payer_name",
            "source_updated_at": "w.updated_at",
        },
        "orphan_check": True,
    },
    {
        "name": "payment",
        "staging": "staging.payment",
        "target": "utm.fact_payment",
        "pk": "payment_id",
        "bk": "payment_ref",
        "event": "received_at",
        "target_event": "received_at",
        "required": ["account_number", "facility_code", "payment_method", "payment_channel", "amount_cents", "received_at", "updated_at"],
        "columns": {
            "payment_ref": "w.payment_ref",
            "account_number": "w.account_number",
            "account_sk": "(SELECT d.account_sk FROM utm.dim_patient_account d WHERE d.account_number = w.account_number)",
            "facility_code": "w.facility_code",
            "payment_method": "w.payment_method",
            "payment_channel": "w.payment_channel",
            "amount_cents": "w.amount_cents",
            "settled_at": "w.settled_at",
            "received_at": "w.received_at",
            "reversal_of_ref": "w.reversal_of_ref",
            "source_updated_at": "w.updated_at",
        },
        "orphan_check": True,
    },
]


def _classified_cte(t: dict[str, Any]) -> str:
    """Newest CDC version per source row, restricted to the window, each tagged with
    the reason it must be rejected (NULL when it is loadable)."""
    required_null = " OR ".join(f"l.{c} IS NULL" for c in t["required"])
    orphan = (
        "WHEN NOT EXISTS (SELECT 1 FROM staging.patient_account a "
        "WHERE a.account_number = l.account_number) THEN 'orphan_account'"
        if t["orphan_check"]
        else ""
    )
    return f"""
    latest AS (
      SELECT DISTINCT ON ({t['pk']}) *
      FROM {t['staging']}
      ORDER BY {t['pk']}, dms_loaded_at DESC, updated_at DESC
    ),
    dup AS (
      SELECT {t['bk']} AS bk FROM latest
      WHERE {t['bk']} IS NOT NULL AND btrim({t['bk']}) <> ''
      GROUP BY {t['bk']} HAVING count(*) > 1
    ),
    w AS (
      SELECT l.*,
        CASE
          WHEN l.{t['bk']} IS NULL OR btrim(l.{t['bk']}) = '' THEN 'null_business_key'
          WHEN l.{t['bk']} IN (SELECT bk FROM dup) THEN 'duplicate_business_key'
          WHEN {required_null} THEN 'missing_required_field'
          {orphan}
        END AS reject_reason
      FROM latest l
      WHERE l.{t['event']} >= %(lo)s AND l.{t['event']} < %(hi)s
    )
    """


def _load_sql(t: dict[str, Any]) -> str:
    cols = list(t["columns"])
    staging_cols_sql = ", ".join(t["columns"].values())
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != t["bk"])
    return f"""
    WITH {_classified_cte(t)},
    rej AS (
      INSERT INTO utm.ingestion_reject
        (utm_batch_id, source_table, reject_reason, business_key, raw_row)
      SELECT %(batch_id)s, %(source_table)s, w.reject_reason, w.{t['bk']},
             to_jsonb(w) - 'reject_reason'
      FROM w WHERE w.reject_reason IS NOT NULL
      RETURNING 1
    ),
    ins AS (
      INSERT INTO {t['target']} ({', '.join(cols)}, utm_batch_id, utm_loaded_at)
      SELECT {staging_cols_sql}, %(batch_id)s, now()
      FROM w WHERE w.reject_reason IS NULL
      ON CONFLICT ({t['bk']}) DO UPDATE
        SET {updates}, utm_batch_id = EXCLUDED.utm_batch_id,
            utm_loaded_at = EXCLUDED.utm_loaded_at
      RETURNING 1
    )
    SELECT (SELECT count(*) FROM w), (SELECT count(*) FROM ins), (SELECT count(*) FROM rej)
    """


def validate_conf(**context: Any) -> None:
    interval = push_interval(context)
    context["ti"].xcom_push(key="batch_key", value=interval.prefix)
    log.info("interval [%s, %s) -> batch_key %s", interval.start_iso, interval.end_iso, interval.prefix)


def _connector_states() -> dict[str, Any]:
    base = env("NFL_CONNECT_URL").rstrip("/")
    names = [
        n.strip()
        for n in os.environ.get(
            "NFL_CONNECTORS", "nfl-hawks-mysql-source,nfl-staging-jdbc-sink"
        ).split(",")
        if n.strip()
    ]
    states: dict[str, Any] = {}
    for name in names:
        try:
            with urllib.request.urlopen(f"{base}/connectors/{name}/status", timeout=10) as resp:
                doc = json.loads(resp.read())
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            raise AirflowFailException(
                f"cannot read status of connector {name} from Kafka Connect: {exc}"
            ) from exc
        states[name] = {
            "connector": doc.get("connector", {}).get("state"),
            "tasks": [task.get("state") for task in doc.get("tasks", [])],
        }
    return states


def _heartbeat_freshness(cur: Any) -> dict[str, Any]:
    """End-to-end replication freshness from staging.replication_heartbeat: the
    newest beat of every source must be younger than the limit. An empty table
    means nothing is beating, so freshness cannot be enforced; that is reported as
    staleness_enforced=false, or fails the run when the heartbeat is required."""
    max_age = env_int("NFL_REPLICATION_HEARTBEAT_MAX_AGE_SECONDS", 300)
    if max_age <= 0:
        raise AirflowFailException(
            f"NFL_REPLICATION_HEARTBEAT_MAX_AGE_SECONDS={max_age} is not a positive "
            f"number of seconds; a non-positive limit would switch the freshness check off"
        )
    required = os.environ.get("NFL_REPLICATION_HEARTBEAT_REQUIRED", "").strip().lower() in {
        "1", "true", "yes",
    }
    source = os.environ.get("NFL_REPLICATION_HEARTBEAT_SOURCE", "").strip()

    cur.execute(
        "SELECT source_name, max(beat_at), extract(epoch FROM now() - max(beat_at)) "
        "FROM staging.replication_heartbeat "
        "WHERE %(source)s = '' OR source_name = %(source)s "
        "GROUP BY source_name ORDER BY source_name",
        {"source": source},
    )
    sources = {
        name: {"last_beat_at": beat.isoformat(), "age_seconds": round(float(age), 1)}
        for name, beat, age in cur.fetchall()
    }
    result: dict[str, Any] = {
        "staleness_enforced": bool(sources),
        "heartbeat_max_age_seconds": max_age,
        "heartbeat_required": required,
        "heartbeat_source_filter": source or None,
        "heartbeat_sources": sources,
    }
    if not sources:
        scope = f"for source {source!r}" if source else "at all"
        if required:
            raise AirflowFailException(
                f"staging.replication_heartbeat holds no beat {scope} and "
                f"NFL_REPLICATION_HEARTBEAT_REQUIRED is set: replication freshness "
                f"cannot be established"
            )
        result["staleness_not_enforced_reason"] = (
            f"staging.replication_heartbeat holds no beat {scope}"
        )
        return result

    stale = {n: s for n, s in sources.items() if s["age_seconds"] > max_age}
    if stale:
        raise AirflowFailException(
            f"replication is stale: newest heartbeat older than {max_age}s for "
            + ", ".join(
                f"{n} (last beat {s['last_beat_at']}, {s['age_seconds']:.0f}s ago)"
                for n, s in stale.items()
            )
        )
    result["heartbeat_age_seconds"] = max(s["age_seconds"] for s in sources.values())
    return result


def pre_check(**context: Any) -> None:
    interval = interval_from_context(context)
    ti = context["ti"]

    states = _connector_states()
    unhealthy = {
        name: s
        for name, s in states.items()
        if s["connector"] != "RUNNING" or not s["tasks"] or any(x != "RUNNING" for x in s["tasks"])
    }
    if unhealthy:
        raise AirflowFailException(f"replication connector(s) not RUNNING: {unhealthy}")

    window_rows: dict[str, int] = {}
    with pg("HOP") as conn, conn.cursor() as cur:
        freshness = _heartbeat_freshness(cur)
        cur.execute(
            "SELECT max(m), extract(epoch FROM now() - max(m)) FROM ("
            " SELECT max(dms_loaded_at) m FROM staging.patient_account"
            " UNION ALL SELECT max(dms_loaded_at) FROM staging.account_transaction"
            " UNION ALL SELECT max(dms_loaded_at) FROM staging.payment) s"
        )
        last_landed, age_seconds = cur.fetchone()
        for t in TABLES:
            cur.execute(
                f"SELECT count(DISTINCT {t['pk']}) FROM {t['staging']} "
                f"WHERE {t['event']} >= %s AND {t['event']} < %s",
                (interval.start, interval.end),
            )
            window_rows[t["name"]] = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM utm.ingestion_batch")
        utm_batches = int(cur.fetchone()[0])

    if last_landed is None:
        raise AirflowFailException("staging is empty: replication has never landed a row")

    max_age = env_int("NFL_STAGING_MAX_AGE_SECONDS", 0)
    if max_age > 0 and float(age_seconds) > max_age:
        raise AirflowFailException(
            f"staging is stale: last row landed {float(age_seconds):.0f}s ago, limit {max_age}s"
        )
    if sum(window_rows.values()) == 0:
        raise AirflowFailException(
            f"staging holds no rows for [{interval.start_iso}, {interval.end_iso}); "
            f"there is nothing to ingest and an empty load is not a success"
        )

    metrics = {
        "connectors": states,
        **freshness,
        "staging_last_landed_at": last_landed.isoformat(),
        "staging_age_seconds": round(float(age_seconds), 1),
        "staging_max_age_enforced_seconds": max_age or None,
        "staging_window_source_rows": window_rows,
        "utm_reachable": True,
        "utm_batches_seen": utm_batches,
    }
    if freshness["staleness_enforced"]:
        freshness_text = (
            f"replication heartbeat {freshness['heartbeat_age_seconds']}s old "
            f"(limit {freshness['heartbeat_max_age_seconds']}s)"
        )
    else:
        freshness_text = (
            f"replication freshness NOT enforced "
            f"({freshness['staleness_not_enforced_reason']})"
        )
    ti.xcom_push(key="pre_check", value=metrics)
    record_event(
        context,
        stage="replication",
        status="success",
        summary=(
            f"connectors RUNNING; {freshness_text}; "
            f"staging last landed {metrics['staging_age_seconds']}s ago; "
            f"{sum(window_rows.values())} source rows in window"
        ),
        metrics=metrics,
    )


def _open_batch(conn: Any, interval: Any) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO utm.ingestion_batch (batch_key, window_start, window_end, status) "
            "VALUES (%s, %s, %s, 'RUNNING') "
            "ON CONFLICT (batch_key) DO UPDATE SET "
            "  window_start = EXCLUDED.window_start, window_end = EXCLUDED.window_end, "
            "  started_at = now(), finished_at = NULL, status = 'RUNNING', "
            "  rows_read = 0, rows_loaded = 0, rows_rejected = 0, failure_reason = NULL "
            "RETURNING batch_id",
            (interval.prefix, interval.start, interval.end),
        )
        batch_id = int(cur.fetchone()[0])
    conn.commit()
    return batch_id


def _fail_batch(batch_id: int, reason: str) -> None:
    with pg("HOP") as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE utm.ingestion_batch SET status = 'FAILED', finished_at = now(), "
            "failure_reason = %s WHERE batch_id = %s",
            (reason[:2000], batch_id),
        )
        conn.commit()


def load_staging_to_utm(**context: Any) -> None:
    interval = interval_from_context(context)
    ti = context["ti"]

    with pg("HOP") as conn:
        batch_id = _open_batch(conn, interval)
        ti.xcom_push(key="batch_id", value=batch_id)
        per_table: dict[str, dict[str, int]] = {}
        try:
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur.execute(
                    "DELETE FROM utm.ingestion_reject WHERE utm_batch_id = %s", (batch_id,)
                )
                for t in TABLES:
                    cur.execute(
                        _load_sql(t),
                        {
                            "lo": interval.start,
                            "hi": interval.end,
                            "batch_id": batch_id,
                            "source_table": t["name"],
                        },
                    )
                    read, loaded, rejected = (int(v) for v in cur.fetchone())
                    per_table[t["name"]] = {
                        "rows_read": read,
                        "rows_loaded": loaded,
                        "rows_rejected": rejected,
                    }
                # Facts loaded before their account arrived carry a NULL account_sk;
                # resolve any the dimension can now answer.
                for fact in ("utm.fact_account_transaction", "utm.fact_payment"):
                    cur.execute(
                        f"UPDATE {fact} f SET account_sk = d.account_sk "
                        "FROM utm.dim_patient_account d "
                        "WHERE f.account_sk IS NULL AND d.account_number = f.account_number"
                    )
                totals = {
                    k: sum(v[k] for v in per_table.values())
                    for k in ("rows_read", "rows_loaded", "rows_rejected")
                }
                cur.execute(
                    "UPDATE utm.ingestion_batch SET rows_read = %s, rows_loaded = %s, "
                    "rows_rejected = %s WHERE batch_id = %s",
                    (totals["rows_read"], totals["rows_loaded"], totals["rows_rejected"], batch_id),
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            _fail_batch(batch_id, f"load aborted and rolled back: {type(exc).__name__}: {exc}")
            raise

    ti.xcom_push(key="load_by_table", value=per_table)
    for key, value in totals.items():
        ti.xcom_push(key=key, value=value)
    log.info("batch %s loaded: %s", batch_id, per_table)


def data_quality(**context: Any) -> None:
    interval = interval_from_context(context)
    ti = context["ti"]
    batch_id = int(ti.xcom_pull(task_ids="load_staging_to_utm", key="batch_id"))
    params = {"lo": interval.start, "hi": interval.end}

    tables: dict[str, Any] = {}
    with pg("HOP") as conn, conn.cursor() as cur:
        for t in TABLES:
            cur.execute(
                f"WITH {_classified_cte(t)} "
                "SELECT count(*), "
                f" count(*) FILTER (WHERE {t['bk']} IS NULL OR btrim({t['bk']}) = ''), "
                " count(*) FILTER (WHERE reject_reason = 'duplicate_business_key'), "
                f" count(DISTINCT {t['bk']}) FILTER (WHERE reject_reason = 'duplicate_business_key') "
                "FROM w",
                params,
            )
            source_rows, null_keys, dup_rows, dup_groups = (int(v) for v in cur.fetchone())

            cur.execute(
                f"SELECT count(*), count(*) FILTER (WHERE {t['bk']} IS NULL), "
                f" count(*) - count(DISTINCT {t['bk']}) "
                f"FROM {t['target']} WHERE {t['target_event']} >= %(lo)s "
                f"AND {t['target_event']} < %(hi)s AND utm_batch_id = %(batch)s",
                {**params, "batch": batch_id},
            )
            utm_rows, utm_null_keys, utm_dup_keys = (int(v) for v in cur.fetchone())

            cur.execute(
                "SELECT reject_reason, count(*) FROM utm.ingestion_reject "
                "WHERE utm_batch_id = %s AND source_table = %s GROUP BY 1 ORDER BY 1",
                (batch_id, t["name"]),
            )
            rejects = {reason: int(n) for reason, n in cur.fetchall()}
            rejected = sum(rejects.values())

            tables[t["name"]] = {
                "staging_rows": source_rows,
                "utm_rows": utm_rows,
                "rejected_rows": rejected,
                "unaccounted_rows": source_rows - utm_rows - rejected,
                "staging_null_business_keys": null_keys,
                "staging_duplicate_business_key_rows": dup_rows,
                "staging_duplicate_business_key_groups": dup_groups,
                "utm_null_business_keys": utm_null_keys,
                "utm_duplicate_business_keys": utm_dup_keys,
                "rejects_by_reason": rejects,
            }

    def total(field: str) -> int:
        return sum(v[field] for v in tables.values())

    quality = {
        "batch_id": batch_id,
        "tables": tables,
        "staging_rows": total("staging_rows"),
        "utm_rows": total("utm_rows"),
        "rejected_rows": total("rejected_rows"),
        "unaccounted_rows": sum(abs(v["unaccounted_rows"]) for v in tables.values()),
        "null_business_keys": total("staging_null_business_keys"),
        "duplicate_business_key_groups": total("staging_duplicate_business_key_groups"),
        "utm_null_business_keys": total("utm_null_business_keys"),
        "utm_duplicate_business_keys": total("utm_duplicate_business_keys"),
    }
    problems: list[str] = []
    if quality["unaccounted_rows"]:
        problems.append(
            f"reconciliation: {quality['unaccounted_rows']} staging row(s) neither loaded nor rejected"
        )
    if quality["rejected_rows"]:
        by_reason: dict[str, int] = {}
        for v in tables.values():
            for reason, n in v["rejects_by_reason"].items():
                by_reason[reason] = by_reason.get(reason, 0) + n
        problems.append(
            f"{quality['rejected_rows']} row(s) rejected ("
            + ", ".join(f"{n} {r}" for r, n in sorted(by_reason.items()))
            + ")"
        )
    if quality["utm_null_business_keys"] or quality["utm_duplicate_business_keys"]:
        problems.append(
            f"utm holds {quality['utm_null_business_keys']} null and "
            f"{quality['utm_duplicate_business_keys']} duplicate business key(s)"
        )
    if quality["utm_rows"] == 0:
        problems.append("no row reached utm")
    quality["problems"] = problems
    quality["passed"] = not problems

    ti.xcom_push(key="quality", value=quality)
    ti.xcom_push(key="quality_passed", value=quality["passed"])
    log.info("quality: %s", json.dumps(quality, sort_keys=True))


def is_failed_gate(**context: Any) -> None:
    ti = context["ti"]
    quality = ti.xcom_pull(task_ids="data_quality", key="quality")
    batch_id = int(quality["batch_id"])
    failed = not quality["passed"]
    reason = "; ".join(quality["problems"]) if failed else None

    with pg("HOP") as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE utm.ingestion_batch SET status = %s, finished_at = now(), "
            "failure_reason = %s WHERE batch_id = %s",
            ("FAILED" if failed else "SUCCEEDED", reason, batch_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            raise AirflowFailException(
                f"batch {batch_id} no longer exists in utm.ingestion_batch"
            )
        conn.commit()

    ti.xcom_push(key="is_failed", value=failed)
    ti.xcom_push(key="batch_status", value="FAILED" if failed else "SUCCEEDED")
    if failed:
        raise AirflowFailException(f"Is Failed = YES for batch {batch_id}: {reason}")


def record_success(**context: Any) -> None:
    ti = context["ti"]
    quality = ti.xcom_pull(task_ids="data_quality", key="quality")
    record_event(
        context,
        stage="ingestion",
        status="success",
        summary=(
            f"batch {quality['batch_id']} SUCCEEDED: {quality['utm_rows']} rows in utm, "
            f"{quality['rejected_rows']} rejected, reconciled"
        ),
        metrics=quality,
    )


with DAG(
    dag_id="nfl_ingestion",
    description="Stage 2: staging -> utm (Apache Hop stand-in), quality checks, Is Failed gate",
    doc_md=DOC_MD,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(STAGE_BY_TASK, "ingestion"),
    tags=["nfl", "stage-2", "ingestion"],
) as dag:
    t_validate = PythonOperator(task_id="validate_conf", python_callable=validate_conf)
    t_pre = PythonOperator(task_id="pre_check", python_callable=pre_check)
    t_load = PythonOperator(task_id="load_staging_to_utm", python_callable=load_staging_to_utm)
    t_quality = PythonOperator(task_id="data_quality", python_callable=data_quality)
    t_gate = PythonOperator(task_id="is_failed_gate", python_callable=is_failed_gate)
    t_event = PythonOperator(task_id="record_event", python_callable=record_success)

    t_validate >> t_pre >> t_load >> t_quality >> t_gate >> t_event
