from __future__ import annotations

import hashlib
import io
import json
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator

from nfl_common import (
    GATE_RETRIES,
    GATE_RETRY_DELAY,
    JOB_VERSION,
    MANIFEST_NAME,
    MANIFEST_SCHEMA_VERSION,
    SUCCESS_MARKER,
    bucket,
    default_args,
    interval_from_context,
    log,
    pg,
    push_interval,
    record_event,
    s3,
    s3_delete,
    s3_list,
)

DOC_MD = """
### nfl_extraction - stage 3: utm -> Silver + Gold Parquet on S3

**Local stand-in for the Spark job.** In production this stage is a Spark application
on EMR/Glue; here the same contract is implemented with psycopg2 + pyarrow. It reads
`utm` as the `nfl_spark` Postgres role (SELECT on `utm` only, read-only transactions)
and writes S3 with the extraction stage's own key pair.

Trigger with `conf = {"interval_start": "...", "interval_end": "..."}` (ISO-8601 UTC
midnights, half-open window, same values as `nfl_ingestion`).

Flow: `pre_check_ingestion` refuses to start unless the interval's
`utm.ingestion_batch` exists and is not FAILED. `is_utm` ("Is UTM") is the data-ready
gate and uses real Airflow retries: it passes only when the batch is SUCCEEDED and utm
holds exactly the rows that batch loaded. `write_parquet_temp` writes every Parquet
file under `_tmp/<prefix>/<run token>/`. `publish` removes the old `_SUCCESS`, replaces
the interval prefix with the temp objects, writes `manifest.json`, then writes
`_SUCCESS` last - the marker is the commit point, so a reader never sees `_SUCCESS`
next to a half-written prefix.

Layout under `s3://<bucket>/df_<YYYYMMDD>_<YYYYMMDD>/`:
`silver/<table>/part-00000.parquet` (row-level, one per utm table),
`gold/facility_daily_activity/part-00000.parquet`, `manifest.json`, `_SUCCESS`.

Output is deterministic: rows are ordered by business key, load-time and surrogate-key
columns are left out, and the manifest carries no wall-clock value. A rerun of the same
interval produces byte-identical Parquet and an identical manifest.
"""

STAGE_BY_TASK: dict[str, str] = {}

SILVER: dict[str, dict[str, Any]] = {
    "patient_account": {
        "sql": (
            "SELECT account_number, mrn, facility_code, patient_last_name, "
            "patient_first_name, guarantor_name, account_status, balance_cents, "
            "opened_at, closed_at, source_updated_at "
            "FROM utm.dim_patient_account WHERE opened_at >= %(lo)s AND opened_at < %(hi)s "
            "ORDER BY account_number"
        ),
        "schema": [
            ("account_number", "string"), ("mrn", "string"), ("facility_code", "string"),
            ("patient_last_name", "string"), ("patient_first_name", "string"),
            ("guarantor_name", "string"), ("account_status", "string"),
            ("balance_cents", "int64"), ("opened_at", "ts"), ("closed_at", "ts"),
            ("source_updated_at", "ts"),
        ],
        "utm_table": "utm.dim_patient_account",
        "event": "opened_at",
    },
    "account_transaction": {
        "sql": (
            "SELECT transaction_ref, account_number, facility_code, transaction_type, "
            "cpt_code, service_date, posted_at, amount_cents, payer_name, source_updated_at "
            "FROM utm.fact_account_transaction WHERE posted_at >= %(lo)s AND posted_at < %(hi)s "
            "ORDER BY transaction_ref"
        ),
        "schema": [
            ("transaction_ref", "string"), ("account_number", "string"),
            ("facility_code", "string"), ("transaction_type", "string"),
            ("cpt_code", "string"), ("service_date", "date"), ("posted_at", "ts"),
            ("amount_cents", "int64"), ("payer_name", "string"), ("source_updated_at", "ts"),
        ],
        "utm_table": "utm.fact_account_transaction",
        "event": "posted_at",
    },
    "payment": {
        "sql": (
            "SELECT payment_ref, account_number, facility_code, payment_method, "
            "payment_channel, amount_cents, settled_at, received_at, reversal_of_ref, "
            "source_updated_at "
            "FROM utm.fact_payment WHERE received_at >= %(lo)s AND received_at < %(hi)s "
            "ORDER BY payment_ref"
        ),
        "schema": [
            ("payment_ref", "string"), ("account_number", "string"),
            ("facility_code", "string"), ("payment_method", "string"),
            ("payment_channel", "string"), ("amount_cents", "int64"), ("settled_at", "ts"),
            ("received_at", "ts"), ("reversal_of_ref", "string"), ("source_updated_at", "ts"),
        ],
        "utm_table": "utm.fact_payment",
        "event": "received_at",
    },
}

GOLD_NAME = "facility_daily_activity"
GOLD_SQL = """
SELECT facility_code, activity_date,
       sum(accounts_opened)::bigint   AS accounts_opened,
       sum(transaction_count)::bigint AS transaction_count,
       sum(charge_cents)::bigint      AS charge_cents,
       sum(adjustment_cents)::bigint  AS adjustment_cents,
       sum(refund_cents)::bigint      AS refund_cents,
       sum(writeoff_cents)::bigint    AS writeoff_cents,
       sum(payment_count)::bigint     AS payment_count,
       sum(payment_cents)::bigint     AS payment_cents
FROM (
  SELECT facility_code, (opened_at AT TIME ZONE 'UTC')::date AS activity_date,
         1 AS accounts_opened, 0 AS transaction_count, 0 AS charge_cents,
         0 AS adjustment_cents, 0 AS refund_cents, 0 AS writeoff_cents,
         0 AS payment_count, 0 AS payment_cents
  FROM utm.dim_patient_account WHERE opened_at >= %(lo)s AND opened_at < %(hi)s
  UNION ALL
  SELECT facility_code, (posted_at AT TIME ZONE 'UTC')::date, 0, 1,
         CASE WHEN transaction_type = 'CHARGE' THEN amount_cents ELSE 0 END,
         CASE WHEN transaction_type = 'ADJUSTMENT' THEN amount_cents ELSE 0 END,
         CASE WHEN transaction_type = 'REFUND' THEN amount_cents ELSE 0 END,
         CASE WHEN transaction_type = 'WRITEOFF' THEN amount_cents ELSE 0 END,
         0, 0
  FROM utm.fact_account_transaction WHERE posted_at >= %(lo)s AND posted_at < %(hi)s
  UNION ALL
  SELECT facility_code, (received_at AT TIME ZONE 'UTC')::date, 0, 0, 0, 0, 0, 0, 1, amount_cents
  FROM utm.fact_payment WHERE received_at >= %(lo)s AND received_at < %(hi)s
) activity
GROUP BY facility_code, activity_date
ORDER BY facility_code, activity_date
"""
GOLD_SCHEMA = [
    ("facility_code", "string"), ("activity_date", "date"), ("accounts_opened", "int64"),
    ("transaction_count", "int64"), ("charge_cents", "int64"), ("adjustment_cents", "int64"),
    ("refund_cents", "int64"), ("writeoff_cents", "int64"), ("payment_count", "int64"),
    ("payment_cents", "int64"),
]


def _arrow_schema(spec: list[tuple[str, str]]) -> Any:
    import pyarrow as pa

    types = {
        "string": pa.string(),
        "int64": pa.int64(),
        "date": pa.date32(),
        "ts": pa.timestamp("us", tz="UTC"),
    }
    return pa.schema([(name, types[kind]) for name, kind in spec])


def _parquet_bytes(rows: list[tuple], spec: list[tuple[str, str]]) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = _arrow_schema(spec)
    columns = list(zip(*rows)) if rows else [[] for _ in spec]
    table = pa.table(
        [pa.array(list(col), type=field.type) for col, field in zip(columns, schema)],
        schema=schema,
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", write_statistics=True)
    return buf.getvalue()


def _temp_prefix(context: dict[str, Any], prefix: str) -> str:
    token = hashlib.sha256(context["dag_run"].run_id.encode()).hexdigest()[:16]
    return f"_tmp/{prefix}/{token}"


def validate_conf(**context: Any) -> None:
    interval = push_interval(context)
    ti = context["ti"]
    ti.xcom_push(key="marker_key", value=f"{interval.prefix}/{SUCCESS_MARKER}")
    ti.xcom_push(key="manifest_key", value=f"{interval.prefix}/{MANIFEST_NAME}")


def _batch(cur: Any, batch_key: str) -> tuple | None:
    cur.execute(
        "SELECT batch_id, status, rows_loaded, rows_rejected, failure_reason, finished_at "
        "FROM utm.ingestion_batch WHERE batch_key = %s",
        (batch_key,),
    )
    return cur.fetchone()


def pre_check_ingestion(**context: Any) -> None:
    interval = interval_from_context(context)
    with pg("SPARK") as conn, conn.cursor() as cur:
        row = _batch(cur, interval.prefix)
    if row is None:
        raise AirflowFailException(
            f"no utm.ingestion_batch for {interval.prefix}: ingestion has not run for "
            f"[{interval.start_iso}, {interval.end_iso})"
        )
    batch_id, status, rows_loaded, rows_rejected, reason, _ = row
    if status == "FAILED":
        raise AirflowFailException(
            f"ingestion batch {batch_id} for {interval.prefix} FAILED "
            f"({rows_rejected} rejected): {reason}"
        )
    ti = context["ti"]
    ti.xcom_push(key="batch_id", value=int(batch_id))
    ti.xcom_push(key="batch_status", value=status)


def is_utm(**context: Any) -> None:
    interval = interval_from_context(context)
    params = {"lo": interval.start, "hi": interval.end}
    counts: dict[str, int] = {}
    with pg("SPARK") as conn, conn.cursor() as cur:
        row = _batch(cur, interval.prefix)
        if row is None:
            raise AirflowFailException(f"ingestion batch for {interval.prefix} disappeared")
        batch_id, status, rows_loaded, _, reason, _ = row
        for name, spec in SILVER.items():
            cur.execute(
                f"SELECT count(*) FROM {spec['utm_table']} "
                f"WHERE {spec['event']} >= %(lo)s AND {spec['event']} < %(hi)s "
                "AND utm_batch_id = %(batch)s",
                {**params, "batch": batch_id},
            )
            counts[name] = int(cur.fetchone()[0])

    if status == "FAILED":
        raise AirflowFailException(f"ingestion batch {batch_id} FAILED: {reason}")
    if status != "SUCCEEDED":
        raise AirflowException(
            f"Is UTM = NO: ingestion batch {batch_id} is {status}, not SUCCEEDED yet"
        )
    total = sum(counts.values())
    if total == 0 or total != int(rows_loaded):
        raise AirflowException(
            f"Is UTM = NO: utm holds {total} rows for batch {batch_id} in the window, "
            f"the batch recorded {rows_loaded} loaded"
        )
    ti = context["ti"]
    ti.xcom_push(key="utm_row_counts", value=counts)
    ti.xcom_push(key="utm_rows", value=total)
    ti.xcom_push(key="batch_id", value=int(batch_id))


def write_parquet_temp(**context: Any) -> None:
    interval = interval_from_context(context)
    params = {"lo": interval.start, "hi": interval.end}
    temp = _temp_prefix(context, interval.prefix)
    client = s3("EXTRACTION")

    s3_delete(client, s3_list(client, f"{temp}/"))

    datasets: list[tuple[str, str, list[tuple], list[tuple[str, str]]]] = []
    with pg("SPARK") as conn, conn.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        for name, spec in SILVER.items():
            cur.execute(spec["sql"], params)
            datasets.append(("silver", name, cur.fetchall(), spec["schema"]))
        cur.execute(GOLD_SQL, params)
        datasets.append(("gold", GOLD_NAME, cur.fetchall(), GOLD_SCHEMA))

    layers: dict[str, dict[str, Any]] = {
        "silver": {"row_count": 0, "files": []},
        "gold": {"row_count": 0, "files": []},
    }
    for layer, name, rows, spec in datasets:
        body = _parquet_bytes(rows, spec)
        path = f"{layer}/{name}/part-00000.parquet"
        client.put_object(Bucket=bucket(), Key=f"{temp}/{path}", Body=body)
        layers[layer]["files"].append(
            {
                "path": path,
                "dataset": name,
                "rows": len(rows),
                "size_bytes": len(body),
                "checksum": hashlib.sha256(body).hexdigest(),
                "checksum_algorithm": "sha256",
            }
        )
        layers[layer]["row_count"] += len(rows)
        log.info("wrote s3://%s/%s/%s (%d rows, %d bytes)", bucket(), temp, path, len(rows), len(body))

    expected = context["ti"].xcom_pull(task_ids="is_utm", key="utm_rows")
    if layers["silver"]["row_count"] != expected:
        raise AirflowFailException(
            f"silver holds {layers['silver']['row_count']} rows but Is UTM counted "
            f"{expected}: utm changed between the gate and the read"
        )
    if layers["gold"]["row_count"] == 0:
        raise AirflowFailException("gold aggregation produced zero rows")

    ti = context["ti"]
    ti.xcom_push(key="temp_prefix", value=temp)
    ti.xcom_push(key="layers", value=layers)


def publish(**context: Any) -> None:
    interval = interval_from_context(context)
    ti = context["ti"]
    temp = ti.xcom_pull(task_ids="write_parquet_temp", key="temp_prefix")
    layers = ti.xcom_pull(task_ids="write_parquet_temp", key="layers")
    batch_id = ti.xcom_pull(task_ids="is_utm", key="batch_id")
    prefix = interval.prefix
    marker_key = f"{prefix}/{SUCCESS_MARKER}"
    manifest_key = f"{prefix}/{MANIFEST_NAME}"
    client = s3("EXTRACTION")

    client.delete_object(Bucket=bucket(), Key=marker_key)
    s3_delete(client, s3_list(client, f"{prefix}/"))

    for layer in layers.values():
        for entry in layer["files"]:
            client.copy_object(
                Bucket=bucket(),
                Key=f"{prefix}/{entry['path']}",
                CopySource={"Bucket": bucket(), "Key": f"{temp}/{entry['path']}"},
            )
            body = client.get_object(Bucket=bucket(), Key=f"{prefix}/{entry['path']}")["Body"].read()
            if hashlib.sha256(body).hexdigest() != entry["checksum"]:
                raise AirflowFailException(
                    f"published object {prefix}/{entry['path']} does not match the checksum "
                    f"of what was written; _SUCCESS withheld"
                )

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "job": "nfl_extraction",
        "job_version": JOB_VERSION,
        "interval": {"start": interval.start_date, "end": interval.end_date},
        "window": {"start": interval.start_iso, "end_exclusive": interval.end_iso},
        "source": {"schema": "utm", "ingestion_batch_key": prefix},
        "layers": layers,
    }
    manifest_body = json.dumps(manifest, indent=2, sort_keys=True).encode()
    client.put_object(
        Bucket=bucket(), Key=manifest_key, Body=manifest_body, ContentType="application/json"
    )
    client.put_object(Bucket=bucket(), Key=marker_key, Body=b"")
    s3_delete(client, s3_list(client, f"{temp}/"))

    manifest_sha = hashlib.sha256(manifest_body).hexdigest()
    ti.xcom_push(key="interval_prefix", value=prefix)
    ti.xcom_push(key="marker_key", value=marker_key)
    ti.xcom_push(key="manifest_key", value=manifest_key)
    ti.xcom_push(key="manifest_sha256", value=manifest_sha)
    ti.xcom_push(key="silver_row_count", value=layers["silver"]["row_count"])
    ti.xcom_push(key="gold_row_count", value=layers["gold"]["row_count"])
    ti.xcom_push(
        key="file_checksums",
        value={e["path"]: e["checksum"] for layer in layers.values() for e in layer["files"]},
    )
    ti.xcom_push(key="published_keys", value=s3_list(client, f"{prefix}/"))
    ti.xcom_push(key="ingestion_batch_id", value=batch_id)


def record_success(**context: Any) -> None:
    ti = context["ti"]
    pull = lambda key: ti.xcom_pull(task_ids="publish", key=key)
    metrics = {
        "interval_prefix": pull("interval_prefix"),
        "marker_key": pull("marker_key"),
        "manifest_key": pull("manifest_key"),
        "manifest_sha256": pull("manifest_sha256"),
        "silver_row_count": pull("silver_row_count"),
        "gold_row_count": pull("gold_row_count"),
        "file_checksums": pull("file_checksums"),
        "ingestion_batch_id": pull("ingestion_batch_id"),
        "job_version": JOB_VERSION,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
    }
    record_event(
        context,
        stage="extraction",
        status="success",
        summary=(
            f"published {metrics['interval_prefix']}: silver {metrics['silver_row_count']} rows, "
            f"gold {metrics['gold_row_count']} rows, _SUCCESS written"
        ),
        metrics=metrics,
    )


with DAG(
    dag_id="nfl_extraction",
    description="Stage 3: utm -> Silver/Gold Parquet on S3 (Spark stand-in), Is UTM gate, atomic publish",
    doc_md=DOC_MD,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(STAGE_BY_TASK, "extraction"),
    tags=["nfl", "stage-3", "extraction"],
) as dag:
    t_validate = PythonOperator(task_id="validate_conf", python_callable=validate_conf)
    t_pre = PythonOperator(task_id="pre_check_ingestion", python_callable=pre_check_ingestion)
    t_is_utm = PythonOperator(
        task_id="is_utm",
        python_callable=is_utm,
        retries=GATE_RETRIES,
        retry_delay=GATE_RETRY_DELAY,
    )
    t_write = PythonOperator(task_id="write_parquet_temp", python_callable=write_parquet_temp)
    t_publish = PythonOperator(task_id="publish", python_callable=publish)
    t_event = PythonOperator(task_id="record_event", python_callable=record_success)

    t_validate >> t_pre >> t_is_utm >> t_write >> t_publish >> t_event
