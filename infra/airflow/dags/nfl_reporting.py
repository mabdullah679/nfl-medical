from __future__ import annotations

import base64
import hashlib
import io
import json
import os
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator

from nfl_common import (
    CHECKSUM_SIDECAR_SUFFIX,
    GATE_RETRIES,
    GATE_RETRY_DELAY,
    MANIFEST_NAME,
    REPORT_CODE,
    REPORT_VERSION,
    SUCCESS_MARKER,
    bucket,
    default_args,
    env,
    env_int,
    interval_from_context,
    log,
    push_interval,
    record_event,
    s3,
    s3_exists,
)

DOC_MD = """
### nfl_reporting - stages 4 and 5: S3 -> report file -> SFTP

Trigger with `conf = {"interval_start": "...", "interval_end": "..."}` (ISO-8601 UTC
midnights, half-open window). Optional `"force_redeliver": true` re-sends a report
that is already on the SFTP server.

This DAG holds no database data role: it reads Parquet from S3 with the reporting
stage's own key pair and writes to SFTP as the delivery user.

* `wait_for_success_marker` - pre-check with real Airflow retries: the interval's
  `_SUCCESS` and `manifest.json` must exist.
* `build_report` - verifies every gold file against its manifest checksum, then
  writes `reports/<prefix>/<REPORT_CODE>_<start>_<end>_<version>.csv` plus a `.sha256`
  sidecar. The file is `H` header record, column line, `D` detail records, `T` trailer
  with the row count and control totals. Nothing in it depends on the wall clock, so a
  rerun produces the same bytes and the same checksum.
* `is_process_failed` - "Is Process Failed" gate: re-reads the stored report and fails
  unless it is non-empty, matches its sidecar checksum, and its trailer agrees with
  both its detail rows and the manifest's gold row count.
* `deliver_sftp` - uploads under `<name>.tmp`, verifies size and SHA-256 by reading
  the remote file back, then renames to the final name. If the final name already
  holds identical bytes the upload is skipped (no duplicate send); if it holds
  different bytes the task fails rather than overwrite a delivered file. Both are
  overridden by `force_redeliver`.
* `capture_events` - records the delivery event and metrics.
"""

STAGE_BY_TASK = {"deliver_sftp": "delivery", "capture_events": "delivery"}

REPORT_COLUMNS = [
    "facility_code", "activity_date", "accounts_opened", "transaction_count",
    "charge_cents", "adjustment_cents", "refund_cents", "writeoff_cents",
    "payment_count", "payment_cents",
]
GOLD_DATASET = "facility_daily_activity"


def _report_name(interval: Any) -> str:
    return f"{REPORT_CODE}_{interval.start_date}_{interval.end_date}_{REPORT_VERSION}.csv"


def _report_key(interval: Any) -> str:
    reports_prefix = os.environ.get("NFL_REPORTS_PREFIX", "reports")
    return f"{reports_prefix}/{interval.prefix}/{_report_name(interval)}"


def validate_conf(**context: Any) -> None:
    interval = push_interval(context)
    conf = context["dag_run"].conf or {}
    force = conf.get("force_redeliver", False)
    if not isinstance(force, bool):
        raise AirflowFailException("dag_run.conf.force_redeliver must be a JSON boolean")
    ti = context["ti"]
    ti.xcom_push(key="force_redeliver", value=force)
    ti.xcom_push(key="marker_key", value=f"{interval.prefix}/{SUCCESS_MARKER}")
    ti.xcom_push(key="report_name", value=_report_name(interval))
    ti.xcom_push(key="report_key", value=_report_key(interval))


def wait_for_success_marker(**context: Any) -> None:
    interval = interval_from_context(context)
    client = s3("REPORTING")
    marker_key = f"{interval.prefix}/{SUCCESS_MARKER}"
    manifest_key = f"{interval.prefix}/{MANIFEST_NAME}"
    if not s3_exists(client, marker_key):
        raise AirflowException(
            f"s3://{bucket()}/{marker_key} is absent: extraction has not published "
            f"[{interval.start_iso}, {interval.end_iso})"
        )
    if not s3_exists(client, manifest_key):
        raise AirflowException(
            f"s3://{bucket()}/{marker_key} exists but {manifest_key} does not"
        )
    ti = context["ti"]
    ti.xcom_push(key="marker_key", value=marker_key)
    ti.xcom_push(key="manifest_key", value=manifest_key)


def _csv_line(values: list[Any]) -> str:
    out = []
    for v in values:
        text = "" if v is None else str(v)
        if any(ch in text for ch in ',"\r\n'):
            text = '"' + text.replace('"', '""') + '"'
        out.append(text)
    return ",".join(out) + "\r\n"


def build_report(**context: Any) -> None:
    import pyarrow.parquet as pq

    interval = interval_from_context(context)
    client = s3("REPORTING")
    manifest_key = f"{interval.prefix}/{MANIFEST_NAME}"
    manifest_body = client.get_object(Bucket=bucket(), Key=manifest_key)["Body"].read()
    manifest = json.loads(manifest_body)

    gold_files = [
        f for f in manifest["layers"]["gold"]["files"] if f.get("dataset") == GOLD_DATASET
    ]
    if not gold_files:
        raise AirflowFailException(
            f"{manifest_key} lists no gold/{GOLD_DATASET} file; nothing to report on"
        )

    rows: list[dict[str, Any]] = []
    for entry in gold_files:
        key = f"{interval.prefix}/{entry['path']}"
        body = client.get_object(Bucket=bucket(), Key=key)["Body"].read()
        digest = hashlib.sha256(body).hexdigest()
        if digest != entry["checksum"]:
            raise AirflowFailException(
                f"s3://{bucket()}/{key} sha256 {digest} does not match manifest "
                f"{entry['checksum']}: refusing to report on corrupt input"
            )
        rows.extend(pq.read_table(io.BytesIO(body)).to_pylist())
    rows.sort(key=lambda r: (r["facility_code"], r["activity_date"]))

    totals = {
        c: sum(int(r[c]) for r in rows)
        for c in REPORT_COLUMNS
        if c not in ("facility_code", "activity_date")
    }
    lines = [
        _csv_line(
            ["H", REPORT_CODE, REPORT_VERSION, interval.start_iso, interval.end_iso,
             manifest.get("job_version"), manifest.get("schema_version")]
        ),
        _csv_line(["C"] + REPORT_COLUMNS),
    ]
    lines += [_csv_line(["D"] + [r[c] for c in REPORT_COLUMNS]) for r in rows]
    lines.append(
        _csv_line(
            ["T", len(rows), totals["transaction_count"], totals["charge_cents"],
             totals["payment_count"], totals["payment_cents"]]
        )
    )
    body = "".join(lines).encode("utf-8")
    digest = hashlib.sha256(body).hexdigest()

    name = _report_name(interval)
    key = _report_key(interval)
    client.put_object(
        Bucket=bucket(), Key=key, Body=body, ContentType="text/csv",
        Metadata={"sha256": digest, "report-code": REPORT_CODE, "report-version": REPORT_VERSION},
    )
    client.put_object(
        Bucket=bucket(), Key=f"{key}{CHECKSUM_SIDECAR_SUFFIX}",
        Body=f"{digest}  {name}\n".encode(), ContentType="text/plain",
    )

    ti = context["ti"]
    ti.xcom_push(key="report_name", value=name)
    ti.xcom_push(key="report_key", value=key)
    ti.xcom_push(key="report_checksum_key", value=f"{key}{CHECKSUM_SIDECAR_SUFFIX}")
    ti.xcom_push(key="report_sha256", value=digest)
    ti.xcom_push(key="report_size_bytes", value=len(body))
    ti.xcom_push(key="report_row_count", value=len(rows))
    ti.xcom_push(key="report_totals", value=totals)
    ti.xcom_push(key="manifest_sha256", value=hashlib.sha256(manifest_body).hexdigest())
    ti.xcom_push(key="manifest_gold_row_count", value=manifest["layers"]["gold"]["row_count"])
    log.info("wrote s3://%s/%s (%d rows, %d bytes, sha256 %s)", bucket(), key, len(rows), len(body), digest)


def is_process_failed(**context: Any) -> None:
    interval = interval_from_context(context)
    ti = context["ti"]
    key = ti.xcom_pull(task_ids="build_report", key="report_key")
    gold_rows = ti.xcom_pull(task_ids="build_report", key="manifest_gold_row_count")
    client = s3("REPORTING")

    body = client.get_object(Bucket=bucket(), Key=key)["Body"].read()
    sidecar = client.get_object(Bucket=bucket(), Key=f"{key}{CHECKSUM_SIDECAR_SUFFIX}")["Body"].read()
    expected = sidecar.decode().split()[0] if sidecar.strip() else ""
    digest = hashlib.sha256(body).hexdigest()
    lines = body.decode("utf-8").split("\r\n")
    if lines and lines[-1] == "":
        lines.pop()
    detail = [line for line in lines if line.startswith("D,")]
    trailer = lines[-1].split(",") if lines else []

    problems: list[str] = []
    if not body:
        problems.append("report is zero bytes")
    if digest != expected:
        problems.append(f"stored report sha256 {digest} != sidecar {expected!r}")
    if not lines or not lines[0].startswith(f"H,{REPORT_CODE},{REPORT_VERSION},"):
        problems.append("header record missing or wrong report code/version")
    if len(trailer) < 2 or trailer[0] != "T" or not trailer[1].isdigit():
        problems.append("trailer record missing")
    elif int(trailer[1]) != len(detail):
        problems.append(f"trailer declares {trailer[1]} rows, file holds {len(detail)}")
    if len(detail) == 0:
        problems.append("report holds no detail rows")
    if len(detail) != int(gold_rows):
        problems.append(f"report holds {len(detail)} rows, manifest gold layer has {gold_rows}")

    failed = bool(problems)
    ti.xcom_push(key="is_process_failed", value=failed)
    ti.xcom_push(key="report_sha256", value=digest)
    ti.xcom_push(key="report_row_count", value=len(detail))
    if failed:
        raise AirflowFailException(
            f"Is Process Failed = YES for {key}: " + "; ".join(problems)
        )

    record_event(
        context,
        stage="reporting",
        status="success",
        summary=f"report {key} built: {len(detail)} rows, {len(body)} bytes, sha256 {digest}",
        metrics={
            "report_key": key,
            "report_sha256": digest,
            "report_size_bytes": len(body),
            "report_row_count": len(detail),
            "interval_prefix": interval.prefix,
            "manifest_sha256": ti.xcom_pull(task_ids="build_report", key="manifest_sha256"),
        },
    )


def _sftp_connect() -> tuple[Any, Any, str]:
    import paramiko

    host = env("NFL_SFTP_HOST")
    port = env_int("NFL_SFTP_PORT", 22)
    transport = paramiko.Transport((host, port))
    transport.banner_timeout = 15
    try:
        transport.start_client(timeout=15)
        host_key = transport.get_remote_server_key()
        fingerprint = "SHA256:" + base64.b64encode(
            hashlib.sha256(host_key.asbytes()).digest()
        ).decode().rstrip("=")
        known_hosts_path = env("NFL_SFTP_KNOWN_HOSTS")
        known = paramiko.HostKeys()
        try:
            known.load(known_hosts_path)
        except OSError as exc:
            raise AirflowFailException(
                f"cannot read pinned SFTP host keys at {known_hosts_path}: {exc}; refusing "
                f"to send a report to an unverified server"
            ) from exc
        lookup = host if port == 22 else f"[{host}]:{port}"
        if not known.check(lookup, host_key):
            raise AirflowFailException(
                f"SFTP server {host}:{port} presented host key {fingerprint}, which is not "
                f"pinned for {lookup} in {known_hosts_path}: refusing to deliver"
            )
        transport.auth_password(env("NFL_SFTP_USER"), env("NFL_SFTP_PASSWORD"))
        return transport, paramiko.SFTPClient.from_transport(transport), fingerprint
    except Exception:
        transport.close()
        raise


def _remote_sha256(sftp: Any, path: str) -> tuple[str | None, int]:
    try:
        handle = sftp.open(path, "rb")
    except FileNotFoundError:
        return None, 0
    digest = hashlib.sha256()
    total = 0
    with handle:
        handle.prefetch()
        while True:
            chunk = handle.read(262144)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def deliver_sftp(**context: Any) -> None:
    ti = context["ti"]
    key = ti.xcom_pull(task_ids="build_report", key="report_key")
    name = ti.xcom_pull(task_ids="build_report", key="report_name")
    force = bool(ti.xcom_pull(task_ids="validate_conf", key="force_redeliver"))

    body = s3("REPORTING").get_object(Bucket=bucket(), Key=key)["Body"].read()
    digest = hashlib.sha256(body).hexdigest()
    gate_digest = ti.xcom_pull(task_ids="is_process_failed", key="report_sha256")
    if digest != gate_digest:
        raise AirflowFailException(
            f"s3 report {key} changed after the gate approved it ({gate_digest} -> {digest})"
        )

    base = env("NFL_SFTP_RECIPIENT_DIR").rstrip("/")
    final_path = f"{base}/{name}"
    temp_path = f"{final_path}{os.environ.get('NFL_SFTP_TEMP_SUFFIX', '.tmp')}"

    transport, sftp, fingerprint = _sftp_connect()
    try:
        existing_digest, existing_size = _remote_sha256(sftp, final_path)
        if existing_digest is not None and not force:
            if existing_digest != digest:
                raise AirflowFailException(
                    f"{final_path} is already delivered with different content "
                    f"(remote sha256 {existing_digest}, new {digest}); refusing to overwrite "
                    f"a delivered file without force_redeliver"
                )
            action = "skipped_already_delivered"
            delivered_size = existing_size
        else:
            try:
                sftp.remove(temp_path)
            except FileNotFoundError:
                pass
            with sftp.open(temp_path, "wb") as handle:
                handle.set_pipelined(True)
                handle.write(body)
            uploaded_size = sftp.stat(temp_path).st_size
            uploaded_digest, read_back = _remote_sha256(sftp, temp_path)
            if uploaded_size != len(body) or read_back != len(body) or uploaded_digest != digest:
                sftp.remove(temp_path)
                raise AirflowFailException(
                    f"upload verification failed for {temp_path}: size {uploaded_size} vs "
                    f"{len(body)}, sha256 {uploaded_digest} vs {digest}; temp file removed, "
                    f"nothing was published to the recipient"
                )
            sftp.posix_rename(temp_path, final_path)
            action = "redelivered_forced" if existing_digest is not None else "delivered"
            delivered_size = uploaded_size

        final_digest, final_size = _remote_sha256(sftp, final_path)
        listing = sorted(sftp.listdir(base))
    finally:
        sftp.close()
        transport.close()

    if final_digest != digest or final_size != len(body):
        raise AirflowFailException(
            f"{final_path} does not match the source after delivery "
            f"(sha256 {final_digest} vs {digest}, size {final_size} vs {len(body)})"
        )
    leftovers = [n for n in listing if n.startswith(name) and n != name]
    if leftovers:
        raise AirflowFailException(f"temporary file(s) left beside {final_path}: {leftovers}")

    ti.xcom_push(key="sftp_final_path", value=final_path)
    ti.xcom_push(key="sftp_temp_path", value=temp_path)
    ti.xcom_push(key="delivery_action", value=action)
    ti.xcom_push(key="delivered", value=action != "skipped_already_delivered")
    ti.xcom_push(key="delivered_sha256", value=final_digest)
    ti.xcom_push(key="delivered_size_bytes", value=delivered_size)
    ti.xcom_push(key="sftp_host_key_fingerprint", value=fingerprint)
    ti.xcom_push(key="copies_on_sftp", value=listing.count(name))
    log.info("%s: %s (%d bytes, sha256 %s)", action, final_path, delivered_size, final_digest)


def capture_events(**context: Any) -> None:
    ti = context["ti"]
    pull = lambda task, key: ti.xcom_pull(task_ids=task, key=key)
    metrics = {
        "report_name": pull("build_report", "report_name"),
        "report_key": pull("build_report", "report_key"),
        "report_sha256": pull("is_process_failed", "report_sha256"),
        "report_size_bytes": pull("build_report", "report_size_bytes"),
        "report_row_count": pull("is_process_failed", "report_row_count"),
        "sftp_final_path": pull("deliver_sftp", "sftp_final_path"),
        "delivery_action": pull("deliver_sftp", "delivery_action"),
        "delivered_sha256": pull("deliver_sftp", "delivered_sha256"),
        "delivered_size_bytes": pull("deliver_sftp", "delivered_size_bytes"),
        "sftp_host_key_fingerprint": pull("deliver_sftp", "sftp_host_key_fingerprint"),
        "force_redeliver": pull("validate_conf", "force_redeliver"),
    }
    record_event(
        context,
        stage="delivery",
        status="success",
        summary=(
            f"{metrics['delivery_action']}: {metrics['sftp_final_path']} "
            f"({metrics['delivered_size_bytes']} bytes, sha256 {metrics['delivered_sha256']})"
        ),
        metrics=metrics,
    )


with DAG(
    dag_id="nfl_reporting",
    description="Stages 4-5: S3 marker pre-check, report build, Is Process Failed gate, SFTP delivery",
    doc_md=DOC_MD,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(STAGE_BY_TASK, "reporting"),
    tags=["nfl", "stage-4", "stage-5", "reporting", "delivery"],
) as dag:
    t_validate = PythonOperator(task_id="validate_conf", python_callable=validate_conf)
    t_wait = PythonOperator(
        task_id="wait_for_success_marker",
        python_callable=wait_for_success_marker,
        retries=GATE_RETRIES,
        retry_delay=GATE_RETRY_DELAY,
    )
    t_build = PythonOperator(task_id="build_report", python_callable=build_report)
    t_gate = PythonOperator(task_id="is_process_failed", python_callable=is_process_failed)
    t_deliver = PythonOperator(task_id="deliver_sftp", python_callable=deliver_sftp)
    t_events = PythonOperator(task_id="capture_events", python_callable=capture_events)

    t_validate >> t_wait >> t_build >> t_gate >> t_deliver >> t_events
