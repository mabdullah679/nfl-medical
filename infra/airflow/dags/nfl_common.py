"""Shared plumbing for the NFL pipeline DAGs: interval handling, per-role
connections, event/alert recording. No DAG objects are defined here."""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator

from airflow.exceptions import AirflowFailException

log = logging.getLogger("nfl")

JOB_VERSION = "1.0.0"
MANIFEST_SCHEMA_VERSION = "1.0.0"
REPORT_CODE = "NFL_ACCT_ACTIVITY"
REPORT_VERSION = "v1"

EVENT_TABLE = "observability.pipeline_event"
ALERT_SEVERITY = "critical"

SUCCESS_MARKER = "_SUCCESS"
MANIFEST_NAME = "manifest.json"
CHECKSUM_SIDECAR_SUFFIX = ".sha256"
RAW_CONF_MAX_CHARS = 4000


def env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise AirflowFailException(
            f"required environment variable {name} is not set on the Airflow service"
        )
    return value


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


GATE_RETRIES = env_int("NFL_GATE_RETRIES", 3)
GATE_RETRY_DELAY = timedelta(seconds=env_int("NFL_GATE_RETRY_DELAY_SECONDS", 15))


@dataclass(frozen=True)
class Interval:
    start: datetime
    end: datetime

    @property
    def start_date(self) -> str:
        return self.start.strftime("%Y%m%d")

    @property
    def end_date(self) -> str:
        """Last calendar day covered. The window is half-open, so an end bound of
        2026-09-01T00:00:00Z labels the interval as ending 20260831."""
        return (self.end - timedelta(days=1)).strftime("%Y%m%d")

    @property
    def last_day(self) -> date:
        return (self.end - timedelta(days=1)).date()

    @property
    def prefix(self) -> str:
        template = os.environ.get("NFL_INTERVAL_PREFIX_TEMPLATE", "df_{start}_{end}")
        return template.format(start=self.start_date, end=self.end_date)

    @property
    def start_iso(self) -> str:
        return self.start.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def end_iso(self) -> str:
        return self.end.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_bound(conf: dict[str, Any], field: str) -> datetime:
    raw = conf.get(field)
    if not isinstance(raw, str) or not raw.strip():
        raise AirflowFailException(
            f"dag_run.conf.{field} is required (ISO-8601 UTC, e.g. 2026-08-01T00:00:00Z)"
        )
    try:
        parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise AirflowFailException(
            f"dag_run.conf.{field}={raw!r} is not ISO-8601"
        ) from exc
    if parsed.tzinfo is None:
        raise AirflowFailException(
            f"dag_run.conf.{field}={raw!r} carries no UTC offset; use a trailing Z"
        )
    parsed = parsed.astimezone(timezone.utc)
    if (parsed.hour, parsed.minute, parsed.second, parsed.microsecond) != (0, 0, 0, 0):
        raise AirflowFailException(
            f"dag_run.conf.{field}={raw!r} must fall on a UTC midnight: S3 prefixes, "
            f"batch keys and report names are day-granular, so a sub-day bound would "
            f"collide with another interval"
        )
    return parsed


def interval_from_conf(conf: dict[str, Any] | None) -> Interval:
    conf = conf or {}
    start = _parse_bound(conf, "interval_start")
    end = _parse_bound(conf, "interval_end")
    if end <= start:
        raise AirflowFailException(
            f"interval_end {conf.get('interval_end')!r} must be after interval_start "
            f"{conf.get('interval_start')!r} (the window is [start, end))"
        )
    return Interval(start, end)


def interval_from_context(context: dict[str, Any]) -> Interval:
    return interval_from_conf(context["dag_run"].conf)


def push_interval(context: dict[str, Any]) -> Interval:
    interval = interval_from_context(context)
    ti = context["ti"]
    ti.xcom_push(key="interval_start", value=interval.start_iso)
    ti.xcom_push(key="interval_end", value=interval.end_iso)
    ti.xcom_push(key="interval_start_date", value=interval.start_date)
    ti.xcom_push(key="interval_end_date", value=interval.end_date)
    ti.xcom_push(key="interval_prefix", value=interval.prefix)
    return interval


@contextmanager
def pg(role: str) -> Iterator[Any]:
    """Connection as exactly one pipeline role. `role` is HOP, SPARK or EVENTS and
    selects NFL_PG_<role>_USER / NFL_PG_<role>_PASSWORD."""
    import psycopg2

    conn = psycopg2.connect(
        host=env("NFL_POSTGRES_HOST"),
        port=env_int("NFL_POSTGRES_PORT", 5432),
        dbname=env("NFL_POSTGRES_DB"),
        user=env(f"NFL_PG_{role}_USER"),
        password=env(f"NFL_PG_{role}_PASSWORD"),
        connect_timeout=10,
        application_name=f"airflow-nfl-{role.lower()}",
    )
    try:
        yield conn
    finally:
        conn.close()


def s3(stage: str) -> Any:
    """S3 client under the stage's own key pair (NFL_<stage>_AWS_*)."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=env("AWS_ENDPOINT_URL"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=env(f"NFL_{stage}_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=env(f"NFL_{stage}_AWS_SECRET_ACCESS_KEY"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def bucket() -> str:
    return env("NFL_S3_BUCKET")


def s3_exists(client: Any, key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client.head_object(Bucket=bucket(), Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    return True


def s3_list(client: Any, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket(), Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []) or [])
    return sorted(keys)


def s3_delete(client: Any, keys: list[str]) -> None:
    for key in keys:
        client.delete_object(Bucket=bucket(), Key=key)


def _error_summary(context: dict[str, Any]) -> str:
    exc = context.get("exception")
    if exc is None:
        return "task failed without an exception object"
    text = f"{type(exc).__name__}: {exc}"
    return " ".join(text.split())[:2000]


def _raw_conf(context: dict[str, Any]) -> Any:
    conf = context["dag_run"].conf
    text = json.dumps(conf, default=str, sort_keys=True)
    return conf if len(text) <= RAW_CONF_MAX_CHARS else text[:RAW_CONF_MAX_CHARS]


def record_event(
    context: dict[str, Any],
    *,
    stage: str,
    status: str,
    summary: str,
    metrics: dict[str, Any] | None = None,
    allow_missing_interval: bool = False,
) -> None:
    """One row in observability.pipeline_event, written as the nfl_events role.
    A run whose conf yields no valid interval is recorded only when
    allow_missing_interval is set (failure and retry paths): the row then carries
    NULL interval bounds and the raw conf, so the alert is not lost with the
    interval."""
    ti = context["ti"]
    payload = {"task_id": ti.task_id, "try_number": ti.try_number, **(metrics or {})}
    try:
        interval = interval_from_context(context)
    except AirflowFailException as exc:
        if not allow_missing_interval:
            raise
        bounds: tuple[date | None, date | None] = (None, None)
        payload.update(
            interval_derivable=False,
            interval_error=" ".join(str(exc).split())[:500],
            raw_conf=_raw_conf(context),
        )
    else:
        bounds = (interval.start.date(), interval.last_day)
    with pg("EVENTS") as conn, conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {EVENT_TABLE} "
            "(stage, step, status, interval_start, interval_end, dag_id, run_id, "
            " metrics, message) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)",
            (
                stage,
                ti.task_id,
                status,
                bounds[0],
                bounds[1],
                ti.dag_id,
                context["dag_run"].run_id,
                json.dumps(payload, default=str, sort_keys=True),
                summary[:2000],
            ),
        )
        conn.commit()
    log.info("event recorded: stage=%s step=%s status=%s %s", stage, ti.task_id, status, summary)


def make_failure_hook(stage_by_task: dict[str, str], default_stage: str):
    """The single Operations alert hook. Airflow calls it once per task, when the
    task has exhausted its retries. Locally the alert is a pipeline_event row with
    status 'failed' and metrics.alert = true carrying severity and the error
    summary; in production the same hook also publishes to the Operations channel
    (SNS/PagerDuty)."""

    def alert_operations(context: dict[str, Any]) -> None:
        ti = context["ti"]
        stage = stage_by_task.get(ti.task_id, default_stage)
        summary = _error_summary(context)
        try:
            record_event(
                context,
                stage=stage,
                status="failed",
                summary=summary,
                metrics={
                    "alert": True,
                    "severity": ALERT_SEVERITY,
                    "error_summary": summary,
                    "notify": "operations",
                },
                allow_missing_interval=True,
            )
            log.error(
                "OPERATIONS ALERT recorded: severity=%s stage=%s task=%s run=%s: %s",
                ALERT_SEVERITY, stage, ti.task_id, context["dag_run"].run_id, summary,
            )
        except Exception:
            log.exception(
                "OPERATIONS ALERT COULD NOT BE RECORDED in %s for stage=%s task=%s run=%s: %s",
                EVENT_TABLE, stage, ti.task_id, context["dag_run"].run_id, summary,
            )
            raise

    return alert_operations


def make_retry_hook(stage_by_task: dict[str, str], default_stage: str):
    def record_retry(context: dict[str, Any]) -> None:
        stage = stage_by_task.get(context["ti"].task_id, default_stage)
        record_event(
            context,
            stage=stage,
            status="retry",
            summary=_error_summary(context),
            allow_missing_interval=True,
        )

    return record_retry


def default_args(stage_by_task: dict[str, str], default_stage: str) -> dict[str, Any]:
    return {
        "owner": "nfl",
        "retries": 0,
        "on_failure_callback": make_failure_hook(stage_by_task, default_stage),
        "on_retry_callback": make_retry_hook(stage_by_task, default_stage),
    }
