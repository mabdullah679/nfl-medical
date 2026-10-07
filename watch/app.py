"""nfl-watch: the part of the platform that looks without being asked.

Every tick it runs three probes, appends what it measured to
observability.monitor_sample, and when a probe changes level it appends the
alert (or the recovery) to observability.pipeline_event. It then delivers every
event that asks for a notification and has not been delivered, and records each
attempt in observability.alert_delivery.

It is deliberately not an Airflow DAG: the orchestrator is one of the things it
watches, and a watcher that stops with the thing it watches reports nothing.

Two database identities, one per job:
  nfl_monitor   reads the replication heartbeat, writes samples and alert events
  nfl_notifier  reads events, appends delivery attempts
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import format_datetime, make_msgid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import psycopg

logging.basicConfig(
    level=os.environ.get("NFL_WATCH_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s nfl-watch %(message)s",
)
log = logging.getLogger("nfl-watch")

LEVELS = ("ok", "warning", "critical")
RANK = {level: i for i, level in enumerate(LEVELS)}
UNKNOWN = "unknown"
CHANNEL = "email"


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, "").strip()
    if value:
        return value
    if default is None:
        raise SystemExit(f"required environment variable {name} is not set")
    return default


def env_int(name: str, default: int, minimum: int = 1) -> int:
    value = int(env(name, str(default)))
    if value < minimum:
        raise SystemExit(f"{name}={value} must be at least {minimum}")
    return value


TICK_SECONDS = env_int("NFL_WATCH_INTERVAL_SECONDS", 15)
HEARTBEAT_MAX_AGE = env_int("NFL_WATCH_HEARTBEAT_MAX_AGE_SECONDS", 300)
# A stale heartbeat is a warning until it is this many times the limit.
HEARTBEAT_CRITICAL_FACTOR = env_int("NFL_WATCH_HEARTBEAT_CRITICAL_FACTOR", 2, minimum=2)
# A probe that cannot run holds the previous level for this many ticks in a
# row, then becomes critical: not knowing is not the same as healthy.
UNKNOWN_TICKS = env_int("NFL_WATCH_UNKNOWN_TICKS", 3)
RETENTION_DAYS = env_int("NFL_WATCH_SAMPLE_RETENTION_DAYS", 7)
MAX_ATTEMPTS = env_int("NFL_ALERT_MAX_ATTEMPTS", 10)
# A failed send is retried after 30s, then 60s, 120s, ... up to this cap, so
# ten attempts cover about an hour of a mail server being away, not ten ticks.
RETRY_BASE_SECONDS = env_int("NFL_ALERT_RETRY_BASE_SECONDS", 30)
RETRY_CAP_SECONDS = env_int("NFL_ALERT_RETRY_CAP_SECONDS", 600)
DISPATCH_MAX_AGE_HOURS = env_int("NFL_ALERT_DISPATCH_MAX_AGE_HOURS", 24)
HTTP_TIMEOUT = env_int("NFL_WATCH_HTTP_TIMEOUT_SECONDS", 5)
HEALTH_PORT = env_int("NFL_WATCH_HEALTH_PORT", 8099)

CONNECT_URL = env("NFL_CONNECT_URL").rstrip("/")
CONNECTORS = [n.strip() for n in env("NFL_CONNECTORS").split(",") if n.strip()]
ORCHESTRATOR_HEALTH_URL = env("NFL_ORCHESTRATOR_HEALTH_URL")
LOG_GROUP_PREFIX = env("NFL_LOG_GROUP_PREFIX", "/nfl").rstrip("/")

SMTP_HOST = env("NFL_ALERT_SMTP_HOST")
SMTP_PORT = env_int("NFL_ALERT_SMTP_PORT", 25)
ALERT_FROM = env("NFL_ALERT_FROM")
ALERT_TO = [a.strip() for a in env("NFL_ALERT_TO").split(",") if a.strip()]
AIRFLOW_PUBLIC_URL = env("NFL_AIRFLOW_PUBLIC_URL").rstrip("/")


def connect(role: str) -> psycopg.Connection:
    return psycopg.connect(
        host=env("NFL_PG_HOST"),
        port=env_int("NFL_PG_PORT", 5432),
        dbname=env("NFL_PG_DB"),
        user=env(f"NFL_PG_{role}_USER"),
        password=env(f"NFL_PG_{role}_PASSWORD"),
        connect_timeout=HTTP_TIMEOUT,
        application_name=f"nfl-watch/{role.lower()}",
    )


@dataclass
class Reading:
    """What one probe found on one tick. level is UNKNOWN when it could not run."""

    probe: str
    level: str
    summary: str
    value: float | None = None
    threshold: float | None = None
    stage: str = "replication"
    log_group: str = ""
    detail: dict[str, Any] = field(default_factory=dict)


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read())


def probe_heartbeat(conn: psycopg.Connection) -> Reading:
    """Age of the newest end-to-end replication beat of every source. The beat is
    written at the source on a timer and replicated like any other row, so its
    age is the lag a real change would see."""
    base = dict(
        probe="replication_heartbeat",
        threshold=float(HEARTBEAT_MAX_AGE),
        log_group=f"{LOG_GROUP_PREFIX}/nfl-kafka-connect",
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT source_name, max(beat_at), extract(epoch FROM now() - max(beat_at)) "
            "FROM staging.replication_heartbeat GROUP BY source_name ORDER BY source_name"
        )
        rows = cur.fetchall()
    conn.rollback()
    if not rows:
        return Reading(
            level="critical",
            summary="staging.replication_heartbeat holds no beat: replication has never landed one",
            detail={"sources": {}},
            **base,
        )
    sources = {
        name: {"last_beat_at": beat.isoformat(), "age_seconds": round(float(age), 1)}
        for name, beat, age in rows
    }
    worst_name, worst = max(sources.items(), key=lambda kv: kv[1]["age_seconds"])
    age = worst["age_seconds"]
    if age <= HEARTBEAT_MAX_AGE:
        level = "ok"
        summary = f"replication heartbeat {age:.0f}s old (limit {HEARTBEAT_MAX_AGE}s)"
    else:
        critical_at = HEARTBEAT_MAX_AGE * HEARTBEAT_CRITICAL_FACTOR
        level = "critical" if age > critical_at else "warning"
        summary = (
            f"replication lag above threshold: newest heartbeat of {worst_name} is "
            f"{age:.0f}s old, limit {HEARTBEAT_MAX_AGE}s (critical above {critical_at}s)"
        )
    return Reading(level=level, summary=summary, value=age, detail={"sources": sources}, **base)


def probe_connectors() -> Reading:
    base = dict(probe="connector_status", log_group=f"{LOG_GROUP_PREFIX}/nfl-kafka-connect")
    states: dict[str, Any] = {}
    try:
        for name in CONNECTORS:
            doc = _get_json(f"{CONNECT_URL}/connectors/{name}/status")
            states[name] = {
                "connector": doc.get("connector", {}).get("state"),
                "tasks": [t.get("state") for t in doc.get("tasks", [])],
            }
    except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
        return Reading(
            level=UNKNOWN,
            summary=f"Kafka Connect status could not be read: {type(exc).__name__}: {exc}",
            detail={"connectors": states},
            **base,
        )
    down = {
        n: s
        for n, s in states.items()
        if s["connector"] != "RUNNING" or not s["tasks"] or any(x != "RUNNING" for x in s["tasks"])
    }
    if down:
        text = ", ".join(
            f"{n} is {s['connector']} (tasks {s['tasks'] or 'none'})" for n, s in sorted(down.items())
        )
        return Reading(
            level="critical",
            summary=f"replication connector not RUNNING: {text}",
            value=float(len(down)),
            threshold=0.0,
            detail={"connectors": states},
            **base,
        )
    return Reading(
        level="ok",
        summary=f"{len(states)} replication connector(s) RUNNING",
        value=0.0,
        threshold=0.0,
        detail={"connectors": states},
        **base,
    )


def probe_orchestrator() -> Reading:
    """Airflow's own health document. A scheduler that has stopped heartbeating
    runs nothing, and nothing a DAG does can report that."""
    base = dict(probe="orchestrator", stage="platform", log_group=f"{LOG_GROUP_PREFIX}/nfl-airflow")
    try:
        doc = _get_json(ORCHESTRATOR_HEALTH_URL)
    except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
        return Reading(
            level=UNKNOWN,
            summary=f"Airflow health could not be read: {type(exc).__name__}: {exc}",
            **base,
        )
    parts = {
        name: (doc.get(name) or {}).get("status")
        for name in ("metadatabase", "scheduler")
    }
    sick = {n: s for n, s in parts.items() if s != "healthy"}
    if sick:
        return Reading(
            level="critical",
            summary="Airflow is not healthy: "
            + ", ".join(f"{n} is {s}" for n, s in sorted(sick.items())),
            value=float(len(sick)),
            threshold=0.0,
            detail={"components": parts},
            **base,
        )
    return Reading(
        level="ok",
        summary="Airflow scheduler and metadata database healthy",
        value=0.0,
        threshold=0.0,
        detail={"components": parts},
        **base,
    )


def _history(cur: psycopg.Cursor, probe: str) -> list[dict[str, Any]]:
    cur.execute(
        "SELECT detail FROM observability.monitor_sample WHERE probe = %s "
        "ORDER BY sampled_at DESC, sample_id DESC LIMIT %s",
        (probe, UNKNOWN_TICKS),
    )
    return [row[0] for row in cur.fetchall()]


def settle(reading: Reading, history: list[dict[str, Any]]) -> tuple[str, str, str | None]:
    """(previous level, level to act on, incident id). A probe that could not run
    keeps the previous level until it has failed UNKNOWN_TICKS times in a row."""
    last = history[0] if history else {}
    previous = last.get("level", "ok")
    incident = last.get("incident")
    level = reading.level
    if level == UNKNOWN:
        earlier = [h.get("raw_level") for h in history[: UNKNOWN_TICKS - 1]]
        blind = len(earlier) == UNKNOWN_TICKS - 1 and all(r == UNKNOWN for r in earlier)
        level = "critical" if blind else previous
    if level == "ok":
        incident = incident if previous != "ok" else None
    elif previous == "ok" or not incident:
        incident = f"nfl_watch_{reading.probe}_{int(time.time())}"
    return previous, level, incident


def record(conn: psycopg.Connection, reading: Reading) -> None:
    """Append the sample and, when the level changed, the alert or recovery."""
    with conn.cursor() as cur:
        previous, level, incident = settle(reading, _history(cur, reading.probe))
        detail = {
            **reading.detail,
            "level": level,
            "raw_level": reading.level,
            "summary": reading.summary,
            "incident": incident if level != "ok" else None,
        }
        cur.execute(
            "INSERT INTO observability.monitor_sample (probe, healthy, value, threshold, detail) "
            "VALUES (%s, %s, %s, %s, %s::jsonb)",
            (
                reading.probe,
                None if reading.level == UNKNOWN else reading.level == "ok",
                reading.value,
                reading.threshold,
                json.dumps(detail, default=str, sort_keys=True),
            ),
        )
        worse = RANK[level] > RANK[previous]
        recovered = level == "ok" and previous != "ok"
        if worse or recovered:
            summary = reading.summary
            if reading.level == UNKNOWN and worse:
                summary = f"probe could not run {UNKNOWN_TICKS} times in a row: {summary}"
            metrics: dict[str, Any] = {
                "probe": reading.probe,
                "severity": "info" if recovered else level,
                "notify": "operations",
                "value": reading.value,
                "threshold": reading.threshold,
                "previous_level": previous,
                "level": level,
                "log_group": reading.log_group,
                "log_filter": incident,
                **reading.detail,
            }
            if recovered:
                metrics["recovered"] = True
                summary = f"recovered: {summary}"
            else:
                metrics.update(alert=True, error_summary=summary)
            cur.execute(
                "INSERT INTO observability.pipeline_event "
                "(stage, step, status, run_id, metrics, message, correlation_id) "
                "VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)",
                (
                    reading.stage,
                    f"watch_{reading.probe}",
                    "success" if recovered else "failed",
                    incident,
                    json.dumps(metrics, default=str, sort_keys=True),
                    summary[:2000],
                    incident,
                ),
            )
            log.warning(
                "state change: correlation_id=%s probe=%s %s -> %s: %s",
                incident, reading.probe, previous, level, summary,
            )
    conn.commit()


def prune(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM observability.monitor_sample "
            "WHERE sampled_at < now() - make_interval(days => %s)",
            (RETENTION_DAYS,),
        )
        removed = cur.rowcount
    conn.commit()
    if removed:
        log.info("pruned %d sample(s) older than %d day(s)", removed, RETENTION_DAYS)


def compose(event: dict[str, Any]) -> EmailMessage:
    m = event["metrics"]
    severity = str(m.get("severity") or "unknown")
    interval = (
        f"{event['interval_start']} .. {event['interval_end']}"
        if event["interval_start"]
        else "none"
    )
    kind = "RECOVERED" if m.get("recovered") else severity.upper()
    run = f" (run {event['run_id']})" if event["dag_id"] else ""
    msg = EmailMessage()
    msg["Subject"] = f"[NFL][{kind}] {event['stage']}/{event['step']} {event['status']}{run}"
    msg["From"] = ALERT_FROM
    msg["To"] = ", ".join(ALERT_TO)
    msg["Date"] = format_datetime(datetime.now(timezone.utc))
    msg["Message-ID"] = make_msgid(idstring=f"nfl-event-{event['event_id']}", domain="nfl-watch")
    msg["X-NFL-Event-Id"] = str(event["event_id"])
    msg["X-NFL-Correlation-Id"] = str(event["correlation_id"] or "")
    msg["X-NFL-Severity"] = severity
    lines = [
        f"Severity:       {severity}",
        f"Stage / step:   {event['stage']} / {event['step']}",
        f"Status:         {event['status']}",
        f"Run id:         {event['run_id'] or 'none'}",
        f"DAG:            {event['dag_id'] or 'none (raised by nfl-watch)'}",
        f"Interval:       {interval}",
        f"Correlation id: {event['correlation_id'] or 'none'}",
        f"Event:          event_id={event['event_id']} at {event['event_time'].isoformat()}",
        "",
        "Summary:",
        f"  {m.get('error_summary') or event['message']}",
        "",
    ]
    if m.get("log_url"):
        lines.append(f"Task log (Airflow UI):  {m['log_url']}")
    if m.get("log_api_path"):
        lines.append(f"Task log (REST):        {AIRFLOW_PUBLIC_URL}{m['log_api_path']}")
    if m.get("log_group"):
        lines.append(f"Log group:              {m['log_group']}")
    if event["correlation_id"]:
        lines.append(
            f"Every log line of this thread: filter pattern {event['correlation_id']} "
            f"in log groups {LOG_GROUP_PREFIX}/*"
        )
    msg.set_content("\n".join(lines) + "\n")
    return msg


def dispatch(conn: psycopg.Connection) -> int:
    """Deliver every notifiable event that has no successful delivery yet."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT e.event_id, e.stage, e.step, e.status, e.interval_start, e.interval_end, "
            "       e.event_time, e.dag_id, e.run_id, e.message, e.metrics, e.correlation_id, "
            "       (SELECT count(*) FROM observability.alert_delivery d "
            "         WHERE d.event_id = e.event_id AND d.channel = %(channel)s), "
            "       (SELECT extract(epoch FROM now() - max(d.attempted_at)) "
            "          FROM observability.alert_delivery d "
            "         WHERE d.event_id = e.event_id AND d.channel = %(channel)s) "
            "FROM observability.pipeline_event e "
            "WHERE e.metrics ? 'notify' "
            "  AND e.event_time > now() - make_interval(hours => %(hours)s) "
            "  AND NOT EXISTS (SELECT 1 FROM observability.alert_delivery d "
            "                   WHERE d.event_id = e.event_id AND d.channel = %(channel)s "
            "                     AND d.status = 'sent') "
            "ORDER BY e.event_id LIMIT 50",
            {"channel": CHANNEL, "hours": DISPATCH_MAX_AGE_HOURS},
        )
        columns = (
            "event_id", "stage", "step", "status", "interval_start", "interval_end",
            "event_time", "dag_id", "run_id", "message", "metrics", "correlation_id", "attempts",
            "since_last_attempt",
        )
        pending = [dict(zip(columns, row)) for row in cur.fetchall()]
    conn.rollback()

    sent = 0
    for event in pending:
        if event["attempts"] >= MAX_ATTEMPTS:
            continue
        if event["attempts"]:
            wait = min(RETRY_BASE_SECONDS * 2 ** (event["attempts"] - 1), RETRY_CAP_SECONDS)
            if float(event["since_last_attempt"]) < wait:
                continue
        attempt = event["attempts"] + 1
        message = compose(event)
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=HTTP_TIMEOUT) as smtp:
                refused = smtp.send_message(message)
            if refused:
                raise smtplib.SMTPRecipientsRefused(refused)
            status, detail = "sent", f"accepted by {SMTP_HOST}:{SMTP_PORT}"
        except (smtplib.SMTPException, OSError) as exc:
            status, detail = "failed", f"{type(exc).__name__}: {exc}"[:1000]
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO observability.alert_delivery "
                    "(event_id, channel, recipients, status, attempt, message_id, detail) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        event["event_id"], CHANNEL, ", ".join(ALERT_TO), status, attempt,
                        message["Message-ID"], detail,
                    ),
                )
            conn.commit()
        except psycopg.errors.UniqueViolation:
            conn.rollback()
            continue
        log_fn = log.info if status == "sent" else log.error
        log_fn(
            "alert delivery %s: correlation_id=%s event_id=%s attempt=%d channel=%s to=%s %s",
            status, event["correlation_id"], event["event_id"], attempt, CHANNEL,
            ",".join(ALERT_TO), detail,
        )
        sent += status == "sent"
    return sent


STATE: dict[str, Any] = {"ticks": 0, "last_tick_at": None, "last_tick_ok": None, "last_error": None}


def tick() -> None:
    errors: list[str] = []
    try:
        with connect("MONITOR") as conn:
            for probe in (lambda: probe_heartbeat(conn), probe_connectors, probe_orchestrator):
                try:
                    record(conn, probe())
                except psycopg.Error as exc:
                    conn.rollback()
                    errors.append(f"{type(exc).__name__}: {exc}")
            if STATE["ticks"] % 240 == 0:
                prune(conn)
    except psycopg.Error as exc:
        errors.append(f"monitor connection: {type(exc).__name__}: {exc}")
    try:
        with connect("NOTIFIER") as conn:
            dispatch(conn)
    except psycopg.Error as exc:
        errors.append(f"notifier connection: {type(exc).__name__}: {exc}")
    STATE.update(
        ticks=STATE["ticks"] + 1,
        last_tick_at=time.time(),
        last_tick_ok=not errors,
        last_error="; ".join(errors)[:1000] or None,
    )
    for err in errors:
        log.error("tick problem: %s", " ".join(err.split()))


class Health(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        last = STATE["last_tick_at"]
        age = None if last is None else time.time() - last
        alive = age is not None and age < TICK_SECONDS * 3
        body = json.dumps(
            {
                "ok": alive and bool(STATE["last_tick_ok"]),
                "alive": alive,
                "ticks": STATE["ticks"],
                "seconds_since_last_tick": None if age is None else round(age, 1),
                "tick_interval_seconds": TICK_SECONDS,
                "last_tick_ok": STATE["last_tick_ok"],
                "last_error": STATE["last_error"],
            }
        ).encode()
        self.send_response(200 if alive else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:
        return


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", HEALTH_PORT), Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info(
        "started: tick %ss, heartbeat limit %ss, connectors %s, alerts to %s via %s:%s",
        TICK_SECONDS, HEARTBEAT_MAX_AGE, ",".join(CONNECTORS), ",".join(ALERT_TO),
        SMTP_HOST, SMTP_PORT,
    )
    while True:
        started = time.monotonic()
        try:
            tick()
        except Exception:  # the loop must outlive any single bad tick
            log.exception("tick crashed")
            STATE.update(last_tick_at=time.time(), last_tick_ok=False, last_error="tick crashed")
        time.sleep(max(1.0, TICK_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
