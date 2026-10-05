"""Live readings from the systems that do the actual replication work.

Debezium (Kafka Connect, 8097) performs the real MySQL -> Kafka CDC and owns
the only truthful notion of "is replication running". Postgres (5440) holds the
landed rows and owns the only truthful notion of "how much arrived". Every
number this stub reports about progress traces back to one of these two.

Both are treated as optional at every call site: when a source is down the
caller receives an explicit failure and surfaces `unknown`, never a zero. A
zero and an unreachable source are different facts and must never render alike.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any

import httpx

log = logging.getLogger("nfl.stub.sources")

CONNECT_URL = os.environ.get("NFL_CONNECT_URL", "http://localhost:8097")
PG_HOST = os.environ.get("NFL_PG_HOST", "localhost")
PG_PORT = int(os.environ.get("NFL_PG_PORT", "5440"))
PG_DB = os.environ.get("NFL_PG_DB", "nfl")
PG_USER = os.environ.get("NFL_PG_USER", "nfl_stub_reader")
PG_PASSWORD = os.environ.get("NFL_PG_PASSWORD", "")
PG_STAGING_SCHEMA = os.environ.get("NFL_PG_STAGING_SCHEMA", "staging")
PROBE_TIMEOUT = float(os.environ.get("NFL_STUB_PROBE_TIMEOUT", "2.0"))


@dataclass
class SourceResult:
    """Outcome of one probe. `ok` false means the number is unknowable now."""

    ok: bool
    data: Any = None
    error: str = ""


@dataclass
class ConnectState:
    connector_state: str = ""
    task_states: list[str] = field(default_factory=list)
    connector_name: str = ""
    tables_captured: list[str] = field(default_factory=list)
    config_read: bool = False
    snapshot_mode: str | None = None

    @property
    def all_tasks_running(self) -> bool:
        return bool(self.task_states) and all(s == "RUNNING" for s in self.task_states)

    @property
    def any_failed(self) -> bool:
        return self.connector_state == "FAILED" or any(s == "FAILED" for s in self.task_states)


def probe_connect() -> SourceResult:
    """Read Debezium connector + task status from Kafka Connect."""
    connector = os.environ.get("NFL_CONNECT_CONNECTOR", "nfl-hawks-mysql-source")
    try:
        with httpx.Client(timeout=PROBE_TIMEOUT) as client:
            resp = client.get(f"{CONNECT_URL}/connectors/{connector}/status")
            if resp.status_code == 404:
                return SourceResult(
                    ok=False,
                    error=(
                        f"Kafka Connect reachable at {CONNECT_URL} but connector "
                        f"'{connector}' is not registered (HTTP 404)."
                    ),
                )
            resp.raise_for_status()
            status = resp.json()

            tables: list[str] = []
            config_read = False
            snapshot_mode: str | None = None
            try:
                cfg = client.get(f"{CONNECT_URL}/connectors/{connector}/config")
                if cfg.status_code == 200:
                    config = cfg.json()
                    config_read = True
                    include = config.get("table.include.list", "")
                    tables = [t.strip() for t in include.split(",") if t.strip()]
                    snapshot_mode = config.get("snapshot.mode")
            except (httpx.HTTPError, ValueError):
                pass

            return SourceResult(
                ok=True,
                data=ConnectState(
                    connector_state=status.get("connector", {}).get("state", "UNKNOWN"),
                    task_states=[t.get("state", "UNKNOWN") for t in status.get("tasks", [])],
                    connector_name=status.get("name", connector),
                    tables_captured=tables,
                    config_read=config_read,
                    snapshot_mode=snapshot_mode,
                ),
            )
    except (httpx.HTTPError, ValueError) as exc:
        return SourceResult(
            ok=False,
            error=f"Kafka Connect at {CONNECT_URL} unreachable: {type(exc).__name__}: {exc}",
        )


def _connect_pg():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is not installed in the stub image; Postgres-derived counts unavailable"
        ) from exc

    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=int(PROBE_TIMEOUT),
        options=f"-c statement_timeout={int(PROBE_TIMEOUT * 1000)}",
    )


def probe_staging_counts() -> SourceResult:
    """Row counts per table in the staging schema — the landed side of CDC.

    Exact COUNT(*) rather than a planner estimate: the whole point is that a
    Bruno assertion can compare this against the source and have the comparison
    mean something. On a local test dataset the cost is irrelevant.
    """
    try:
        with _connect_pg() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (PG_STAGING_SCHEMA,),
            )
            tables = [r[0] for r in cur.fetchall()]

            counts: dict[str, int] = {}
            for table in tables:
                cur.execute(
                    f'SELECT count(*) FROM "{PG_STAGING_SCHEMA}"."{table}"'  # noqa: S608
                )
                counts[table] = cur.fetchone()[0]
            return SourceResult(ok=True, data=counts)
    except Exception as exc:  # noqa: BLE001 - any failure means "unknown", not zero
        return SourceResult(
            ok=False,
            error=(
                f"Postgres at {PG_HOST}:{PG_PORT}/{PG_DB} schema "
                f"'{PG_STAGING_SCHEMA}' unreadable: {type(exc).__name__}: {exc}"
            ),
        )
