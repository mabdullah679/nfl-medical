from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class TablePair:
    """One staging table and the utm table ingestion loads it into."""

    name: str
    staging: str
    utm: str
    source_pk: str
    business_key: str
    staging_event: str
    utm_event: str


# Mirrors TABLES in infra/airflow/dags/nfl_ingestion.py and the relations in
# infra/sql/01-staging-schema.sql and 02-utm-schema.sql.
TABLE_PAIRS: tuple[TablePair, ...] = (
    TablePair(
        "patient_account",
        "staging.patient_account",
        "utm.dim_patient_account",
        "account_id",
        "account_number",
        "opened_at",
        "opened_at",
    ),
    TablePair(
        "account_transaction",
        "staging.account_transaction",
        "utm.fact_account_transaction",
        "transaction_id",
        "transaction_ref",
        "posted_at",
        "posted_at",
    ),
    TablePair(
        "payment",
        "staging.payment",
        "utm.fact_payment",
        "payment_id",
        "payment_ref",
        "received_at",
        "received_at",
    ),
)

# Staging is an append-only CDC log (JDBC sink insert mode, no key): an UPDATE at
# the source lands as another row. The newest version of a source row is the one
# with the greatest values of these columns, the same rule ingestion applies.
STAGING_VERSION_ORDER: tuple[str, ...] = ("dms_loaded_at", "updated_at")


@dataclass(frozen=True)
class Endpoint:
    host: str
    port: int

    @property
    def label(self) -> str:
        return f"{self.host}:{self.port}"


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"${name} must be an integer, got {raw!r}") from exc


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"${name} must be a number, got {raw!r}") from exc


class Settings:
    def __init__(self) -> None:
        self.s3_endpoint_url: str = os.environ.get(
            "SHIM_S3_ENDPOINT_URL", "http://localhost:4566"
        )
        self.aws_region: str = os.environ.get("SHIM_AWS_REGION", "us-east-1")
        self.s3_bucket: str = os.environ.get("SHIM_S3_BUCKET", "nfl-data-platform")

        self.interval_prefix_template: str = os.environ.get(
            "SHIM_INTERVAL_PREFIX_TEMPLATE", "df_{start}_{end}"
        )
        self.silver_subprefix: str = os.environ.get("SHIM_SILVER_SUBPREFIX", "silver")
        self.gold_subprefix: str = os.environ.get("SHIM_GOLD_SUBPREFIX", "gold")
        self.reports_prefix: str = os.environ.get("SHIM_REPORTS_PREFIX", "reports")

        self.replication_lag_threshold_seconds: int = _int_env(
            "SHIM_REPLICATION_LAG_THRESHOLD_SECONDS", 300
        )
        self.reconcile_tolerance_rows: int = _int_env(
            "SHIM_RECONCILE_TOLERANCE_ROWS", 0
        )
        self.freshness_threshold_seconds: int = _int_env(
            "SHIM_FRESHNESS_THRESHOLD_SECONDS", 900
        )
        self.quality_sample_cap: int = _int_env("SHIM_QUALITY_SAMPLE_CAP", 20)

        self.lineage_table: str = os.environ.get(
            "SHIM_LINEAGE_TABLE", "observability.pipeline_event"
        )
        self.events_table: str = os.environ.get(
            "SHIM_EVENTS_TABLE", "observability.pipeline_event"
        )
        self.events_row_cap: int = _int_env("SHIM_EVENTS_ROW_CAP", 200)
        self.ingestion_batch_table: str = os.environ.get(
            "SHIM_INGESTION_BATCH_TABLE", "utm.ingestion_batch"
        )
        self.ingestion_reject_table: str = os.environ.get(
            "SHIM_INGESTION_REJECT_TABLE", "utm.ingestion_reject"
        )
        # Freshness measures when data LANDED in each layer, not the source
        # system's own mutation time.
        self.staging_freshness_column: str = os.environ.get(
            "SHIM_STAGING_FRESHNESS_COLUMN", "dms_loaded_at"
        )
        self.utm_freshness_column: str = os.environ.get(
            "SHIM_UTM_FRESHNESS_COLUMN", "utm_loaded_at"
        )
        # End-to-end replication heartbeat: a row written at the source on a timer
        # and replicated like any other. Set the table to an empty string to
        # declare that no heartbeat exists; freshness then says so in its verdict.
        self.heartbeat_table: str = os.environ.get(
            "SHIM_HEARTBEAT_TABLE", "staging.replication_heartbeat"
        ).strip()
        self.heartbeat_column: str = os.environ.get(
            "SHIM_HEARTBEAT_COLUMN", "beat_at"
        )
        self.heartbeat_source_column: str = os.environ.get(
            "SHIM_HEARTBEAT_SOURCE_COLUMN", "source_name"
        )
        # Empty: every source that has ever beaten must be fresh.
        self.heartbeat_source: str = os.environ.get("SHIM_HEARTBEAT_SOURCE", "").strip()
        self.heartbeat_max_age_seconds: int = _int_env(
            "SHIM_HEARTBEAT_MAX_AGE_SECONDS", 300
        )

        self.debezium_connector: str = os.environ.get(
            "SHIM_DEBEZIUM_CONNECTOR", "nfl-hawks-mysql-source"
        )
        self.sink_connector: str = os.environ.get(
            "SHIM_SINK_CONNECTOR", "nfl-staging-jdbc-sink"
        )
        self.kafka_connect_url: str = os.environ.get(
            "SHIM_CONNECTOR_STATUS_CONNECT_URL", "http://localhost:8097"
        )

        self.sftp_host: str = os.environ.get("SHIM_DELIVERY_SFTP_HOST", "localhost")
        self.sftp_port: int = _int_env("SHIM_DELIVERY_SFTP_PORT", 2225)
        self.sftp_recipient_dir: str = os.environ.get(
            "SHIM_DELIVERY_SFTP_RECIPIENT_DIR", "/outbound"
        )
        self.sftp_temp_suffix: str = os.environ.get(
            "SHIM_DELIVERY_SFTP_TEMP_SUFFIX", ".tmp"
        )

        self.nfl_postgres = Endpoint(
            os.environ.get("SHIM_PG_HOST", "localhost"), _int_env("SHIM_PG_PORT", 5440)
        )
        self.mysql_source = Endpoint(
            os.environ.get("SHIM_MYSQL_HOST", "localhost"),
            _int_env("SHIM_MYSQL_PORT", 3307),
        )

        self.http_timeout_seconds: float = _float_env("SHIM_HTTP_TIMEOUT_SECONDS", 5.0)
        self.db_timeout_seconds: int = _int_env("SHIM_DB_TIMEOUT_SECONDS", 5)
        self.tcp_probe_timeout_seconds: float = _float_env(
            "SHIM_TCP_PROBE_TIMEOUT_SECONDS", 2.0
        )

        self.parquet_rowcount_enabled: bool = os.environ.get(
            "SHIM_PARQUET_ROWCOUNT_ENABLED", "true"
        ).strip().lower() not in {"0", "false", "no"}

    def interval_prefix(self, start: str, end: str) -> str:
        return self.interval_prefix_template.format(start=start, end=end)

    def dependency_endpoints(self) -> dict[str, Endpoint]:
        s3 = urlsplit(self.s3_endpoint_url)
        connect = urlsplit(self.kafka_connect_url)
        return {
            "s3": Endpoint(s3.hostname or "localhost", s3.port or 4566),
            "nfl_postgres": self.nfl_postgres,
            "mysql_source": self.mysql_source,
            "kafka_connect": Endpoint(
                connect.hostname or "localhost", connect.port or 8097
            ),
            "sftp": Endpoint(self.sftp_host, self.sftp_port),
        }


settings = Settings()
