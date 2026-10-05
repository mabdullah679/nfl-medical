from __future__ import annotations

import socket
from contextlib import contextmanager
from typing import Any, Iterator

import boto3
import httpx
import psycopg
import pymysql
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

from .config import Endpoint, settings
from .result import CheckUnverifiable
from .roles import Role, RoleConfigurationError


def tcp_reachable(endpoint: Endpoint, timeout: float | None = None) -> tuple[bool, str]:
    timeout = timeout if timeout is not None else settings.tcp_probe_timeout_seconds
    try:
        with socket.create_connection((endpoint.host, endpoint.port), timeout=timeout):
            return True, "accepting connections"
    except OSError as exc:
        return False, f"{type(exc).__name__}: {exc}"


@contextmanager
def postgres(role: Role, check: str) -> Iterator[psycopg.Connection]:
    try:
        dsn = role.secret(role.credential_env or "")
    except RoleConfigurationError as exc:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="nfl_postgres",
            reason=str(exc),
        ) from exc

    try:
        conn = psycopg.connect(
            dsn,
            connect_timeout=settings.db_timeout_seconds,
            application_name=f"nfl-shim/{role.name}",
        )
    except psycopg.Error as exc:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="nfl_postgres",
            reason=(
                f"cannot connect to Postgres as role "
                f"'{role.identity.get('name')}': {_pg_reason(exc)}"
            ),
        ) from exc

    try:
        with conn:
            yield conn
    except psycopg.errors.InsufficientPrivilege as exc:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="nfl_postgres",
            reason=(
                f"role '{role.identity.get('name')}' lacks a required grant: "
                f"{_pg_reason(exc)}. Grants declared in {role.file.name} are not "
                f"applied to the database."
            ),
        ) from exc
    except psycopg.errors.UndefinedTable as exc:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="nfl_postgres",
            reason=f"expected relation does not exist: {_pg_reason(exc)}",
        ) from exc
    except psycopg.Error as exc:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="nfl_postgres",
            reason=f"query failed: {_pg_reason(exc)}",
        ) from exc
    finally:
        conn.close()


def _pg_reason(exc: Exception) -> str:
    return " ".join(str(exc).split())[:400]


@contextmanager
def mysql(role: Role, check: str) -> Iterator[pymysql.connections.Connection]:
    try:
        dsn = role.secret(role.credential_env or "")
    except RoleConfigurationError as exc:
        raise CheckUnverifiable(
            check=check, role=role.name, dependency="mysql_source", reason=str(exc)
        ) from exc

    params = _parse_mysql_dsn(dsn, check=check, role=role)
    try:
        conn = pymysql.connect(
            connect_timeout=settings.db_timeout_seconds,
            read_timeout=settings.db_timeout_seconds,
            cursorclass=pymysql.cursors.DictCursor,
            **params,
        )
    except pymysql.MySQLError as exc:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="mysql_source",
            reason=(
                f"cannot connect to MySQL as '{role.identity.get('name')}': "
                f"{_pg_reason(exc)}"
            ),
        ) from exc
    try:
        yield conn
    except pymysql.err.OperationalError as exc:
        code = exc.args[0] if exc.args else None
        hint = (
            " REPLICATION CLIENT grant appears to be missing."
            if code in {1227, 1142}
            else ""
        )
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="mysql_source",
            reason=f"query failed: {_pg_reason(exc)}.{hint}",
        ) from exc
    except pymysql.MySQLError as exc:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="mysql_source",
            reason=f"query failed: {_pg_reason(exc)}",
        ) from exc
    finally:
        conn.close()


def _parse_mysql_dsn(dsn: str, *, check: str, role: Role) -> dict[str, Any]:
    from urllib.parse import unquote, urlsplit

    parts = urlsplit(dsn)
    if parts.scheme not in {"mysql", "mysql+pymysql"}:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="mysql_source",
            reason=(
                f"${role.credential_env} must be a mysql:// URL, got scheme "
                f"{parts.scheme!r}"
            ),
        )
    if not parts.hostname or not parts.username:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="mysql_source",
            reason=f"${role.credential_env} is missing a username or host",
        )
    out: dict[str, Any] = {
        "host": parts.hostname,
        "port": parts.port or 3306,
        "user": unquote(parts.username),
    }
    if parts.password:
        out["password"] = unquote(parts.password)
    database = parts.path.lstrip("/")
    if database:
        out["database"] = database
    return out


def s3_client(role: Role, check: str):
    key_id_var = next(
        (v for v in role.required_env if v.endswith("_AWS_ACCESS_KEY_ID")), None
    )
    secret_var = next(
        (v for v in role.required_env if v.endswith("_AWS_SECRET_ACCESS_KEY")), None
    )
    if not key_id_var or not secret_var:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="s3",
            reason=(
                f"role {role.name} does not declare both an access key id and secret "
                f"env var; declared: {role.required_env}"
            ),
        )
    try:
        access_key = role.secret(key_id_var)
        secret_key = role.secret(secret_var)
    except RoleConfigurationError as exc:
        raise CheckUnverifiable(
            check=check, role=role.name, dependency="s3", reason=str(exc)
        ) from exc

    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.aws_region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 2, "mode": "standard"},
            connect_timeout=int(settings.http_timeout_seconds),
            read_timeout=int(settings.http_timeout_seconds),
        ),
    )


def s3_error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", "Unknown"))


def unverifiable_s3(exc: Exception, *, check: str, role: Role, detail: str) -> CheckUnverifiable:
    if isinstance(exc, ClientError):
        code = s3_error_code(exc)
        if code in {"AccessDenied", "AllAccessDisabled", "403"}:
            return CheckUnverifiable(
                check=check,
                role=role.name,
                dependency="s3",
                reason=(
                    f"S3 denied {detail} for identity "
                    f"'{role.identity.get('name')}' (code {code}). The grants declared "
                    f"in {role.file.name} are not in force."
                ),
            )
        return CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="s3",
            reason=f"S3 error {code} while {detail}",
        )
    if isinstance(exc, BotoCoreError):
        return CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="s3",
            reason=f"cannot reach S3 at {settings.s3_endpoint_url} while {detail}: {exc}",
        )
    return CheckUnverifiable(
        check=check, role=role.name, dependency="s3", reason=f"{detail} failed: {exc}"
    )


def connect_rest_get(role: Role, check: str, path: str) -> Any:
    url_var = next(
        (v for v in role.required_env if v.endswith("_CONNECT_URL")), None
    )
    if url_var is None:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="kafka_connect",
            reason=(
                f"role {role.name} declares no *_CONNECT_URL credential, so the Kafka "
                f"Connect REST API cannot be reached. Declared: {role.required_env}"
            ),
        )
    try:
        base = role.secret(url_var).rstrip("/")
    except RoleConfigurationError as exc:
        raise CheckUnverifiable(
            check=check, role=role.name, dependency="kafka_connect", reason=str(exc)
        ) from exc

    url = f"{base}{path}"
    try:
        response = httpx.get(url, timeout=settings.http_timeout_seconds)
    except httpx.HTTPError as exc:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="kafka_connect",
            reason=f"cannot reach Kafka Connect REST at {_redact(url)}: {exc}",
        ) from exc

    if response.status_code == 404:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="kafka_connect",
            reason=(
                f"Kafka Connect returned 404 for {path}: that connector is not "
                f"registered"
            ),
        )
    if response.status_code in {401, 403}:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="kafka_connect",
            reason=(
                f"Kafka Connect rejected identity '{role.identity.get('name')}' with "
                f"HTTP {response.status_code}"
            ),
        )
    if response.status_code >= 400:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="kafka_connect",
            reason=f"Kafka Connect returned HTTP {response.status_code} for {path}",
        )
    try:
        return response.json()
    except ValueError as exc:
        raise CheckUnverifiable(
            check=check,
            role=role.name,
            dependency="kafka_connect",
            reason=f"Kafka Connect returned non-JSON for {path}",
        ) from exc


def _redact(url: str) -> str:
    from urllib.parse import urlsplit, urlunsplit

    p = urlsplit(url)
    if p.username or p.password:
        netloc = f"***@{p.hostname}" + (f":{p.port}" if p.port else "")
        return urlunsplit((p.scheme, netloc, p.path, p.query, p.fragment))
    return url
