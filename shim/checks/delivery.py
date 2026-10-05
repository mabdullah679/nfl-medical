from __future__ import annotations

import hashlib
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import paramiko
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, Query

from ..clients import s3_client, s3_error_code, unverifiable_s3
from ..config import settings
from ..deps import role_dep
from ..params import IntervalEnd, IntervalStart, interval_bounds
from ..result import CheckResult, CheckUnverifiable
from ..roles import Role
from ..roles import RoleConfigurationError
from .reporting import report_key

router = APIRouter(prefix="/checks/delivery", tags=["delivery"])

ROLES = {"delivery_sftp"}

SFTP_CHECK = "delivery.sftp"

READ_CHUNK = 262144


@contextmanager
def _sftp(role: Role) -> Iterator[paramiko.SFTPClient]:
    try:
        key_path = Path(role.secret("SHIM_DELIVERY_SFTP_PRIVATE_KEY_PATH"))
        username = role.secret("SHIM_DELIVERY_SFTP_USER")
        host_key_path = Path(role.secret("SHIM_DELIVERY_SFTP_HOST_KEY_PATH"))
    except RoleConfigurationError as exc:
        raise CheckUnverifiable(
            check=SFTP_CHECK, role=role.name, dependency="sftp", reason=str(exc)
        ) from exc

    if not key_path.is_file():
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="sftp",
            reason=f"private key not found at {key_path}",
        )
    if not host_key_path.is_file():
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="sftp",
            reason=(
                f"known-hosts file not found at {host_key_path}. Host-key verification "
                f"is mandatory: without it this check could be satisfied by an impostor "
                f"server reporting any file as delivered."
            ),
        )

    pkey: paramiko.PKey | None = None
    errors: list[str] = []
    for loader in (
        paramiko.Ed25519Key,
        paramiko.ECDSAKey,
        paramiko.RSAKey,
    ):
        try:
            pkey = loader.from_private_key_file(str(key_path))
            break
        except Exception as exc:
            errors.append(f"{loader.__name__}: {exc}")
    if pkey is None:
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="sftp",
            reason=f"private key at {key_path} is unreadable ({'; '.join(errors)})",
        )

    client = paramiko.SSHClient()
    try:
        client.load_host_keys(str(host_key_path))
    except OSError as exc:
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="sftp",
            reason=f"cannot load known-hosts {host_key_path}: {exc}",
        ) from exc
    client.set_missing_host_key_policy(paramiko.RejectPolicy())

    try:
        client.connect(
            hostname=settings.sftp_host,
            port=settings.sftp_port,
            username=username,
            pkey=pkey,
            timeout=settings.http_timeout_seconds,
            banner_timeout=settings.http_timeout_seconds,
            auth_timeout=settings.http_timeout_seconds,
            allow_agent=False,
            look_for_keys=False,
        )
    except paramiko.BadHostKeyException as exc:
        client.close()
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="sftp",
            reason=(
                f"SFTP host key for {settings.sftp_host}:{settings.sftp_port} does NOT "
                f"match {host_key_path} — refusing to trust the server. Got "
                f"{exc.key.get_base64()[:32]}..."
            ),
        ) from exc
    except paramiko.AuthenticationException as exc:
        client.close()
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="sftp",
            reason=(
                f"SFTP rejected identity '{username}' with the declared key: {exc}"
            ),
        ) from exc
    except (paramiko.SSHException, OSError) as exc:
        client.close()
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="sftp",
            reason=(
                f"cannot reach SFTP at {settings.sftp_host}:{settings.sftp_port}: "
                f"{type(exc).__name__}: {exc}"
            ),
        ) from exc

    try:
        sftp = client.open_sftp()
    except paramiko.SSHException as exc:
        client.close()
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="sftp",
            reason=f"SFTP subsystem unavailable: {exc}",
        ) from exc

    try:
        yield sftp
    finally:
        sftp.close()
        client.close()


def _remote_digest(sftp: paramiko.SFTPClient, path: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    with sftp.open(path, "rb") as handle:
        handle.prefetch()
        while True:
            chunk = handle.read(READ_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


@router.get("/sftp", response_model=None)
def delivery_sftp(
    interval_start: str = IntervalStart,
    interval_end: str = IntervalEnd,
    report: str = Query(
        ..., description="Report file name as delivered.", examples=["nfl_monthly.csv"]
    ),
    role: Role = Depends(role_dep("delivery_sftp")),
) -> CheckResult:
    s, e, _, _ = interval_bounds(interval_start, interval_end)
    if "/" in report or report in {"", ".", ".."}:
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="sftp",
            reason=f"report={report!r} must be a bare file name, not a path",
        )

    base = settings.sftp_recipient_dir.rstrip("/")
    final_path = f"{base}/{report}"
    temp_path = f"{final_path}{settings.sftp_temp_suffix}"
    params = {"interval_start": s, "interval_end": e, "report": report}
    threshold = {
        "final_name_present": True,
        "temp_name_absent": True,
        "size_matches_source": True,
        "checksum_matches_source": True,
    }
    measured: dict[str, Any] = {
        "sftp_host": f"{settings.sftp_host}:{settings.sftp_port}",
        "recipient_dir": base,
        "final_path": final_path,
        "temp_path_checked": temp_path,
        "host_key_verified": True,
    }

    with _sftp(role) as sftp:
        try:
            entries = {a.filename: a for a in sftp.listdir_attr(base)}
        except FileNotFoundError as exc:
            raise CheckUnverifiable(
                check=SFTP_CHECK,
                role=role.name,
                dependency="sftp",
                reason=f"recipient directory {base} does not exist on the SFTP server",
                detail_extra=measured,
            ) from exc
        except PermissionError as exc:
            raise CheckUnverifiable(
                check=SFTP_CHECK,
                role=role.name,
                dependency="sftp",
                reason=(
                    f"identity '{role.identity.get('name')}' cannot list {base}; the "
                    f"chroot/ACL declared in {role.file.name} is not in force"
                ),
                detail_extra=measured,
            ) from exc

        temp_name = Path(temp_path).name
        final_attr = entries.get(report)
        temp_attr = entries.get(temp_name)
        leftover_temps = sorted(
            n
            for n, a in entries.items()
            if n.startswith(report) and n != report and not stat.S_ISDIR(a.st_mode or 0)
        )

        measured["directory_entry_count"] = len(entries)
        measured["temp_name_present"] = temp_attr is not None
        measured["leftover_partial_names"] = leftover_temps

        if final_attr is None:
            if temp_attr is not None:
                summary = (
                    f"Delivery INCOMPLETE: {temp_path} exists "
                    f"({temp_attr.st_size} bytes) but the final name {final_path} does "
                    f"not — the upload was never renamed, so the recipient cannot see it."
                )
            else:
                summary = (
                    f"Delivery FAILED: neither {final_path} nor {temp_path} exists in "
                    f"{base} for {s}..{e}."
                )
            return CheckResult.failed(
                check=SFTP_CHECK,
                role=role.name,
                summary=summary,
                measured={**measured, "final_name_present": False},
                threshold=threshold,
                params=params,
            )

        measured["final_name_present"] = True
        measured["delivered_size_bytes"] = int(final_attr.st_size or 0)
        measured["delivered_mtime"] = final_attr.st_mtime

        remote_digest, read_bytes = _remote_digest(sftp, final_path)

    measured["delivered_sha256"] = remote_digest
    measured["delivered_bytes_read"] = read_bytes

    source = _source_object(s, e, report, role)
    measured["source"] = source

    problems: list[str] = []
    if temp_attr is not None:
        problems.append(
            f"temp file {temp_name} still present, so the rename did not clean up"
        )

    if source.get("available"):
        size_match = source["size_bytes"] == read_bytes
        sum_match = str(source["sha256"]).lower() == remote_digest.lower()
        measured["size_matches_source"] = size_match
        measured["checksum_matches_source"] = sum_match
        if not size_match:
            problems.append(
                f"size mismatch: delivered {read_bytes} vs source {source['size_bytes']}"
            )
        if not sum_match:
            problems.append(
                f"checksum mismatch: delivered {remote_digest} vs source {source['sha256']}"
            )
    else:
        raise CheckUnverifiable(
            check=SFTP_CHECK,
            role=role.name,
            dependency="s3",
            reason=(
                f"file {final_path} is present under its final name "
                f"({read_bytes} bytes, sha256 {remote_digest}), but the SOURCE object "
                f"could not be read to compare against ({source.get('reason')}). "
                f"Presence is proven; INTEGRITY is not, so this is not a pass."
            ),
            detail_extra=measured,
        )

    ok = not problems
    return CheckResult.verdict(
        ok,
        check=SFTP_CHECK,
        role=role.name,
        summary=(
            f"{report} delivered to {final_path} under its final name, {read_bytes} "
            f"bytes, sha256 matches source, no temp file left behind."
            if ok
            else f"Delivery verification FAILED for {final_path}: "
            + "; ".join(problems)
            + "."
        ),
        measured=measured,
        threshold=threshold,
        params=params,
    )


def _source_object(
    interval_start: str, interval_end: str, report: str, role: Role
) -> dict[str, Any]:
    key = report_key(interval_start, interval_end, report)
    bucket = settings.s3_bucket
    source: dict[str, Any] = {
        "kind": "s3",
        "bucket": bucket,
        "key": key,
        "s3_uri": f"s3://{bucket}/{key}",
    }
    try:
        client = s3_client(role, SFTP_CHECK)
    except CheckUnverifiable as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else {}
        return {
            **source,
            "available": False,
            "reason": str(detail.get("reason", "no S3 credential declared for this role")),
        }
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        return {
            **source,
            "available": False,
            "reason": f"S3 {s3_error_code(exc)} reading {key}",
        }
    except BotoCoreError as exc:
        return {**source, "available": False, "reason": f"cannot reach S3: {exc}"}
    return {
        **source,
        "available": True,
        "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }
