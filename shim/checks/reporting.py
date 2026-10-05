from __future__ import annotations

import hashlib
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, Query

from ..clients import s3_client, s3_error_code, unverifiable_s3
from ..config import settings
from ..deps import role_dep
from ..params import IntervalEnd, IntervalStart, interval_bounds
from ..result import CheckResult, CheckUnverifiable
from ..roles import Role

router = APIRouter(prefix="/checks/reporting", tags=["reporting"])

ROLES = {"reporting_file"}

REPORT_CHECK = "reporting.file"

CHECKSUM_SIDECAR_SUFFIX = ".sha256"


def report_key(interval_start: str, interval_end: str, report: str) -> str:
    prefix = settings.interval_prefix(interval_start, interval_end)
    return f"{settings.reports_prefix}/{prefix}/{report}"


@router.get("/file", response_model=None)
def reporting_file(
    interval_start: str = IntervalStart,
    interval_end: str = IntervalEnd,
    report: str = Query(
        ...,
        description="Report file name within the interval's reports prefix.",
        examples=["nfl_monthly.csv"],
    ),
    role: Role = Depends(role_dep("reporting_file")),
) -> CheckResult:
    s, e, _, _ = interval_bounds(interval_start, interval_end)
    if "/" in report or report in {"", ".", ".."}:
        raise CheckUnverifiable(
            check=REPORT_CHECK,
            role=role.name,
            dependency="s3",
            reason=f"report={report!r} must be a bare file name, not a path",
        )

    bucket = settings.s3_bucket
    key = report_key(s, e, report)
    params = {"interval_start": s, "interval_end": e, "report": report}
    threshold = {
        "file_exists": True,
        "size_bytes_min": 1,
        "checksum_matches_expected": True,
    }
    measured: dict[str, Any] = {
        "bucket": bucket,
        "key_checked": key,
        "s3_uri": f"s3://{bucket}/{key}",
    }

    client = s3_client(role, REPORT_CHECK)
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
    except ClientError as exc:
        code = s3_error_code(exc)
        if code in {"404", "NoSuchKey", "NotFound"}:
            return CheckResult.failed(
                check=REPORT_CHECK,
                role=role.name,
                summary=(
                    f"Report file is ABSENT at s3://{bucket}/{key} — reporting did not "
                    f"produce {report} for {s}..{e}."
                ),
                measured={**measured, "exists": False},
                threshold=threshold,
                params=params,
            )
        raise unverifiable_s3(
            exc, check=REPORT_CHECK, role=role, detail=f"GetObject on {key}"
        ) from exc
    except BotoCoreError as exc:
        raise unverifiable_s3(
            exc, check=REPORT_CHECK, role=role, detail=f"GetObject on {key}"
        ) from exc

    computed = hashlib.sha256(body).hexdigest()
    size = len(body)
    measured.update(
        {
            "exists": True,
            "size_bytes": size,
            "computed_sha256": computed,
            "etag": str(obj.get("ETag", "")).strip('"'),
            "last_modified": (
                obj["LastModified"].isoformat() if obj.get("LastModified") else None
            ),
            "checksum_algorithm": "sha256",
        }
    )

    expected, source = _expected_checksum(client, role, bucket, key, obj)
    measured["expected_sha256"] = expected
    measured["expected_checksum_source"] = source

    if size == 0:
        return CheckResult.failed(
            check=REPORT_CHECK,
            role=role.name,
            summary=(
                f"Report s3://{bucket}/{key} exists but is ZERO bytes — an empty report "
                f"is a failure, not a delivery."
            ),
            measured=measured,
            threshold=threshold,
            params=params,
        )

    if expected is None:
        raise CheckUnverifiable(
            check=REPORT_CHECK,
            role=role.name,
            dependency="s3",
            reason=(
                f"report exists ({size} bytes, sha256 {computed}) but NO expected "
                f"checksum was published — neither object metadata 'sha256' nor a "
                f"{key}{CHECKSUM_SIDECAR_SUFFIX} sidecar exists. Integrity is therefore "
                f"UNVERIFIED; the file's presence alone is not a pass."
            ),
            detail_extra=measured,
        )

    matches = computed.lower() == expected.lower()
    return CheckResult.verdict(
        matches,
        check=REPORT_CHECK,
        role=role.name,
        summary=(
            f"Report s3://{bucket}/{key} is {size} bytes; computed sha256 {computed} "
            + (
                f"matches the expected checksum from {source}."
                if matches
                else f"does NOT match expected {expected} from {source} — the file is corrupt."
            )
        ),
        measured=measured,
        threshold=threshold,
        params=params,
    )


def _expected_checksum(
    client: Any, role: Role, bucket: str, key: str, obj: dict[str, Any]
) -> tuple[str | None, str]:
    metadata = {k.lower(): v for k, v in (obj.get("Metadata") or {}).items()}
    for field in ("sha256", "checksum", "sha256sum"):
        if metadata.get(field):
            return str(metadata[field]).strip(), f"object metadata '{field}'"

    sidecar = f"{key}{CHECKSUM_SIDECAR_SUFFIX}"
    try:
        raw = client.get_object(Bucket=bucket, Key=sidecar)["Body"].read()
    except (ClientError, BotoCoreError):
        return None, "not published"
    text = raw.decode("utf-8", errors="replace").strip()
    token = text.split()[0] if text else ""
    if len(token) == 64 and all(c in "0123456789abcdefABCDEF" for c in token):
        return token, f"sidecar s3://{bucket}/{sidecar}"
    return None, f"sidecar s3://{bucket}/{sidecar} present but not a sha256 hex digest"
