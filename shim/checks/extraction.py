from __future__ import annotations

import io
import json
from typing import Any, Literal

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, Query

from ..clients import s3_client, s3_error_code, unverifiable_s3
from ..config import settings
from ..deps import role_dep
from ..params import IntervalEnd, IntervalStart, interval_bounds
from ..result import CheckResult, CheckUnverifiable, NotImplementedCheck
from ..roles import Role

router = APIRouter(prefix="/checks/extraction", tags=["extraction"])

ROLES = {
    "extraction_s3_marker",
    "extraction_manifest",
    "extraction_parquet_rowcount",
}

MARKER_CHECK = "extraction.s3_marker"
MANIFEST_CHECK = "extraction.manifest"
ROWCOUNT_CHECK = "extraction.parquet_rowcount"

REQUIRED_MANIFEST_FIELDS = ("schema_version", "interval", "layers")


@router.get("/s3-marker", response_model=None)
def s3_marker(
    interval_start: str = IntervalStart,
    interval_end: str = IntervalEnd,
    role: Role = Depends(role_dep("extraction_s3_marker")),
) -> CheckResult:
    s, e, _, _ = interval_bounds(interval_start, interval_end)
    prefix = settings.interval_prefix(s, e)
    key = f"{prefix}/_SUCCESS"
    bucket = settings.s3_bucket
    client = s3_client(role, MARKER_CHECK)

    measured: dict[str, Any] = {
        "bucket": bucket,
        "key_checked": key,
        "s3_uri": f"s3://{bucket}/{key}",
        "endpoint_url": settings.s3_endpoint_url,
    }

    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = s3_error_code(exc)
        if code in {"404", "NoSuchKey", "NotFound"}:
            return CheckResult.failed(
                check=MARKER_CHECK,
                role=role.name,
                summary=(
                    f"_SUCCESS marker is ABSENT at s3://{bucket}/{key} — the extraction "
                    f"stage has not completed for interval {s}..{e}."
                ),
                measured={**measured, "exists": False},
                threshold={"marker_exists": True},
                params={"interval_start": s, "interval_end": e},
            )
        if code == "NoSuchBucket":
            raise CheckUnverifiable(
                check=MARKER_CHECK,
                role=role.name,
                dependency="s3",
                reason=f"bucket '{bucket}' does not exist, so marker state is unknown",
                detail_extra=measured,
            ) from exc
        raise unverifiable_s3(
            exc, check=MARKER_CHECK, role=role, detail=f"HeadObject on {key}"
        ) from exc
    except BotoCoreError as exc:
        raise unverifiable_s3(
            exc, check=MARKER_CHECK, role=role, detail=f"HeadObject on {key}"
        ) from exc

    measured.update(
        {
            "exists": True,
            "size_bytes": int(head.get("ContentLength", 0)),
            "etag": str(head.get("ETag", "")).strip('"'),
            "last_modified": (
                head["LastModified"].isoformat() if head.get("LastModified") else None
            ),
        }
    )
    return CheckResult.passed(
        check=MARKER_CHECK,
        role=role.name,
        summary=(
            f"_SUCCESS marker present at s3://{bucket}/{key} "
            f"({measured['size_bytes']} bytes, modified {measured['last_modified']})."
        ),
        measured=measured,
        threshold={"marker_exists": True},
        params={"interval_start": s, "interval_end": e},
    )


def _fetch_manifest(client: Any, role: Role, check: str, key: str) -> dict[str, Any]:
    bucket = settings.s3_bucket
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        code = s3_error_code(exc)
        if code in {"404", "NoSuchKey", "NotFound"}:
            raise _ManifestMissing(key) from exc
        raise unverifiable_s3(
            exc, check=check, role=role, detail=f"GetObject on {key}"
        ) from exc
    except BotoCoreError as exc:
        raise unverifiable_s3(
            exc, check=check, role=role, detail=f"GetObject on {key}"
        ) from exc

    try:
        parsed = json.loads(body)
    except ValueError as exc:
        raise _ManifestUnparseable(key, str(exc), len(body)) from exc
    if not isinstance(parsed, dict):
        raise _ManifestUnparseable(key, "top-level JSON is not an object", len(body))
    return parsed


class _ManifestMissing(Exception):
    def __init__(self, key: str) -> None:
        self.key = key


class _ManifestUnparseable(Exception):
    def __init__(self, key: str, reason: str, size: int) -> None:
        self.key = key
        self.reason = reason
        self.size = size


@router.get("/manifest", response_model=None)
def manifest(
    interval_start: str = IntervalStart,
    interval_end: str = IntervalEnd,
    role: Role = Depends(role_dep("extraction_manifest")),
) -> CheckResult:
    s, e, _, _ = interval_bounds(interval_start, interval_end)
    prefix = settings.interval_prefix(s, e)
    key = f"{prefix}/manifest.json"
    bucket = settings.s3_bucket
    params = {"interval_start": s, "interval_end": e}
    threshold = {
        "manifest_exists": True,
        "required_fields": list(REQUIRED_MANIFEST_FIELDS),
        "files_with_checksum": "all",
        "declared_row_counts": "> 0",
    }
    base_measured = {"bucket": bucket, "key_checked": key, "s3_uri": f"s3://{bucket}/{key}"}

    client = s3_client(role, MANIFEST_CHECK)
    try:
        doc = _fetch_manifest(client, role, MANIFEST_CHECK, key)
    except _ManifestMissing as exc:
        return CheckResult.failed(
            check=MANIFEST_CHECK,
            role=role.name,
            summary=(
                f"manifest.json is ABSENT at s3://{bucket}/{exc.key} for interval "
                f"{s}..{e}."
            ),
            measured={**base_measured, "exists": False},
            threshold=threshold,
            params=params,
        )
    except _ManifestUnparseable as exc:
        return CheckResult.failed(
            check=MANIFEST_CHECK,
            role=role.name,
            summary=(
                f"manifest.json at s3://{bucket}/{exc.key} exists ({exc.size} bytes) but "
                f"is not valid JSON: {exc.reason}"
            ),
            measured={**base_measured, "exists": True, "parse_error": exc.reason},
            threshold=threshold,
            params=params,
        )

    missing_fields = [f for f in REQUIRED_MANIFEST_FIELDS if f not in doc]
    layers = doc.get("layers") if isinstance(doc.get("layers"), dict) else {}

    layer_report: dict[str, Any] = {}
    files_total = 0
    files_without_checksum: list[str] = []
    declared_rows_total = 0

    for layer_name, layer in sorted(layers.items()):
        if not isinstance(layer, dict):
            layer_report[layer_name] = {"error": "layer entry is not an object"}
            continue
        files = layer.get("files") if isinstance(layer.get("files"), list) else []
        declared_rows = layer.get("row_count")
        missing_ck = [
            str(f.get("path", f"<index {i}>"))
            for i, f in enumerate(files)
            if not isinstance(f, dict)
            or not (f.get("checksum") or f.get("sha256") or f.get("md5"))
        ]
        files_total += len(files)
        files_without_checksum.extend(
            p if p.startswith(f"{layer_name}/") else f"{layer_name}/{p}"
            for p in missing_ck
        )
        if isinstance(declared_rows, int):
            declared_rows_total += declared_rows
        layer_report[layer_name] = {
            "declared_row_count": declared_rows,
            "file_count": len(files),
            "files_missing_checksum": missing_ck,
            "file_paths": [
                str(f.get("path")) for f in files if isinstance(f, dict) and f.get("path")
            ],
        }

    interval_declared = doc.get("interval")
    interval_matches = True
    if isinstance(interval_declared, dict):
        d_start = str(interval_declared.get("start", "")).replace("-", "")
        d_end = str(interval_declared.get("end", "")).replace("-", "")
        interval_matches = (d_start, d_end) == (s, e)
    elif interval_declared is not None:
        interval_matches = str(interval_declared).replace("-", "") in {
            prefix,
            f"{s}_{e}",
        }

    measured = {
        **base_measured,
        "exists": True,
        "schema_version": doc.get("schema_version"),
        "declared_interval": interval_declared,
        "interval_matches_request": interval_matches,
        "missing_required_fields": missing_fields,
        "layer_count": len(layers),
        "layers": layer_report,
        "files_total": files_total,
        "files_missing_checksum": files_without_checksum,
        "declared_rows_total": declared_rows_total,
    }

    problems: list[str] = []
    if missing_fields:
        problems.append(f"missing required field(s) {', '.join(missing_fields)}")
    if not layers:
        problems.append("declares no layers")
    if files_without_checksum:
        problems.append(
            f"{len(files_without_checksum)} file(s) carry no checksum: "
            f"{', '.join(files_without_checksum[:5])}"
        )
    if files_total == 0 and layers:
        problems.append("declares layers but zero files")
    if not interval_matches:
        problems.append(
            f"declared interval {interval_declared!r} does not match requested {s}..{e}"
        )
    if declared_rows_total == 0:
        problems.append("declared row counts total zero")

    ok = not problems
    summary = (
        f"manifest.json valid for {s}..{e}: schema_version "
        f"{doc.get('schema_version')!r}, {len(layers)} layer(s), {files_total} file(s) "
        f"all checksummed, {declared_rows_total} declared rows."
        if ok
        else f"manifest.json INVALID for {s}..{e}: " + "; ".join(problems) + "."
    )

    return CheckResult.verdict(
        ok,
        check=MANIFEST_CHECK,
        role=role.name,
        summary=summary,
        measured=measured,
        threshold=threshold,
        params=params,
    )


@router.get("/parquet-rowcount", response_model=None)
def parquet_rowcount(
    interval_start: str = IntervalStart,
    interval_end: str = IntervalEnd,
    layer: Literal["silver", "gold"] = Query(
        ..., description="Which Parquet layer to count."
    ),
    role: Role = Depends(role_dep("extraction_parquet_rowcount")),
) -> CheckResult:
    if not settings.parquet_rowcount_enabled:
        raise NotImplementedCheck(
            check=ROWCOUNT_CHECK,
            role=role.name,
            reason=(
                "disabled by SHIM_PARQUET_ROWCOUNT_ENABLED=false; row counts are "
                "therefore NOT verified"
            ),
        )

    import pyarrow.parquet as pq

    s, e, _, _ = interval_bounds(interval_start, interval_end)
    prefix = settings.interval_prefix(s, e)
    sub = settings.silver_subprefix if layer == "silver" else settings.gold_subprefix
    layer_prefix = f"{prefix}/{sub}/"
    bucket = settings.s3_bucket
    params = {"interval_start": s, "interval_end": e, "layer": layer}
    client = s3_client(role, ROWCOUNT_CHECK)

    keys: list[dict[str, Any]] = []
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=layer_prefix):
            for obj in page.get("Contents", []) or []:
                if str(obj["Key"]).endswith(".parquet"):
                    keys.append({"key": obj["Key"], "size_bytes": int(obj["Size"])})
    except (ClientError, BotoCoreError) as exc:
        raise unverifiable_s3(
            exc, check=ROWCOUNT_CHECK, role=role, detail=f"ListObjectsV2 on {layer_prefix}"
        ) from exc

    measured: dict[str, Any] = {
        "bucket": bucket,
        "layer": layer,
        "prefix_listed": layer_prefix,
        "parquet_file_count": len(keys),
        "parquet_files": keys,
        "read_method": "boto3 GetObject into memory, pyarrow.parquet footer metadata",
    }

    if not keys:
        return CheckResult.failed(
            check=ROWCOUNT_CHECK,
            role=role.name,
            summary=(
                f"No .parquet objects found under s3://{bucket}/{layer_prefix} — the "
                f"{layer} layer was not written for {s}..{e}."
            ),
            measured={**measured, "actual_rows": 0},
            threshold={"actual_rows_min": 1, "matches_manifest": True},
            params=params,
        )

    actual_rows = 0
    per_file: list[dict[str, Any]] = []
    for entry in keys:
        key = entry["key"]
        try:
            body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except (ClientError, BotoCoreError) as exc:
            raise unverifiable_s3(
                exc, check=ROWCOUNT_CHECK, role=role, detail=f"GetObject on {key}"
            ) from exc
        try:
            pf = pq.ParquetFile(io.BytesIO(body))
            rows = int(pf.metadata.num_rows)
        except Exception as exc:
            raise CheckUnverifiable(
                check=ROWCOUNT_CHECK,
                role=role.name,
                dependency="s3",
                reason=(
                    f"object s3://{bucket}/{key} downloaded ({len(body)} bytes) but is "
                    f"not readable as Parquet: {type(exc).__name__}: {exc}. Row count is "
                    f"UNKNOWN, not zero."
                ),
                detail_extra=measured,
            ) from exc
        actual_rows += rows
        per_file.append(
            {
                "key": key,
                "rows": rows,
                "row_groups": int(pf.metadata.num_row_groups),
                "columns": int(pf.metadata.num_columns),
            }
        )

    measured["actual_rows"] = actual_rows
    measured["per_file"] = per_file

    manifest_key = f"{prefix}/manifest.json"
    declared: int | None = None
    manifest_note: str
    try:
        doc = _fetch_manifest(client, role, ROWCOUNT_CHECK, manifest_key)
        layer_doc = (doc.get("layers") or {}).get(layer)
        if isinstance(layer_doc, dict) and isinstance(layer_doc.get("row_count"), int):
            declared = int(layer_doc["row_count"])
            manifest_note = "declared row_count read from manifest"
        else:
            manifest_note = (
                f"manifest.json has no integer layers.{layer}.row_count, so the "
                f"comparison could not be made"
            )
    except _ManifestMissing:
        manifest_note = "manifest.json absent, so no declared count to compare against"
    except _ManifestUnparseable as exc:
        manifest_note = f"manifest.json unparseable ({exc.reason})"
    except CheckUnverifiable:
        manifest_note = (
            "manifest.json could not be read with this role's credential "
            "(expected if the rowcount role is not granted manifest access)"
        )

    measured["manifest_declared_rows"] = declared
    measured["manifest_key"] = manifest_key
    measured["manifest_comparison"] = manifest_note

    if declared is None:
        raise CheckUnverifiable(
            check=ROWCOUNT_CHECK,
            role=role.name,
            dependency="s3",
            reason=(
                f"counted {actual_rows} real rows across {len(keys)} Parquet file(s), but "
                f"{manifest_note}. The count is measured; the AGREEMENT with the manifest "
                f"is NOT verified, so this is not reported as a pass."
            ),
            detail_extra=measured,
        )

    measured["difference"] = actual_rows - declared
    ok = actual_rows == declared
    return CheckResult.verdict(
        ok,
        check=ROWCOUNT_CHECK,
        role=role.name,
        summary=(
            f"{layer} layer holds {actual_rows} rows across {len(keys)} file(s); manifest "
            f"declares {declared} — "
            + ("they MATCH." if ok else f"MISMATCH of {actual_rows - declared}.")
        ),
        measured=measured,
        threshold={"actual_rows_min": 1, "matches_manifest": True},
        params=params,
    )
