from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from psycopg import sql

from ..clients import postgres
from ..config import STAGING_VERSION_ORDER, TABLE_PAIRS, TablePair, settings
from ..deps import role_dep
from ..params import IntervalEnd, IntervalStart, column, interval_bounds, qualified_table
from ..result import CheckResult, CheckUnverifiable
from ..roles import Role

router = APIRouter(prefix="/checks/ingestion", tags=["ingestion"])

ROLES = {"ingestion_reconcile", "ingestion_quality", "ingestion_freshness"}

RECONCILE_CHECK = "ingestion.reconcile"
QUALITY_CHECK = "ingestion.quality"
FRESHNESS_CHECK = "ingestion.freshness"

PAIR_NAMES = ", ".join(p.name for p in TABLE_PAIRS)


def _window(s_date: date, e_date: date) -> tuple[datetime, datetime]:
    """The half-open UTC window [start 00:00, day after end 00:00) ingestion uses."""
    lo = datetime(s_date.year, s_date.month, s_date.day, tzinfo=timezone.utc)
    hi_day = e_date + timedelta(days=1)
    hi = datetime(hi_day.year, hi_day.month, hi_day.day, tzinfo=timezone.utc)
    return lo, hi


def _latest_staging(pair: TablePair, with_business_key: bool = False) -> sql.Composed:
    """Newest CDC version of each source row: event column as `ev`, and the
    business key as `bk` when asked for."""
    cols = [sql.SQL("{} AS ev").format(column(pair.staging_event))]
    if with_business_key:
        cols.append(sql.SQL("{} AS bk").format(column(pair.business_key)))
    order = sql.SQL(", ").join(
        sql.SQL("{} DESC").format(column(c)) for c in STAGING_VERSION_ORDER
    )
    return sql.SQL(
        "(SELECT DISTINCT ON ({pk}) {cols} FROM {table} ORDER BY {pk}, {order}) latest"
    ).format(
        pk=column(pair.source_pk),
        cols=sql.SQL(", ").join(cols),
        table=qualified_table(pair.staging),
        order=order,
    )


def _reconcile_pair(conn: Any, pair: TablePair, lo: datetime, hi: datetime) -> dict[str, Any]:
    stmt = sql.SQL(
        "SELECT "
        " (SELECT COUNT(*) FROM {staging} WHERE {sev} >= %(lo)s AND {sev} < %(hi)s),"
        " (SELECT COUNT(*) FROM {latest} WHERE ev >= %(lo)s AND ev < %(hi)s),"
        " (SELECT COUNT(*) FROM {utm} WHERE {uev} >= %(lo)s AND {uev} < %(hi)s)"
    ).format(
        staging=qualified_table(pair.staging),
        sev=column(pair.staging_event),
        latest=_latest_staging(pair),
        utm=qualified_table(pair.utm),
        uev=column(pair.utm_event),
    )
    with conn.cursor() as cur:
        cur.execute(stmt, {"lo": lo, "hi": hi})
        raw, source_rows, utm_rows = (int(v) for v in cur.fetchone())
    difference = source_rows - utm_rows
    tolerance = settings.reconcile_tolerance_rows
    return {
        "staging_table": pair.staging,
        "utm_table": pair.utm,
        "interval_column": {"staging": pair.staging_event, "utm": pair.utm_event},
        "staging_rows": source_rows,
        "staging_raw_rows": raw,
        "utm_rows": utm_rows,
        "difference": difference,
        "abs_difference": abs(difference),
        "empty": source_rows == 0 and utm_rows == 0,
        "ok": abs(difference) <= tolerance,
    }


@router.get("/reconcile", response_model=None)
def reconcile(
    interval_start: str = IntervalStart,
    interval_end: str = IntervalEnd,
    role: Role = Depends(role_dep("ingestion_reconcile")),
) -> CheckResult:
    s, e, s_date, e_date = interval_bounds(interval_start, interval_end)
    lo, hi = _window(s_date, e_date)
    tolerance = settings.reconcile_tolerance_rows
    params = {"interval_start": s, "interval_end": e}

    with postgres(role, RECONCILE_CHECK) as conn:
        tables = {p.name: _reconcile_pair(conn, p, lo, hi) for p in TABLE_PAIRS}

    failed = [n for n, t in tables.items() if not t["ok"]]
    empty = [n for n, t in tables.items() if t["empty"]]
    staging_total = sum(t["staging_rows"] for t in tables.values())
    utm_total = sum(t["utm_rows"] for t in tables.values())
    nothing = staging_total == 0 and utm_total == 0
    ok = not failed and not nothing

    measured = {
        "table_pairs_checked": list(tables),
        "table_pairs_failed": failed,
        "table_pairs_empty": empty,
        "tables": tables,
        "staging_rows": staging_total,
        "utm_rows": utm_total,
        "difference": staging_total - utm_total,
        "staging_rows_basis": (
            "newest CDC version of each source primary key whose event time falls in "
            "the window; staging is an append-only CDC log, so raw rows over-count"
        ),
        "interval_sql_bounds": {"gte": lo.isoformat(), "lt": hi.isoformat()},
    }
    per_table = "; ".join(
        f"{n} staging {t['staging_rows']} vs utm {t['utm_rows']}" for n, t in tables.items()
    )

    if nothing:
        summary = (
            f"All {len(tables)} table pairs ({PAIR_NAMES}) report ZERO rows in both "
            f"staging and utm for {s}..{e}. Counts match, but there is nothing to "
            f"reconcile: treat this as 'no data loaded', not as a healthy load."
        )
    elif ok:
        summary = (
            f"Reconciled all {len(tables)} table pairs for {s}..{e} (tolerance "
            f"{tolerance}): {per_table}."
            + (
                f" No rows on either side for: {', '.join(empty)}."
                if empty
                else ""
            )
        )
    else:
        summary = (
            f"Reconciliation FAILED for {s}..{e} in {len(failed)} of {len(tables)} "
            f"table pairs ({', '.join(failed)}; tolerance {tolerance}): {per_table}."
        )

    return CheckResult.verdict(
        ok,
        check=RECONCILE_CHECK,
        role=role.name,
        summary=summary,
        measured=measured,
        threshold={
            "abs_difference_max_per_table": tolerance,
            "table_pairs_failed_max": 0,
            "staging_rows_min": 1,
        },
        params=params,
    )


def _quality_table(
    conn: Any, pair: TablePair, layer: str, lo: datetime, hi: datetime, cap: int
) -> dict[str, Any]:
    if layer == "utm":
        table_name = pair.utm
        source = sql.SQL("(SELECT {bk} AS bk, {ev} AS ev FROM {table}) q").format(
            bk=column(pair.business_key),
            ev=column(pair.utm_event),
            table=qualified_table(pair.utm),
        )
        basis = "every utm row in the window"
    else:
        table_name = pair.staging
        source = _latest_staging(pair, with_business_key=True)
        basis = "newest CDC version of each source row in the window"

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT COUNT(*), "
                "COUNT(*) FILTER (WHERE bk IS NULL OR btrim(bk::text) = '') "
                "FROM {src} WHERE ev >= %s AND ev < %s"
            ).format(src=source),
            (lo, hi),
        )
        total, null_keys = (int(v) for v in cur.fetchone())
        cur.execute(
            sql.SQL(
                "SELECT bk::text, COUNT(*), COUNT(*) OVER () FROM {src} "
                "WHERE ev >= %s AND ev < %s AND bk IS NOT NULL AND btrim(bk::text) <> '' "
                "GROUP BY bk HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC, bk LIMIT %s"
            ).format(src=source),
            (lo, hi, cap),
        )
        dup_rows = cur.fetchall()

    dup_groups = int(dup_rows[0][2]) if dup_rows else 0
    return {
        "table": table_name,
        "business_key_column": pair.business_key,
        "rows_basis": basis,
        "rows_in_interval": total,
        "null_business_keys": null_keys,
        "duplicate_business_key_groups": dup_groups,
        "duplicate_business_keys": [
            {"business_key": r[0], "occurrences": int(r[1])} for r in dup_rows
        ],
        "ok": null_keys == 0 and dup_groups == 0,
    }


def _batch_for_window(conn: Any, lo: datetime, hi: datetime, cap: int) -> dict[str, Any]:
    """The ingestion batch recorded for exactly this window, and what it rejected."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT batch_id, batch_key, status, rows_read, rows_loaded, "
                "       rows_rejected, failure_reason, started_at, finished_at "
                "FROM {batch} WHERE window_start = %s AND window_end = %s "
                "ORDER BY batch_id DESC"
            ).format(batch=qualified_table(settings.ingestion_batch_table)),
            (lo, hi),
        )
        batches = cur.fetchall()
        if not batches:
            return {"recorded": False}
        b = batches[0]
        cur.execute(
            sql.SQL(
                "SELECT source_table, reject_reason, COUNT(*) FROM {rej} "
                "WHERE utm_batch_id = %s GROUP BY 1, 2 ORDER BY 1, 2"
            ).format(rej=qualified_table(settings.ingestion_reject_table)),
            (b[0],),
        )
        grouped = cur.fetchall()
        cur.execute(
            sql.SQL(
                "SELECT source_table, reject_reason, business_key FROM {rej} "
                "WHERE utm_batch_id = %s ORDER BY reject_id LIMIT %s"
            ).format(rej=qualified_table(settings.ingestion_reject_table)),
            (b[0], cap),
        )
        samples = cur.fetchall()

    by_table: dict[str, dict[str, int]] = {}
    for source_table, reason, n in grouped:
        by_table.setdefault(str(source_table), {})[str(reason)] = int(n)
    reject_rows = sum(sum(v.values()) for v in by_table.values())
    return {
        "recorded": True,
        "batches_for_window": len(batches),
        "batch_id": int(b[0]),
        "batch_key": b[1],
        "status": b[2],
        "rows_read": int(b[3]),
        "rows_loaded": int(b[4]),
        "rows_rejected": int(b[5]),
        "failure_reason": b[6],
        "started_at": b[7].isoformat() if b[7] else None,
        "finished_at": b[8].isoformat() if b[8] else None,
        "reject_rows": reject_rows,
        "rejects_by_table_and_reason": by_table,
        "reject_samples": [
            {"source_table": r[0], "reject_reason": r[1], "business_key": r[2]}
            for r in samples
        ],
    }


@router.get("/quality", response_model=None)
def quality(
    interval_start: str = IntervalStart,
    interval_end: str = IntervalEnd,
    layer: Literal["utm", "staging"] = Query(
        "utm",
        description=(
            "Which layer's business keys to inspect. utm (default) is what ingestion "
            "produced; staging is what replication landed. The ingestion batch status "
            "and its rejects are judged for both."
        ),
    ),
    role: Role = Depends(role_dep("ingestion_quality")),
) -> CheckResult:
    s, e, s_date, e_date = interval_bounds(interval_start, interval_end)
    lo, hi = _window(s_date, e_date)
    cap = settings.quality_sample_cap

    with postgres(role, QUALITY_CHECK) as conn:
        tables = {
            p.name: _quality_table(conn, p, layer, lo, hi, cap) for p in TABLE_PAIRS
        }
        batch = _batch_for_window(conn, lo, hi, cap)

    total = sum(t["rows_in_interval"] for t in tables.values())
    null_keys = sum(t["null_business_keys"] for t in tables.values())
    dup_groups = sum(t["duplicate_business_key_groups"] for t in tables.values())
    dirty = [n for n, t in tables.items() if not t["ok"]]
    rejected = (
        max(batch["reject_rows"], batch["rows_rejected"]) if batch["recorded"] else 0
    )
    batch_ok = batch["recorded"] and batch["status"] == "SUCCEEDED" and rejected == 0
    ok = total > 0 and not dirty and batch_ok

    measured = {
        "layer": layer,
        "tables_checked": list(tables),
        "tables_failed": dirty,
        "tables": tables,
        "rows_in_interval": total,
        "null_business_keys": null_keys,
        "duplicate_business_key_groups": dup_groups,
        "ingestion_batch": batch,
        "rejected_rows": rejected,
        "samples_capped_at": cap,
        "interval_sql_bounds": {"gte": lo.isoformat(), "lt": hi.isoformat()},
    }

    if ok:
        summary = (
            f"Quality clean for {s}..{e} across all {len(tables)} {layer} tables "
            f"({PAIR_NAMES}): {total} rows, no null or duplicate business keys; "
            f"ingestion batch {batch['batch_key']} {batch['status']} with {rejected} "
            f"rejects."
        )
    else:
        bits = []
        if not batch["recorded"]:
            bits.append(
                f"no ingestion batch is recorded in {settings.ingestion_batch_table} "
                f"for exactly this window, so ingestion is not known to have run"
            )
        else:
            if batch["status"] != "SUCCEEDED":
                bits.append(
                    f"ingestion batch {batch['batch_key']} is {batch['status']}"
                    + (
                        f" ({batch['failure_reason']})"
                        if batch["failure_reason"]
                        else ""
                    )
                )
            if rejected:
                bits.append(f"{rejected} row(s) rejected by ingestion")
        if total == 0:
            bits.append(
                f"the {layer} tables hold ZERO rows in the interval, so there is "
                f"nothing whose quality could be clean"
            )
        for name in dirty:
            t = tables[name]
            bits.append(
                f"{t['table']}: {t['null_business_keys']} null/blank and "
                f"{t['duplicate_business_key_groups']} duplicated business key(s)"
            )
        summary = f"Quality FAILED for {s}..{e}: " + "; ".join(bits) + "."

    return CheckResult.verdict(
        ok,
        check=QUALITY_CHECK,
        role=role.name,
        summary=summary,
        measured=measured,
        threshold={
            "rows_in_interval_min": 1,
            "null_business_keys_max": 0,
            "duplicate_business_key_groups_max": 0,
            "ingestion_batch_status": "SUCCEEDED",
            "rejected_rows_max": 0,
        },
        params={"interval_start": s, "interval_end": e, "layer": layer},
    )


def _age(value: datetime | None, now: datetime) -> float | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return max(0.0, (now - value).total_seconds())


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _round(value: float | None) -> float | None:
    return round(value, 3) if value is not None else None


@router.get("/freshness", response_model=None)
def freshness(role: Role = Depends(role_dep("ingestion_freshness"))) -> CheckResult:
    utm_limit = settings.freshness_threshold_seconds
    beat_limit = settings.heartbeat_max_age_seconds
    heartbeat_on = bool(settings.heartbeat_table)

    def newest(conn: Any, table: str, col: str) -> datetime | None:
        stmt = sql.SQL("SELECT MAX({col}) FROM {table}").format(
            col=column(col), table=qualified_table(table)
        )
        with conn.cursor() as cur:
            cur.execute(stmt)
            return cur.fetchone()[0]

    with postgres(role, FRESHNESS_CHECK) as conn:
        staging = {
            p.name: newest(conn, p.staging, settings.staging_freshness_column)
            for p in TABLE_PAIRS
        }
        utm = {
            p.name: newest(conn, p.utm, settings.utm_freshness_column)
            for p in TABLE_PAIRS
        }
        beat_rows: list[tuple[Any, ...]] = []
        if heartbeat_on:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT {src}::text, MAX({col}), COUNT(*) FROM {table} "
                        "WHERE {col} IS NOT NULL AND (%(only)s = '' OR {src} = %(only)s) "
                        "GROUP BY {src} ORDER BY {src}"
                    ).format(
                        src=column(settings.heartbeat_source_column),
                        col=column(settings.heartbeat_column),
                        table=qualified_table(settings.heartbeat_table),
                    ),
                    {"only": settings.heartbeat_source},
                )
                beat_rows = cur.fetchall()
        with conn.cursor() as cur:
            cur.execute("SELECT now()")
            now = cur.fetchone()[0]

    staging_max = max((v for v in staging.values() if v), default=None)
    utm_max = max((v for v in utm.values() if v), default=None)
    staging_age = _age(staging_max, now)
    utm_age = _age(utm_max, now)
    sources = {
        str(name): {
            "last_beat_at": _iso(last),
            "age_seconds": _round(_age(last, now)),
            "beats": int(n),
        }
        for name, last, n in beat_rows
    }
    # Every source must be beating, so the verdict rides on the stalest one.
    beat = min((last for _, last, _ in beat_rows), default=None)
    beat_age = _age(beat, now)

    measured: dict[str, Any] = {
        "database_now": now.isoformat(),
        "staging_clock": f"MAX({settings.staging_freshness_column}) over staging: {PAIR_NAMES}",
        "utm_clock": f"MAX({settings.utm_freshness_column}) over the utm targets of: {PAIR_NAMES}",
        "staging_last_loaded_at": _iso(staging_max),
        "utm_last_loaded_at": _iso(utm_max),
        "staging_age_seconds": _round(staging_age),
        "utm_age_seconds": _round(utm_age),
        "tables": {
            p.name: {
                "staging_table": p.staging,
                "staging_last_loaded_at": _iso(staging[p.name]),
                "staging_age_seconds": _round(_age(staging[p.name], now)),
                "utm_table": p.utm,
                "utm_last_loaded_at": _iso(utm[p.name]),
                "utm_age_seconds": _round(_age(utm[p.name], now)),
            }
            for p in TABLE_PAIRS
        },
        "heartbeat_configured": heartbeat_on,
    }
    threshold: dict[str, Any] = {"max_age_seconds": utm_limit}

    if heartbeat_on:
        clock = f"{settings.heartbeat_table}.{settings.heartbeat_column}"
        measured.update(
            end_to_end_basis=(
                f"replication heartbeat: newest {clock} per source, a row written at "
                f"the source on a timer and landed in staging by the same path as data"
            ),
            heartbeat_clock=clock,
            heartbeat_source_filter=settings.heartbeat_source or None,
            heartbeat_sources=sources,
            heartbeat_rows=sum(v["beats"] for v in sources.values()),
            heartbeat_last_beat_at=_iso(beat),
            heartbeat_age_seconds=_round(beat_age),
            heartbeat_age_basis=(
                "the STALEST source's newest beat: every source must be fresh"
            ),
            staging_age_role=(
                "informational: an idle source ages this clock without anything "
                "being wrong, so it is not part of the verdict"
            ),
        )
        threshold["heartbeat_max_age_seconds"] = beat_limit
        only = (
            f" for source '{settings.heartbeat_source}'" if settings.heartbeat_source else ""
        )
        if beat is None:
            raise CheckUnverifiable(
                check=FRESHNESS_CHECK,
                role=role.name,
                dependency="nfl_postgres",
                reason=(
                    f"the replication heartbeat table {settings.heartbeat_table} is "
                    f"configured but holds no beat{only}, "
                    f"so end-to-end staleness cannot be judged. Newest staging data "
                    f"landed {staging_age:.0f}s ago, which cannot distinguish an idle "
                    f"source from a dead pipeline. Not a pass and not a fail."
                    if staging_age is not None
                    else f"the replication heartbeat table {settings.heartbeat_table} "
                    f"is configured but holds no beat{only}, and staging holds no "
                    f"data either, so end-to-end staleness cannot be judged."
                ),
                detail_extra=measured,
            )
    else:
        measured["end_to_end_basis"] = (
            "NO heartbeat configured (SHIM_HEARTBEAT_TABLE is empty): the staging "
            "clock is the newest DATA row landed, so an idle source and a dead "
            "pipeline look the same"
        )

    empty = [
        name for name, value in (("staging", staging_max), ("utm", utm_max)) if value is None
    ]
    if empty:
        return CheckResult.failed(
            check=FRESHNESS_CHECK,
            role=role.name,
            summary=(
                f"Freshness cannot be satisfied: no {' and no '.join(empty)} table "
                f"({PAIR_NAMES}) contains a row, so there is no data to be fresh. "
                f"This is a FAIL, not a pass-by-absence."
            ),
            measured=measured,
            threshold=threshold,
        )

    utm_ok = utm_age <= utm_limit
    if heartbeat_on:
        replication_ok = beat_age <= beat_limit
        replication_text = (
            f"End-to-end replication heartbeat ({len(sources)} source(s), stalest) is "
            f"{beat_age:.0f}s old against a {beat_limit}s limit "
            f"({'within' if replication_ok else 'STALE'})"
        )
    else:
        replication_ok = staging_age <= utm_limit
        replication_text = (
            f"Newest staging DATA row landed {staging_age:.0f}s ago against a "
            f"{utm_limit}s limit ({'within' if replication_ok else 'STALE'}; no "
            f"heartbeat is configured, so an idle source reads as stale)"
        )
    ok = utm_ok and replication_ok
    return CheckResult.verdict(
        ok,
        check=FRESHNESS_CHECK,
        role=role.name,
        summary=(
            f"{replication_text}; utm last loaded {utm_age:.0f}s ago against a "
            f"{utm_limit}s limit ({'within' if utm_ok else 'STALE'})."
        ),
        measured=measured,
        threshold=threshold,
    )
