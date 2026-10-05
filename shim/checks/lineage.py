from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from psycopg import sql

from ..clients import postgres
from ..config import settings
from ..deps import role_dep
from ..params import IntervalEnd, IntervalStart, interval_bounds, qualified_table
from ..result import CheckResult
from ..roles import Role

router = APIRouter(prefix="/checks", tags=["lineage"])

ROLES = {"lineage"}

LINEAGE_CHECK = "lineage"

PIPELINE_STAGES = (
    "replication",
    "ingestion",
    "extraction",
    "reporting",
    "delivery",
)

TERMINAL_OK = {"success", "succeeded", "completed", "ok"}
TERMINAL_BAD = {"failed", "error", "aborted", "cancelled"}


@router.get("/lineage", response_model=None)
def lineage(
    interval_start: str = IntervalStart,
    interval_end: str = IntervalEnd,
    role: Role = Depends(role_dep("lineage")),
) -> CheckResult:
    s, e, s_date, e_date = interval_bounds(interval_start, interval_end)
    table = qualified_table(settings.lineage_table)
    params = {"interval_start": s, "interval_end": e}

    # An event belongs to exactly one interval. Matching on a range would let
    # events of one interval vouch for another that never ran.
    stmt = sql.SQL(
        "SELECT stage, status, COUNT(*) AS events, "
        "       MIN(event_time) AS first_event, MAX(event_time) AS last_event "
        "FROM {table} WHERE interval_start = %s AND interval_end = %s "
        "GROUP BY stage, status ORDER BY stage, status"
    ).format(table=table)
    overlap_stmt = sql.SQL(
        "SELECT DISTINCT interval_start, interval_end FROM {table} "
        "WHERE interval_start <= %s AND interval_end >= %s "
        "  AND NOT (interval_start = %s AND interval_end = %s) "
        "ORDER BY interval_start, interval_end LIMIT 20"
    ).format(table=table)

    with postgres(role, LINEAGE_CHECK) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt, (s_date, e_date))
            rows = cur.fetchall()
            cur.execute(overlap_stmt, (e_date, s_date, s_date, e_date))
            overlapping = [
                {"interval_start": str(a), "interval_end": str(b)}
                for a, b in cur.fetchall()
            ]

    observed: dict[str, list[dict[str, Any]]] = {}
    for stage, status, events, first_event, last_event in rows:
        observed.setdefault(str(stage), []).append(
            {
                "status": str(status),
                "events": int(events),
                "first_event": first_event.isoformat() if first_event else None,
                "last_event": last_event.isoformat() if last_event else None,
            }
        )

    stage_report: dict[str, Any] = {}
    present: list[str] = []
    missing: list[str] = []
    failed: list[str] = []
    unfinished: list[str] = []

    for stage in PIPELINE_STAGES:
        records = observed.get(stage, [])
        if not records:
            missing.append(stage)
            stage_report[stage] = {
                "recorded": False,
                "verdict": "NO EVENT RECORDED — stage either did not run or does not emit",
            }
            continue
        present.append(stage)
        # The LATEST terminal event decides, so a stage that failed and was then
        # re-run successfully for the same interval is not held failed forever,
        # and one that succeeded and later failed is not held passed.
        terminal = [
            r
            for r in records
            if r["status"].lower() in (TERMINAL_OK | TERMINAL_BAD) and r["last_event"]
        ]
        latest = max(terminal, key=lambda r: r["last_event"]) if terminal else None
        if latest is None:
            unfinished.append(stage)
            statuses = sorted({r["status"].lower() for r in records})
            verdict = f"NOT FINISHED - no terminal event (statuses: {', '.join(statuses)})"
        elif latest["status"].lower() in TERMINAL_BAD:
            failed.append(stage)
            verdict = "FAILED"
        else:
            verdict = "succeeded"
        stage_report[stage] = {
            "recorded": True,
            "verdict": verdict,
            "latest_terminal_status": latest["status"] if latest else None,
            "latest_terminal_event": latest["last_event"] if latest else None,
            "by_status": records,
            "total_events": sum(r["events"] for r in records),
        }

    unexpected = sorted(set(observed) - set(PIPELINE_STAGES))
    measured = {
        "lineage_table": settings.lineage_table,
        "interval_match": "exact: interval_start = start AND interval_end = end",
        "matched_interval": {"interval_start": str(s_date), "interval_end": str(e_date)},
        "other_recorded_intervals_overlapping_request_not_counted": overlapping,
        "expected_stages": list(PIPELINE_STAGES),
        "stages_with_events": present,
        "stages_missing_events": missing,
        "stages_failed": failed,
        "stages_unfinished": unfinished,
        "unexpected_stages_recorded": unexpected,
        "stages": stage_report,
        "total_events": sum(int(r[2]) for r in rows),
    }
    threshold = {
        "total_events_min": 1,
        "stages_missing_events_max": 0,
        "stages_failed_max": 0,
        "stages_unfinished_max": 0,
        "expected_stages": list(PIPELINE_STAGES),
    }

    ok = bool(rows) and not missing and not failed and not unfinished
    if ok:
        summary = (
            f"Full lineage recorded for exactly {s}..{e}: all {len(PIPELINE_STAGES)} "
            f"stages ({', '.join(PIPELINE_STAGES)}) ended in success."
        )
    elif not rows:
        others = (
            " Events exist for other, overlapping intervals ("
            + ", ".join(f"{o['interval_start']}..{o['interval_end']}" for o in overlapping)
            + "); they belong to those intervals and are not counted here."
            if overlapping
            else ""
        )
        summary = (
            f"NO lineage for {s}..{e}: no event is recorded for exactly this interval, "
            f"so no stage is known to have run for it.{others}"
        )
    else:
        bits = []
        if missing:
            bits.append(f"NO events for {', '.join(missing)}")
        if failed:
            bits.append(f"latest event FAILED in {', '.join(failed)}")
        if unfinished:
            bits.append(f"no terminal event yet for {', '.join(unfinished)}")
        summary = (
            f"Lineage incomplete for exactly {s}..{e}: "
            + "; ".join(bits)
            + f". Stages seen: {', '.join(present) or 'none'}."
        )

    return CheckResult.verdict(
        ok,
        check=LINEAGE_CHECK,
        role=role.name,
        summary=summary,
        measured=measured,
        threshold=threshold,
        params=params,
    )
