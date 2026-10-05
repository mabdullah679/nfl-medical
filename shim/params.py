from __future__ import annotations

import re
from datetime import date, datetime

from fastapi import HTTPException, Query
from psycopg import sql

INTERVAL_RE = re.compile(r"^\d{8}$|^\d{4}-\d{2}-\d{2}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

IntervalStart = Query(
    ...,
    description="Interval start as YYYYMMDD or YYYY-MM-DD.",
    examples=["20260401"],
)
IntervalEnd = Query(
    ...,
    description="Interval end as YYYYMMDD or YYYY-MM-DD.",
    examples=["20260430"],
)


def normalise_interval(value: str, field: str) -> str:
    raw = value.strip()
    if not INTERVAL_RE.match(raw):
        raise HTTPException(
            status_code=422,
            detail={
                "ok": False,
                "state": "unverifiable",
                "reason": (
                    f"{field}={value!r} is not a valid interval bound; expected "
                    f"YYYYMMDD or YYYY-MM-DD"
                ),
            },
        )
    compact = raw.replace("-", "")
    try:
        datetime.strptime(compact, "%Y%m%d")
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail={
                "ok": False,
                "state": "unverifiable",
                "reason": f"{field}={value!r} is not a real calendar date",
            },
        ) from None
    return compact


def interval_bounds(start: str, end: str) -> tuple[str, str, date, date]:
    s = normalise_interval(start, "interval_start")
    e = normalise_interval(end, "interval_end")
    s_date = datetime.strptime(s, "%Y%m%d").date()
    e_date = datetime.strptime(e, "%Y%m%d").date()
    if e_date < s_date:
        raise HTTPException(
            status_code=422,
            detail={
                "ok": False,
                "state": "unverifiable",
                "reason": f"interval_end {e} precedes interval_start {s}",
            },
        )
    return s, e, s_date, e_date


def qualified_table(dotted: str) -> sql.Identifier:
    parts = dotted.split(".")
    if len(parts) not in (1, 2) or not all(IDENTIFIER_RE.match(p) for p in parts):
        raise HTTPException(
            status_code=500,
            detail={
                "ok": False,
                "state": "unverifiable",
                "reason": (
                    f"configured table name {dotted!r} is not a valid "
                    f"[schema.]table identifier"
                ),
            },
        )
    return sql.Identifier(*parts)


def column(name: str) -> sql.Identifier:
    if not IDENTIFIER_RE.match(name):
        raise HTTPException(
            status_code=500,
            detail={
                "ok": False,
                "state": "unverifiable",
                "reason": f"configured column name {name!r} is not a valid identifier",
            },
        )
    return sql.Identifier(name)
