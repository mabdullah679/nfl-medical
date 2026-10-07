from __future__ import annotations

from . import (
    delivery,
    events,
    extraction,
    ingestion,
    lineage,
    observability,
    replication,
    reporting,
)

ROUTERS = [
    replication.router,
    ingestion.router,
    extraction.router,
    reporting.router,
    delivery.router,
    lineage.router,
    events.router,
    observability.router,
]

REQUIRED_ROLES: set[str] = set()
for _module in (
    replication, ingestion, extraction, reporting, delivery, lineage, events, observability,
):
    REQUIRED_ROLES |= _module.ROLES

__all__ = ["ROUTERS", "REQUIRED_ROLES"]
