"""Provenance-tagged values.

The stub's one hard rule is that no number reaches a response without declaring
where it came from. Wrapping values at the point of computation (rather than
annotating them at serialisation time) makes an un-tagged number a type error
instead of a silent fabrication.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

Provenance = Literal["measured", "derived", "static", "unknown"]

PROVENANCE_MEANING: dict[str, str] = {
    "measured": "Read directly from a live system (Kafka Connect on 8097, Postgres on 5440).",
    "derived": "Computed from one or more measured values. The exact derivation is stated in _provenance_detail.",
    "static": "A fixed configured constant (ARNs, identifiers, engine names). Never a status, progress or volume value.",
    "unknown": "Could not be established. The source was unreachable or does not expose this concept.",
}


@dataclass(frozen=True)
class Value:
    """A single field value plus the story of where it came from."""

    value: Any
    provenance: Provenance
    detail: str

    @property
    def is_unknown(self) -> bool:
        return self.provenance == "unknown"


def measured(value: Any, detail: str) -> Value:
    return Value(value=value, provenance="measured", detail=detail)


def derived(value: Any, detail: str) -> Value:
    return Value(value=value, provenance="derived", detail=detail)


def static(value: Any, detail: str) -> Value:
    return Value(value=value, provenance="static", detail=detail)


def unknown(detail: str) -> Value:
    """An honest null.

    Used wherever real DMS would return a number this stub has no way to
    establish. Returning None here is the entire point: a plausible fake would
    be indistinguishable from a real reading downstream.
    """
    return Value(value=None, provenance="unknown", detail=detail)


def render(obj: Any) -> Any:
    """Collapse Values to their payload, hoisting provenance into sidecar maps.

    A field `X` wrapped in a Value emits `X` plus an entry in the object's
    `_provenance` / `_provenance_detail` maps, so a consumer reading only the
    AWS-shaped fields sees ordinary DMS JSON while an auditor sees the sourcing
    of every single number.
    """
    if isinstance(obj, Value):
        return render(obj.value)

    if isinstance(obj, dict):
        plain: dict[str, Any] = {}
        prov: dict[str, str] = {}
        detail: dict[str, str] = {}
        for key, item in obj.items():
            if isinstance(item, Value):
                plain[key] = render(item.value)
                prov[key] = item.provenance
                detail[key] = item.detail
            else:
                plain[key] = render(item)
        if prov:
            plain["_provenance"] = prov
            plain["_provenance_detail"] = detail
        return plain

    if isinstance(obj, list):
        return [render(item) for item in obj]

    return obj
