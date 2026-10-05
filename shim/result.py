from __future__ import annotations

from enum import StrEnum
from typing import Any, Self

from fastapi import HTTPException
from pydantic import BaseModel, Field


class CheckState(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    UNVERIFIABLE = "unverifiable"


class CheckResult(BaseModel):
    ok: bool
    state: CheckState
    check: str
    role: str
    measured: dict[str, Any] = Field(default_factory=dict)
    threshold: dict[str, Any] = Field(default_factory=dict)
    summary: str
    params: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def passed(
        cls,
        *,
        check: str,
        role: str,
        summary: str,
        measured: dict[str, Any] | None = None,
        threshold: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            ok=True,
            state=CheckState.PASS,
            check=check,
            role=role,
            measured=measured or {},
            threshold=threshold or {},
            summary=summary,
            params=params or {},
        )

    @classmethod
    def failed(
        cls,
        *,
        check: str,
        role: str,
        summary: str,
        measured: dict[str, Any] | None = None,
        threshold: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            ok=False,
            state=CheckState.FAIL,
            check=check,
            role=role,
            measured=measured or {},
            threshold=threshold or {},
            summary=summary,
            params=params or {},
        )

    @classmethod
    def verdict(
        cls,
        passing: bool,
        *,
        check: str,
        role: str,
        summary: str,
        measured: dict[str, Any] | None = None,
        threshold: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Self:
        factory = cls.passed if passing else cls.failed
        return factory(
            check=check,
            role=role,
            summary=summary,
            measured=measured,
            threshold=threshold,
            params=params,
        )


class CheckUnverifiable(HTTPException):
    """A check that could not run. Never a 200, so silence cannot read as success."""

    def __init__(
        self,
        *,
        check: str,
        role: str,
        reason: str,
        dependency: str,
        detail_extra: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "ok": False,
            "state": CheckState.UNVERIFIABLE.value,
            "check": check,
            "role": role,
            "dependency": dependency,
            "reason": reason,
            "summary": f"Check '{check}' could NOT be evaluated: {reason}",
        }
        if detail_extra:
            payload["measured"] = detail_extra
        super().__init__(status_code=503, detail=payload)


class NotImplementedCheck(HTTPException):
    """A check whose real implementation is unavailable here. Explicitly not a pass."""

    def __init__(self, *, check: str, role: str, reason: str) -> None:
        super().__init__(
            status_code=501,
            detail={
                "ok": False,
                "state": CheckState.UNVERIFIABLE.value,
                "check": check,
                "role": role,
                "reason": reason,
                "summary": (
                    f"Check '{check}' is NOT IMPLEMENTED and therefore NOT verified: {reason}"
                ),
            },
        )
