from __future__ import annotations

from typing import Callable

from .roles import Role, RoleRegistry

_registry: RoleRegistry | None = None


def set_registry(registry: RoleRegistry) -> None:
    global _registry
    _registry = registry


def get_registry() -> RoleRegistry:
    if _registry is None:
        raise RuntimeError(
            "Role registry was never installed. The app must call set_registry() during "
            "startup before any endpoint can resolve its identity."
        )
    return _registry


def role_dep(name: str) -> Callable[[], Role]:
    def resolve() -> Role:
        return get_registry()[name]

    resolve.__name__ = f"role_{name}"
    return resolve
