from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import yaml
from psycopg.conninfo import conninfo_to_dict

ROLES_DIR = Path(__file__).parent / "roles"

UNAUTHENTICATED_IDENTITY_KIND = "none"


# identity.kind values whose identity.name is the principal that authenticates,
# so the declared name can be compared with the one inside the credential.
_KIND_SYSTEM = {"postgres_role": "postgres", "mysql_user": "mysql"}


class RoleConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Role:
    name: str
    file: Path
    endpoint: str
    check: str
    summary: str
    identity: dict[str, Any]
    credential_env: str | None
    credential_env_companions: tuple[str, ...]
    dependencies: tuple[str, ...]
    grants: tuple[str, ...]
    grants_rationale: str
    aws_iam_policy: dict[str, Any]
    raw: dict[str, Any] = field(repr=False, default_factory=dict)

    @property
    def requires_credential(self) -> bool:
        return self.identity.get("kind") != UNAUTHENTICATED_IDENTITY_KIND

    @property
    def required_env(self) -> tuple[str, ...]:
        names = ([self.credential_env] if self.credential_env else []) + list(
            self.credential_env_companions
        )
        return tuple(names)

    @property
    def shared_with(self) -> tuple[str, ...]:
        return _as_tuple(self.raw.get("shared_with"))

    def principals(self) -> list[tuple[str, str, str]]:
        """(system, principal, env var) for every credential this role holds whose
        value names an authenticating principal. Raises when a DSN carries none."""
        out: list[tuple[str, str, str]] = []
        kind = self.identity.get("kind")
        for var in self.required_env:
            value = os.environ.get(var, "").strip()
            if not value:
                continue
            if var.endswith("_PG_DSN"):
                try:
                    user = conninfo_to_dict(value).get("user")
                except Exception as exc:
                    raise RoleConfigurationError(
                        f"${var} is not a parseable Postgres DSN ({type(exc).__name__})"
                    ) from None
                if not user:
                    raise RoleConfigurationError(f"${var} names no Postgres user")
                out.append(("postgres", str(user), var))
            elif var.endswith("_MYSQL_DSN"):
                user = urlsplit(value).username
                if not user:
                    raise RoleConfigurationError(f"${var} names no MySQL user")
                out.append(("mysql", unquote(user), var))
            elif var.endswith("_AWS_ACCESS_KEY_ID"):
                out.append(("s3", value, var))
            elif var.endswith("_CONNECT_URL"):
                user = urlsplit(value).username
                if user:
                    out.append(("kafka_connect", unquote(user), var))
            elif var.endswith("_SFTP_USER"):
                out.append(("sftp", value, var))
        complete = all(os.environ.get(v, "").strip() for v in self.required_env)
        if (
            complete
            and kind in _KIND_SYSTEM
            and not any(p[0] == _KIND_SYSTEM[kind] for p in out)
        ):
            raise RoleConfigurationError(
                f"role '{self.name}' declares identity.kind={kind} but none of its "
                f"credential variables ({', '.join(self.required_env)}) carries a "
                f"{_KIND_SYSTEM[kind]} principal"
            )
        return out

    def secret(self, var: str) -> str:
        if var not in self.required_env:
            raise RoleConfigurationError(
                f"Role '{self.name}' tried to read env var '{var}' which it does not "
                f"declare. Declared: {self.required_env or '(none)'}. Add it to "
                f"{self.file.name} before using it."
            )
        value = os.environ.get(var, "")
        if not value:
            raise RoleConfigurationError(
                f"Env var '{var}' for role '{self.name}' is empty at read time."
            )
        return value

    def public_view(self) -> dict[str, Any]:
        return {
            "role": self.name,
            "endpoint": self.endpoint,
            "check": self.check,
            "summary": self.summary,
            "identity": {
                "kind": self.identity.get("kind"),
                "name": self.identity.get("name"),
                "scope": self.identity.get("scope"),
            },
            "credential_env_vars": list(self.required_env),
            "dependencies": list(self.dependencies),
            "grants": list(self.grants),
            "grants_rationale": self.grants_rationale,
            "local_enforcement": str(self.raw.get("local_enforcement") or "").strip()
            or None,
            "shared_with": list(self.shared_with),
            "shared_rationale": str(self.raw.get("shared_rationale") or "").strip()
            or None,
            "aws_iam_policy": self.aws_iam_policy,
        }


def _as_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(v) for v in value)


def _load_one(path: Path) -> Role:
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise RoleConfigurationError(f"{path.name} is not valid YAML: {exc}") from exc

    if not isinstance(data, dict):
        raise RoleConfigurationError(f"{path.name} must contain a YAML mapping.")

    missing_keys = [
        k
        for k in ("endpoint", "check", "identity", "grants", "aws_iam_policy")
        if k not in data
    ]
    if missing_keys:
        raise RoleConfigurationError(
            f"{path.name} is missing required key(s): {', '.join(missing_keys)}"
        )

    identity = data["identity"]
    if not isinstance(identity, dict) or not identity.get("kind"):
        raise RoleConfigurationError(f"{path.name}: identity.kind is required.")

    grants = _as_tuple(data.get("grants"))
    rationale = str(data.get("grants_rationale") or "").strip()
    if not rationale:
        raise RoleConfigurationError(
            f"{path.name}: grants_rationale is required — a grant list with no stated "
            f"justification cannot be audited."
        )

    credential_env = data.get("credential_env")
    if credential_env is not None:
        credential_env = str(credential_env)

    if identity["kind"] != UNAUTHENTICATED_IDENTITY_KIND and not credential_env:
        raise RoleConfigurationError(
            f"{path.name}: identity.kind is '{identity['kind']}' but credential_env is "
            f"null. Only identity.kind='{UNAUTHENTICATED_IDENTITY_KIND}' may omit a "
            f"credential."
        )

    return Role(
        name=path.stem,
        file=path,
        endpoint=str(data["endpoint"]),
        check=str(data["check"]),
        summary=str(data.get("summary", "")).strip(),
        identity=identity,
        credential_env=credential_env,
        credential_env_companions=_as_tuple(data.get("credential_env_companions")),
        dependencies=_as_tuple(data.get("dependencies")),
        grants=grants,
        grants_rationale=rationale,
        aws_iam_policy=data["aws_iam_policy"] or {},
        raw=data,
    )


class RoleRegistry:
    def __init__(self, roles: dict[str, Role]) -> None:
        self._roles = roles

    @classmethod
    def load(cls, roles_dir: Path = ROLES_DIR) -> RoleRegistry:
        if not roles_dir.is_dir():
            raise RoleConfigurationError(f"Roles directory not found: {roles_dir}")
        files = sorted(p for p in roles_dir.glob("*.yaml"))
        if not files:
            raise RoleConfigurationError(f"No role declarations found in {roles_dir}")
        roles = {}
        for path in files:
            role = _load_one(path)
            roles[role.name] = role
        return cls(roles)

    def __getitem__(self, name: str) -> Role:
        try:
            return self._roles[name]
        except KeyError:
            raise RoleConfigurationError(
                f"No role declaration named '{name}'. Expected "
                f"{ROLES_DIR / (name + '.yaml')} to exist. An endpoint without a role "
                f"file cannot be registered."
            ) from None

    def all(self) -> list[Role]:
        return sorted(self._roles.values(), key=lambda r: r.endpoint)

    def dependency_names(self) -> list[str]:
        seen: set[str] = set()
        for role in self._roles.values():
            seen.update(role.dependencies)
        return sorted(seen)

    def validate_environment(self, required_roles: set[str]) -> None:
        problems: list[str] = []

        for name in sorted(required_roles):
            if name not in self._roles:
                problems.append(
                    f"  - endpoint registered with role '{name}' but "
                    f"shim/roles/{name}.yaml does not exist"
                )

        for name in sorted(required_roles & self._roles.keys()):
            role = self._roles[name]
            for var in role.required_env:
                if not os.environ.get(var, "").strip():
                    problems.append(
                        f"  - {role.endpoint}  needs ${var}  "
                        f"(identity: {role.identity.get('name')})"
                    )

        owners: dict[str, list[str]] = {}
        for name in sorted(required_roles & self._roles.keys()):
            role = self._roles[name]
            if role.credential_env:
                owners.setdefault(role.credential_env, []).append(role.name)
        for var, holders in sorted(owners.items()):
            if len(holders) > 1:
                problems.append(
                    f"  - ${var} is claimed by multiple roles ({', '.join(holders)}). "
                    f"Each endpoint reads its own credential variable."
                )

        # Distinct variable NAMES prove nothing: two variables can carry the same
        # login. Compare the principal inside each credential instead.
        by_principal: dict[tuple[str, str], list[str]] = {}
        for name in sorted(required_roles & self._roles.keys()):
            role = self._roles[name]
            if role.shared_with and not str(role.raw.get("shared_rationale") or "").strip():
                problems.append(
                    f"  - {role.file.name} declares shared_with but no shared_rationale"
                )
            try:
                principals = role.principals()
            except RoleConfigurationError as exc:
                problems.append(f"  - {role.endpoint}  {exc}")
                continue
            for system, principal, var in principals:
                by_principal.setdefault((system, principal), []).append(role.name)
                declared = role.identity.get("name")
                if _KIND_SYSTEM.get(role.identity.get("kind")) == system and (
                    principal != declared
                ):
                    problems.append(
                        f"  - {role.endpoint}  ${var} authenticates as {system} "
                        f"principal '{principal}' but {role.file.name} declares "
                        f"identity.name '{declared}'. The declaration must describe "
                        f"the credential actually supplied."
                    )
        for (system, principal), holders in sorted(by_principal.items()):
            holders = sorted(set(holders))
            if len(holders) < 2:
                continue
            undeclared = [
                h
                for h in holders
                if not set(self._roles[h].shared_with) >= set(holders) - {h}
            ]
            if undeclared:
                shown = principal if system != "s3" else principal[:4] + "..."
                problems.append(
                    f"  - {system} principal '{shown}' is used by "
                    f"{len(holders)} endpoints ({', '.join(holders)}) but "
                    f"{', '.join(undeclared)} do(es) not declare the others under "
                    f"'shared_with'. One principal per endpoint, or an explicit, "
                    f"reasoned sharing declaration."
                )

        if problems:
            raise RoleConfigurationError(
                "Refusing to start: least-privilege credentials are not fully "
                "configured.\n\n"
                + "\n".join(problems)
                + "\n\nEvery check runs under the principal its role file declares. Two "
                "endpoints may resolve to one principal only where both role files say "
                "so under shared_with. The shim will not fall back to a broader "
                "credential, because a check that cannot run must never look like a "
                "check that passed.\n"
                "The compose service `shim` sets these from infra/nfl.env; "
                "shim/env.example documents each one."
            )
