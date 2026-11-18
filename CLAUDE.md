# nfl-medical: repo rules for Claude

Master rules (verification, branch + PR + CI, staging gate, reporting style): `~/.claude/CLAUDE.md`. Not copied here.

## What this is
Docker compose harness `nfl` (12 services) that simulates a reporting pipeline for contract testing. All data is
synthetic. It is not the production pipeline. Architecture: `docs/ARCHITECTURE.md`; runbook: `docs/RUNBOOK.md`.

## Commands (verified 2026-11-18)
No-stack checks, same as CI (`.github/workflows/ci.yml`):
- `python3 -m compileall -q docs infra/airflow/dags scripts shim stub watch`
- `python3 docs/check_coverage.py --check`
- `docker compose --project-directory infra -f infra/docker-compose.yml --env-file infra/env.example.txt config --quiet`
- `docker build -t nfl-dms-stub stub && docker run --rm nfl-dms-stub python selftest.py` (78 ok, "0 failure(s)")

Full stack (needs floci on `localhost:4566`, started outside this repo): `scripts/init-env.sh`, `scripts/up.sh`, then
`bru run` from `bruno/` per `docs/RUNBOOK.md` section 3. Not re-run in the 2026-11-18 sanitize pass (floci absent).

## Conventions
- Start/stop only via `scripts/*.sh`, never a bare `docker compose up` (skips env, bucket, grants, connectors).
- Env file is `infra/nfl.env` (gitignored, generated). Never print, commit or rename it; templates are `infra/env.example.txt` and `shim/env.example`.
- Compose project is `nfl`. Do not touch containers or volumes of other projects on the machine (`sso-*`, `pl-*`, `floci`, `local-cloudify-*`, ...). Never `docker volume prune` or `docker system prune`.
- Docs that cite counts (`docs/COVERAGE.md`) are recounted by `docs/check_coverage.py`; update the doc and run `--check` together.
- Python deps are pinned per service in `shim/`, `stub/`, `watch/` requirements.txt.

## Do not touch
- `design/` (gitignored: carries real client names; never commit it).
- `infra/secrets/`, `infra/keys/`: runtime-generated credentials.
- Seed "bad rows" BAD-1..7 in `infra/sql/04-seed.sql`: deliberate failure fixtures.
- Do not use real patient or medical data anywhere; the repo is public.

## Known gaps
- The Bruno end-to-end suite and chaos run are not in CI (need the full stack plus floci).
- `docs/*` carry run dates of 2026-10-04 to 2026-10-07; re-measure before quoting numbers.
