#!/usr/bin/env bash
# Runs inside the postgres container: by the entrypoint on an empty volume, and
# through `docker compose exec` from scripts/up.sh on every start so an existing
# volume converges to the current 03-roles.sql.tmpl.
#
# 03-roles.sql.tmpl is committed with CHANGEME_pg_* placeholders so no real
# credential lands in git. This renders it to a container-local temp file and
# applies that with psql.
#
# It deliberately does NOT write back through the bind mount: doing so would
# modify the committed file on the HOST and leak a real password into the repo
# (verified: writing to a bind-mounted file changes the host file).
#
# Postgres runs /docker-entrypoint-initdb.d entries in lexical order. This is
# numbered "03-" so it runs AFTER the 01-/02- schema files: the role GRANTs
# reference tables in staging and utm, so those must exist first. The template
# is mounted as 03-roles.sql.tmpl (not .sql) so the entrypoint never executes
# the placeholder version itself.
#
# POSIX shell + awk only: postgres:16-alpine has NO python3 (verified by running
# `command -v python3` in the image), and sed would misinterpret / & | in a
# password.
set -euo pipefail

TMPL=/docker-entrypoint-initdb.d/03-roles.sql.tmpl
RENDERED=/tmp/03-roles.rendered.sql

echo "[render-roles] rendering Postgres role DDL from environment"
[ -f "$TMPL" ] || { echo "[render-roles] FATAL: missing $TMPL" >&2; exit 1; }

umask 077
trap 'rm -f "$RENDERED" "$RENDERED.next"' EXIT
cp "$TMPL" "$RENDERED"

render_one() {
  placeholder="$1"; value="$2"; label="$3"
  if [ -z "${value:-}" ]; then
    echo "[render-roles] FATAL: $label is unset/empty - refusing to create a role with a placeholder password" >&2
    exit 1
  fi
  case "$value" in
    *"'"*) echo "[render-roles] FATAL: $label contains a single quote; not supported" >&2; exit 1 ;;
  esac
  awk -v ph="$placeholder" -v val="$value" '
    {
      line = $0; out = ""
      while ((p = index(line, ph)) > 0) {
        out  = out substr(line, 1, p - 1) val
        line = substr(line, p + length(ph))
      }
      print out line
    }
  ' "$RENDERED" > "$RENDERED.next"
  mv "$RENDERED.next" "$RENDERED"
  echo "[render-roles]   $label -> set (${#value} chars)"
}

render_one CHANGEME_pg_repl  "${PG_REPL_PASSWORD:-}"  PG_REPL_PASSWORD
render_one CHANGEME_pg_hop   "${PG_HOP_PASSWORD:-}"   PG_HOP_PASSWORD
render_one CHANGEME_pg_spark "${PG_SPARK_PASSWORD:-}" PG_SPARK_PASSWORD

if grep -q 'CHANGEME_pg_' "$RENDERED"; then
  echo "[render-roles] FATAL: placeholders still present after render" >&2
  exit 1
fi

echo "[render-roles] applying rendered role DDL"
PGOPTIONS='-c client_min_messages=warning' \
  psql -q -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$RENDERED"
echo "[render-roles] done - roles converged, no credential written to the repo"
