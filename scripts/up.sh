#!/usr/bin/env bash
# Bring the NFL local stack up and wait until every service is genuinely
# healthy. Exits non-zero if anything fails to become healthy - a timeout must
# never look like a successful start.
#
# From nothing:
#   scripts/init-env.sh     once: generates infra/nfl.env and the SFTP keys
#   scripts/up.sh           every time: idempotent
#
# Nothing is edited by hand between those two commands. floci (the local AWS
# emulator) must already answer on :4566; this script does not start it, and
# reports a failure - not a warning - when it is unreachable.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="$REPO_ROOT/infra"
ENV_FILE="$INFRA/nfl.env"
COMPOSE=("docker" "compose" "--project-directory" "$INFRA" "-f" "$INFRA/docker-compose.yml" "--env-file" "$ENV_FILE")

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[33m[warn]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[fail]\033[0m %s\n' "$*" >&2; exit 1; }

FAILED=()

# ---------------------------------------------------------------------------
say "Preflight"

[ -f "$ENV_FILE" ] || die "missing $ENV_FILE - generate it first:
    $REPO_ROOT/scripts/init-env.sh"

if grep -v '^[[:space:]]*#' "$ENV_FILE" | grep -q 'CHANGEME'; then
  warn "$ENV_FILE still contains CHANGEME placeholders in:"
  grep -v '^[[:space:]]*#' "$ENV_FILE" | grep 'CHANGEME' | cut -d= -f1 | sed 's/^/    /'
  die "regenerate it with scripts/init-env.sh --force (placeholders are not passwords)"
fi

command -v docker >/dev/null || die "docker not on PATH"
docker compose version >/dev/null 2>&1 || die "docker compose v2+ required"
command -v python3 >/dev/null || die "python3 not on PATH"
command -v curl >/dev/null || die "curl not on PATH"
command -v lsof >/dev/null || die "lsof not on PATH (the port guard needs it to see which ports are taken)"

# The Fernet key must be 32 url-safe base64 bytes or Airflow crashloops with
# "ValueError: Fernet key must be 32 url-safe base64-encoded bytes" - a failure
# that otherwise shows up only as a container restarting forever.
# shellcheck disable=SC1090
( set -a; . "$ENV_FILE"; set +a
  python3 -c '
import base64, sys
key = sys.argv[1]
try:
    if len(base64.urlsafe_b64decode(key.encode())) != 32:
        raise ValueError("decoded length is not 32 bytes")
except Exception as e:
    sys.stderr.write("AIRFLOW_FERNET_KEY is not a valid Fernet key (%s).\n" % e)
    sys.exit(1)
' "${AIRFLOW_FERNET_KEY:-}" || exit 1
) || die "AIRFLOW_FERNET_KEY in $ENV_FILE is invalid - regenerate with scripts/init-env.sh --force"

# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a

# Variables added after the first env files were generated. Named here so an
# older infra/nfl.env fails with the missing NAME instead of a compose error.
MISSING_VARS=()
for v in MYSQL_LAG_CHECK_PASSWORD PG_STUB_PASSWORD PG_EVENTS_PASSWORD \
         PG_CHECK_RECONCILE_PASSWORD PG_CHECK_QUALITY_PASSWORD PG_CHECK_FRESHNESS_PASSWORD \
         PG_CHECK_LINEAGE_PASSWORD PG_CHECK_EVENTS_PASSWORD; do
  [ -n "${!v:-}" ] || MISSING_VARS+=("$v")
done
[ "${#MISSING_VARS[@]}" -eq 0 ] || die "$ENV_FILE is missing: ${MISSING_VARS[*]}
    compare with infra/env.example.txt, or regenerate:
      scripts/reset.sh -y && scripts/init-env.sh --force"

# Guard the ports we claim. A port taken by another project on this machine
# must stop us loudly rather than producing a half-started stack.
OUR_PUBLISHED="$("${COMPOSE[@]}" ps --format '{{.Ports}}' 2>/dev/null || true)"
check_port() {
  local port="$1" label="$2"
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    if printf '%s' "$OUR_PUBLISHED" | grep -q ":$port->"; then
      echo "    port $port ($label) already held by this stack - reused"
    else
      die "port $port ($label) is in use by something else. Our stack must not reuse another project's port."
    fi
  fi
}
check_port "${NFL_MYSQL_PORT:-3307}"    mysql
check_port "${NFL_POSTGRES_PORT:-5440}" postgres
check_port "${NFL_KAFKA_PORT:-9099}"    kafka
check_port "${NFL_CONNECT_PORT:-8097}"  kafka-connect
check_port "${NFL_AIRFLOW_PORT:-8095}"  airflow
check_port "${NFL_SFTP_PORT:-2225}"     sftp
check_port "${NFL_SHIM_PORT:-8096}"     shim
check_port "${NFL_DMS_STUB_PORT:-8098}" dms-stub

# floci is externally managed: never started here. Without it extraction,
# reporting and five shim checks cannot work, so its absence is a FAILURE of
# this bring-up, recorded now and reported at the end.
S3_ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
S3_BUCKET="${NFL_S3_BUCKET:-nfl-data-platform}"
FLOCI_UP=no
if curl -fsS --max-time 5 "$S3_ENDPOINT/_localstack/health" >/dev/null 2>&1; then
  FLOCI_UP=yes
  echo "    floci (AWS emulator) reachable at $S3_ENDPOINT"
else
  warn "floci at $S3_ENDPOINT is NOT reachable. This script does not start it. The rest of the stack will be brought up, but this run will end as FAILED."
  FAILED+=("floci:unreachable")
fi

# ---------------------------------------------------------------------------
say "Bootstrapping the S3 bucket"
if [ "$FLOCI_UP" = yes ]; then
  python3 "$REPO_ROOT/scripts/s3_bucket.py" ensure "$S3_ENDPOINT" "$S3_BUCKET" \
    || die "could not ensure bucket $S3_BUCKET on $S3_ENDPOINT"
else
  echo "    SKIPPED - floci unreachable; bucket $S3_BUCKET was neither checked nor created"
fi

# ---------------------------------------------------------------------------
say "Rendering Connect secrets"
# Connector JSON reads passwords through FileConfigProvider so no credential
# lives in the committed connector configs.
mkdir -p "$INFRA/secrets"
SECRETS="$INFRA/secrets/connect-secrets.properties"
( umask 077
  cat > "$SECRETS" <<EOF
mysql_cdc_password=${MYSQL_CDC_PASSWORD:?MYSQL_CDC_PASSWORD missing from nfl.env}
pg_repl_password=${PG_REPL_PASSWORD:?PG_REPL_PASSWORD missing from nfl.env}
EOF
)
chmod 600 "$SECRETS"
echo "    wrote $SECRETS (0600, gitignored)"

mkdir -p "$INFRA/airflow/dags"

# The sftp entrypoint skips /etc/sftp.d scripts that are not executable, and the
# mount is read-only, so the bit has to be right on the host.
chmod +x "$INFRA/sftp/10-outbound-owner.sh"

# The SFTP host keys and the checker key pair are bind-mounted as FILES. If one
# were missing Docker would silently create a directory in its place, so they
# are ensured (idempotently) before compose runs.
"$REPO_ROOT/scripts/init-env.sh" --keys-only
for f in ssh_host_ed25519_key ssh_host_rsa_key check_ed25519 check_ed25519.pub known_hosts; do
  [ -f "$INFRA/keys/sftp/$f" ] || die "missing $INFRA/keys/sftp/$f after key generation"
done

# ---------------------------------------------------------------------------
say "Starting stack (project: nfl)"
"${COMPOSE[@]}" up -d --build

# ---------------------------------------------------------------------------
say "Waiting for health"

wait_healthy() {
  local svc="$1" timeout="${2:-180}" waited=0 cid state
  printf '    %-16s' "$svc"
  while [ "$waited" -lt "$timeout" ]; do
    cid="$("${COMPOSE[@]}" ps -a -q "$svc" 2>/dev/null || true)"
    if [ -z "$cid" ]; then printf ' no container\n'; return 1; fi

    state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || echo unknown)"
    case "$state" in
      healthy)  printf ' healthy (%ss)\n' "$waited"; return 0 ;;
      running)  printf ' running, no healthcheck (%ss)\n' "$waited"; return 0 ;;
      exited|dead)
        printf ' %s\n' "$state"
        echo "      last 30 log lines:"
        docker logs --tail 30 "$cid" 2>&1 | sed 's/^/        /'
        return 1 ;;
    esac
    sleep 3; waited=$((waited + 3)); printf '.'
  done
  printf ' TIMEOUT after %ss (state=%s)\n' "$timeout" "${state:-unknown}"
  echo "      last 30 log lines:"
  docker logs --tail 30 "$cid" 2>&1 | sed 's/^/        /'
  return 1
}

wait_healthy mysql         240 || FAILED+=(mysql)
wait_healthy postgres      120 || FAILED+=(postgres)
wait_healthy kafka         180 || FAILED+=(kafka)
wait_healthy kafka-connect 240 || FAILED+=(kafka-connect)
wait_healthy airflow-db    120 || FAILED+=(airflow-db)
wait_healthy airflow       300 || FAILED+=(airflow)
wait_healthy sftp           90 || FAILED+=(sftp)
wait_healthy shim          120 || FAILED+=(shim)
# dms-stub is waited for further down: its health is derived from the connector
# and from its Postgres role, neither of which exists yet on a cold start.

UNHEALTHY=()
for f in ${FAILED[@]+"${FAILED[@]}"}; do
  [ "$f" = "floci:unreachable" ] || UNHEALTHY+=("$f")
done
if [ "${#UNHEALTHY[@]}" -gt 0 ]; then
  die "these services did not become healthy: ${UNHEALTHY[*]}"
fi

# ---------------------------------------------------------------------------
say "Converging schemas, roles and grants"
# Applied on EVERY start rather than from the database entrypoints, because
# entrypoint init scripts only run against an empty volume. Everything here is
# idempotent. Templates are rendered in memory and piped over stdin: no
# rendered SQL file exists on disk and no password appears in a process list.

render_sql() {
  local tmpl="$1"; shift
  python3 -c '
import os, sys

text = open(sys.argv[1]).read()
for pair in sys.argv[2:]:
    placeholder, env_name = pair.split("=", 1)
    value = os.environ.get(env_name, "")
    if not value:
        sys.exit(env_name + " is unset - refusing to render a placeholder password")
    if chr(39) in value or chr(92) in value:
        sys.exit(env_name + " contains a quote or backslash; not supported")
    text = text.replace(placeholder, value)
live = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--"))
if "CHANGEME" in live:
    sys.exit("placeholders remain after rendering " + sys.argv[1])
sys.stdout.write(text)
' "$tmpl" "$@"
}

pg_apply() {
  "${COMPOSE[@]}" exec -T -e PGOPTIONS='-c client_min_messages=warning' postgres \
    psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d nfl -f - >/dev/null
}

for f in 01-staging-schema.sql 02-utm-schema.sql 05-observability.sql; do
  pg_apply < "$INFRA/sql/$f" || die "could not apply infra/sql/$f"
done
echo "    schemas staging, utm, observability converged"

# Same script and template the postgres entrypoint runs on an empty volume.
"${COMPOSE[@]}" exec -T postgres bash /docker-entrypoint-initdb.d/03-render-roles.sh >/dev/null \
  || die "could not apply infra/sql/03-roles.sql.tmpl"
echo "    postgres roles nfl_replication, nfl_hop, nfl_spark converged"

export PG_STUB_PASSWORD PG_EVENTS_PASSWORD MYSQL_LAG_CHECK_PASSWORD \
       PG_CHECK_RECONCILE_PASSWORD PG_CHECK_QUALITY_PASSWORD PG_CHECK_FRESHNESS_PASSWORD \
       PG_CHECK_LINEAGE_PASSWORD PG_CHECK_EVENTS_PASSWORD
render_sql "$INFRA/sql/06-check-identities.sql.tmpl" \
    CHANGEME_pg_check_reconcile=PG_CHECK_RECONCILE_PASSWORD \
    CHANGEME_pg_check_quality=PG_CHECK_QUALITY_PASSWORD \
    CHANGEME_pg_check_freshness=PG_CHECK_FRESHNESS_PASSWORD \
    CHANGEME_pg_check_lineage=PG_CHECK_LINEAGE_PASSWORD \
    CHANGEME_pg_check_events=PG_CHECK_EVENTS_PASSWORD \
    CHANGEME_pg_stub=PG_STUB_PASSWORD CHANGEME_pg_events=PG_EVENTS_PASSWORD \
  | pg_apply || die "could not apply infra/sql/06-check-identities.sql.tmpl"
echo "    postgres roles nfl_stub_reader, nfl_events and the five nfl_check_* roles converged"

render_sql "$INFRA/mysql/init/03-lag-check-user.sql.tmpl" \
    CHANGEME_mysql_lag_check=MYSQL_LAG_CHECK_PASSWORD \
  | "${COMPOSE[@]}" exec -T mysql \
      sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --protocol=socket -uroot' \
  || die "could not apply infra/mysql/init/03-lag-check-user.sql.tmpl"
echo "    mysql user nfl_check_replication_lag converged (REPLICATION CLIENT only)"

"${COMPOSE[@]}" exec -T mysql \
    sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --protocol=socket -uroot' \
    < "$INFRA/mysql/init/04-replication-heartbeat.sql" \
  || die "could not apply infra/mysql/init/04-replication-heartbeat.sql"
echo "    mysql hawks.replication_heartbeat present; debezium may INSERT/UPDATE that one table"

# ---------------------------------------------------------------------------
say "Registering Debezium connectors"

CONNECT="http://localhost:${NFL_CONNECT_PORT:-8097}"

register() {
  local file="$1" name
  name="$(sed -n 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$file" | head -1)"
  [ -n "$name" ] || die "could not read connector name from $file"

  if curl -fsS "$CONNECT/connectors/$name" >/dev/null 2>&1; then
    echo "    $name already registered - updating config"
    # PUT /config takes the inner config object, not the whole envelope.
    python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$file" \
      | curl -fsS -X PUT -H 'Content-Type: application/json' \
          --data @- "$CONNECT/connectors/$name/config" >/dev/null \
      || die "failed to update $name"
  else
    curl -fsS -X POST -H 'Content-Type: application/json' \
      --data @"$file" "$CONNECT/connectors" >/dev/null \
      || die "failed to register $name (see: curl $CONNECT/connectors/$name/status)"
    echo "    $name registered"
  fi
}

register "$INFRA/debezium/mysql-source.json"
register "$INFRA/debezium/jdbc-sink.json"

echo "    waiting for connector tasks to report RUNNING"
for name in nfl-hawks-mysql-source nfl-staging-jdbc-sink; do
  ok=no
  for _ in $(seq 1 30); do
    st="$(curl -fsS "$CONNECT/connectors/$name/status" 2>/dev/null || echo '')"
    case "$st" in
      *'"state":"FAILED"'*) : ;;
      *'"tasks":[{'*'"state":"RUNNING"'*) ok=yes; break ;;
    esac
    sleep 3
  done
  if [ "$ok" = yes ]; then
    echo "      $name RUNNING"
  else
    warn "$name is NOT fully RUNNING. Inspect:"
    echo "        curl -s $CONNECT/connectors/$name/status"
    FAILED+=("connector:$name")
  fi
done

# The stub's health is derived from the source connector and from a query as
# nfl_stub_reader; both exist only now.
wait_healthy dms-stub 120 || FAILED+=(dms-stub)

# ---------------------------------------------------------------------------
say "Proving the shim and stub reach their backing stores"
# A healthy container only proves the process answers. These calls prove each
# check identity authenticates: HTTP 200 means the check RAN (whatever its
# verdict); anything else means it could not run, which is a broken stack.
#
# Two checks cannot run in the first seconds of a cold start and are polled
# rather than tried once: Kafka Connect commits the source offset on a 60s
# timer, and the first replication heartbeat lands one heartbeat interval after
# streaming begins. Polling waits for the check to become able to RUN; it does
# not relax what the check then decides.

SHIM="http://localhost:${NFL_SHIM_PORT:-8096}"
STUB="http://localhost:${NFL_DMS_STUB_PORT:-8098}"

probe_ran() {
  local label="$1" url="$2" patience="${3:-0}" waited=0 code
  while :; do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 "$url" || true)"
    [ "$code" = 200 ] && break
    [ "$waited" -ge "$patience" ] && break
    sleep 5; waited=$((waited + 5))
  done
  if [ "$code" = 200 ]; then
    printf '    %-34s ran (HTTP 200, after %ss)\n' "$label" "$waited"
  else
    printf '    %-34s COULD NOT RUN (HTTP %s after %ss)\n' "$label" "$code" "$waited"
    curl -s --max-time 30 "$url" | head -c 600 | sed 's/^/        /'; echo
    FAILED+=("shim:$label")
  fi
}

TODAY="$(date +%Y%m%d)"
INTERVAL="interval_start=$TODAY&interval_end=$TODAY"
probe_ran "source capture lag (mysql)"   "$SHIM/checks/replication/lag" 120
probe_ran "kafka connect status"         "$SHIM/checks/replication/connector-status"
probe_ran "freshness role + heartbeat"   "$SHIM/checks/ingestion/freshness" 120
probe_ran "reconcile role"               "$SHIM/checks/ingestion/reconcile?$INTERVAL"
probe_ran "quality role"                 "$SHIM/checks/ingestion/quality?$INTERVAL"
probe_ran "lineage role"                 "$SHIM/checks/lineage?$INTERVAL"
probe_ran "events role"                  "$SHIM/checks/events?$INTERVAL"
probe_ran "sftp key + pinned host key"   "$SHIM/checks/delivery/sftp?$INTERVAL&report=up-probe.csv"
if [ "$FLOCI_UP" = yes ]; then
  # floci enforces no IAM: this proves the bucket is reachable, not that an
  # identity was authenticated or authorised.
  probe_ran "s3 reachability only (floci)" "$SHIM/checks/extraction/s3-marker?$INTERVAL"
  probe_ran "shim /health (all dependencies)" "$SHIM/health"
else
  printf '    %-34s NOT RUN - floci unreachable (already recorded as a failure)\n' "s3 reachability, shim /health"
fi

stub_health="$(curl -sS --max-time 30 "$STUB/health" 2>/dev/null || echo '')"
if printf '%s' "$stub_health" | grep -q '"degraded":false' \
   && printf '%s' "$stub_health" | grep -q '"_stub":true'; then
  printf '    %-34s reachable (Kafka Connect + Postgres as nfl_stub_reader)\n' "dms-stub upstreams"
else
  printf '    %-34s DEGRADED or unreachable:\n' "dms-stub"
  printf '%s\n' "$stub_health" | head -c 600 | sed 's/^/        /'; echo
  FAILED+=("dms-stub:upstreams")
fi

# ---------------------------------------------------------------------------
say "Stack endpoints"
cat <<EOF
    All ports are bound to 127.0.0.1 only.
    Airflow UI/API   http://localhost:${NFL_AIRFLOW_PORT:-8095}   (basic auth, user ${AIRFLOW_ADMIN_USER})
    Check shim       $SHIM   index: $SHIM/checks   OpenAPI: $SHIM/docs
    DMS stub         $STUB   POST / with X-Amz-Target: AmazonDMSv20160101.<Operation>
    Kafka Connect    http://localhost:${NFL_CONNECT_PORT:-8097}
    Postgres         localhost:${NFL_POSTGRES_PORT:-5440}  db=nfl
    MySQL            localhost:${NFL_MYSQL_PORT:-3307}  db=${MYSQL_DATABASE:-hawks}
    Kafka            localhost:${NFL_KAFKA_PORT:-9099}
    SFTP             localhost:${NFL_SFTP_PORT:-2225}  upload user=${SFTP_USER}  (host keys pinned: infra/keys/sftp/known_hosts)
    floci (external) $S3_ENDPOINT  bucket=$S3_BUCKET

    Credentials live in infra/nfl.env (gitignored) and are never printed.
EOF

if [ "${#FAILED[@]}" -gt 0 ]; then
  die "stack is up but these need attention: ${FAILED[*]}"
fi

say "Stack is up: every service healthy, both connectors RUNNING, every Postgres/MySQL/SFTP check identity authenticated, S3 reachable"
