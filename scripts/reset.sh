#!/usr/bin/env bash
# Destroy the NFL stack INCLUDING its data volumes, then optionally rebuild.
#
# This wipes MySQL source data, the staging/utm database, Kafka's log (so CDC
# offsets and schema history are gone too), Airflow's metadata, and the SFTP
# upload dir, plus the pipeline's own objects in the nfl S3 bucket. The next
# up.sh is a true cold start: schema + seed re-run, Debezium takes a fresh
# snapshot, and no earlier run's S3 marker, parquet or report survives to make
# a check pass for work the new stack never did.
#
# Scoped to the "nfl" compose project: volumes are removed only through that
# project's compose file or by its project label - never by name pattern and
# never with a prune. In floci only the pipeline prefixes of the one nfl bucket
# are deleted (scripts/s3_bucket.py); floci itself, its other buckets and every
# other project on this machine are not touched.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="$REPO_ROOT/infra"
ENV_FILE="$INFRA/nfl.env"
[ -f "$ENV_FILE" ] || ENV_FILE="$INFRA/env.example.txt"

COMPOSE=("docker" "compose" "--project-directory" "$INFRA" "-f" "$INFRA/docker-compose.yml" "--env-file" "$ENV_FILE")

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die() { printf '\033[31m[fail]\033[0m %s\n' "$*" >&2; exit 1; }

FORCE=no
REBUILD=no
for arg in "$@"; do
  case "$arg" in
    -y|--yes)   FORCE=yes ;;
    --up)       REBUILD=yes ;;
    -h|--help)
      cat <<EOF
usage: scripts/reset.sh [-y|--yes] [--up]

  -y, --yes   skip the confirmation prompt
  --up        run scripts/up.sh after the wipe
EOF
      exit 0 ;;
    *) die "unknown argument: $arg" ;;
  esac
done

S3_ENDPOINT="$(sed -n 's/^AWS_ENDPOINT_URL=//p' "$ENV_FILE" | head -1)"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:4566}"
S3_BUCKET="$(sed -n 's/^NFL_S3_BUCKET=//p' "$ENV_FILE" | head -1)"
S3_BUCKET="${S3_BUCKET:-nfl-data-platform}"

say "This DESTROYS all NFL local data"
cat <<EOF
    nfl_mysql-data       source system rows
    nfl_postgres-data    staging + utm schemas and the least-privilege roles
    nfl_kafka-data       CDC offsets and Debezium schema history
    nfl_airflow-db-data  DAG run history
    nfl_airflow-logs     task logs
    nfl_sftp-upload      delivered report files
    nfl_mail-data        the Operations inbox (every alert email)
    nfl_logship-data     the log shipper's read positions
    log groups /nfl/*    shipped container and task logs, in floci's CloudWatch Logs
    s3://$S3_BUCKET   keys under df_*/, reports/, _tmp/, _fixtures/ only
                         (via the floci endpoint $S3_ENDPOINT)

    NOT touched: the floci container and its other buckets, every other
                 project on this machine, infra/nfl.env, infra/keys/
EOF

if [ "$FORCE" != yes ]; then
  printf '\nType "reset" to confirm: '
  read -r reply
  [ "$reply" = "reset" ] || die "aborted (got '$reply')"
fi

say "Tearing down with volumes"
"${COMPOSE[@]}" down --volumes --remove-orphans

say "Verifying volumes are gone"
# Label filter, not a name filter: `--filter name=nfl_` is a substring match and
# would also select another project's volume that merely contains "nfl_".
nfl_volumes() { docker volume ls --filter 'label=com.docker.compose.project=nfl' --format '{{.Name}}' || true; }
LEFT="$(nfl_volumes)"
if [ -n "$LEFT" ]; then
  echo "    still present, removing explicitly:"
  echo "$LEFT" | sed 's/^/      /'
  echo "$LEFT" | xargs docker volume rm
  LEFT="$(nfl_volumes)"
  [ -z "$LEFT" ] || die "could not remove: $LEFT"
fi
echo "    no volumes of compose project nfl remain"

say "Deleting pipeline objects from s3://$S3_BUCKET"
if curl -fsS --max-time 5 "$S3_ENDPOINT/_localstack/health" >/dev/null 2>&1; then
  python3 "$REPO_ROOT/scripts/s3_bucket.py" purge "$S3_ENDPOINT" "$S3_BUCKET" \
    || die "could not purge pipeline objects from s3://$S3_BUCKET"
else
  die "floci at $S3_ENDPOINT is not reachable, so the pipeline's S3 objects were NOT deleted.
    Volumes are already gone. Start floci and re-run scripts/reset.sh, otherwise
    old markers, parquet and reports would survive into the next cold start."
fi

say "Deleting shipped logs (CloudWatch Logs groups /nfl/*)"
# Only groups under the /nfl/ prefix, one DeleteLogGroup each. Old log lines
# must not survive into a cold start any more than old S3 markers may.
logs_call() {
  curl -fsS --max-time 15 -X POST "$S3_ENDPOINT/" \
    --aws-sigv4 "aws:amz:${AWS_REGION_FOR_LOGS}:logs" --user "${LOGS_KEY}:${LOGS_SECRET}" \
    -H 'Content-Type: application/x-amz-json-1.1' -H "X-Amz-Target: Logs_20140328.$1" -d "$2"
}
AWS_REGION_FOR_LOGS="$(sed -n 's/^AWS_DEFAULT_REGION=//p' "$ENV_FILE" | head -1)"; AWS_REGION_FOR_LOGS="${AWS_REGION_FOR_LOGS:-us-east-1}"
LOGS_KEY="$(sed -n 's/^AWS_ACCESS_KEY_ID=//p' "$ENV_FILE" | head -1)"; LOGS_KEY="${LOGS_KEY:-test}"
LOGS_SECRET="$(sed -n 's/^AWS_SECRET_ACCESS_KEY=//p' "$ENV_FILE" | head -1)"; LOGS_SECRET="${LOGS_SECRET:-test}"
GROUPS_JSON="$(logs_call DescribeLogGroups '{"logGroupNamePrefix":"/nfl/"}')" \
  || die "could not list log groups on $S3_ENDPOINT"
printf '%s' "$GROUPS_JSON" | python3 -c '
import json, sys
for g in json.load(sys.stdin).get("logGroups", []):
    name = g["logGroupName"]
    if name.startswith("/nfl/"):
        print(name)
' | while IFS= read -r group; do
  logs_call DeleteLogGroup "{\"logGroupName\":\"$group\"}" >/dev/null \
    || die "could not delete log group $group"
  echo "    deleted $group"
done
LEFT_GROUPS="$(logs_call DescribeLogGroups '{"logGroupNamePrefix":"/nfl/"}' | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("logGroups", [])))')"
[ "$LEFT_GROUPS" = 0 ] || die "$LEFT_GROUPS log group(s) under /nfl/ remain"
echo "    no log group under /nfl/ remains"

say "Clearing rendered secrets"
rm -f "$INFRA/secrets/connect-secrets.properties"
echo "    removed infra/secrets/connect-secrets.properties"

if [ "$REBUILD" = yes ]; then
  say "Rebuilding"
  exec "$REPO_ROOT/scripts/up.sh"
fi

say "Reset complete. Run scripts/up.sh for a cold start."
