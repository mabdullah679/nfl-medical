#!/usr/bin/env bash
# Runs inside the mysql container before the numbered .sql files.
#
# 02-debezium-user.sql.tmpl is committed with CHANGEME_* placeholders so no real
# credential lands in git. This renders it to a container-local temp file and
# applies that with the mysql client.
#
# It deliberately does NOT write back through the bind mount: doing so would
# modify the committed file on the HOST and leak a real password into the repo
# (verified: writing to a bind-mounted file changes the host file).
#
# Without this rendering the debezium user gets the literal placeholder and
# Kafka Connect fails registration with:
#   Unable to connect: Access denied for user 'debezium'@'<ip>' (using password: YES)
#
# awk-based literal substitution: mysql:8.0 has no python3, and sed would
# misinterpret / & | in a password.
set -euo pipefail

TMPL=/docker-entrypoint-initdb.d/02-debezium-user.sql.tmpl
RENDERED=/tmp/02-debezium-user.rendered.sql

echo "[render-users] rendering MySQL user DDL from environment"
[ -f "$TMPL" ] || { echo "[render-users] FATAL: missing $TMPL" >&2; exit 1; }

umask 077
cp "$TMPL" "$RENDERED"

render_one() {
  placeholder="$1"; value="$2"; label="$3"
  if [ -z "${value:-}" ]; then
    echo "[render-users] FATAL: $label is unset/empty - refusing to create a user with a placeholder password" >&2
    exit 1
  fi
  case "$value" in
    *"'"*) echo "[render-users] FATAL: $label contains a single quote; not supported" >&2; exit 1 ;;
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
  echo "[render-users]   $label -> set (${#value} chars)"
}

render_one CHANGEME_mysql_cdc "${MYSQL_CDC_PASSWORD:-}" MYSQL_CDC_PASSWORD
render_one CHANGEME_mysql_app "${MYSQL_APP_PASSWORD:-}" MYSQL_APP_PASSWORD

if grep -q 'CHANGEME_mysql_' "$RENDERED"; then
  echo "[render-users] FATAL: placeholders still present after render" >&2
  exit 1
fi

echo "[render-users] applying rendered user DDL"
mysql --protocol=socket -uroot -p"${MYSQL_ROOT_PASSWORD}" < "$RENDERED"
rm -f "$RENDERED"
echo "[render-users] done - users created, no credential written to the repo"
