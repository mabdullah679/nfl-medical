#!/usr/bin/env bash
# One-time local initialisation. No manual editing afterwards.
#
#   scripts/init-env.sh              create infra/nfl.env + SFTP keys
#   scripts/init-env.sh --force      regenerate infra/nfl.env (new passwords)
#   scripts/init-env.sh --keys-only  only ensure infra/keys/sftp exists
#
# infra/nfl.env is infra/env.example.txt with every CHANGEME_* replaced by a
# freshly generated random value. Values are never printed.
#
# --force on a stack that already has data volumes makes the stored passwords
# disagree with the new file: run scripts/reset.sh first, or the databases will
# reject every login.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="$REPO_ROOT/infra"
TEMPLATE="$INFRA/env.example.txt"
ENV_FILE="$INFRA/nfl.env"
KEY_DIR="$INFRA/keys/sftp"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die() { printf '\033[31m[fail]\033[0m %s\n' "$*" >&2; exit 1; }

FORCE=no
KEYS_ONLY=no
for arg in "$@"; do
  case "$arg" in
    --force)     FORCE=yes ;;
    --keys-only) KEYS_ONLY=yes ;;
    -h|--help)   sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $arg" ;;
  esac
done

command -v python3 >/dev/null || die "python3 not on PATH"
command -v ssh-keygen >/dev/null || die "ssh-keygen not on PATH"

write_env() {
  [ -f "$TEMPLATE" ] || die "missing template $TEMPLATE"
  if [ -f "$ENV_FILE" ] && [ "$FORCE" != yes ]; then
    die "$ENV_FILE already exists - refusing to overwrite it.
    Existing database volumes hold the passwords in that file. To regenerate:
      scripts/reset.sh -y && scripts/init-env.sh --force"
  fi

  umask 077
  python3 - "$TEMPLATE" "$ENV_FILE" <<'PY'
import base64, os, re, secrets, string, sys

template, target = sys.argv[1], sys.argv[2]
alnum = string.ascii_letters + string.digits
generated = []
out = []
for line in open(template):
    m = re.match(r"^([A-Z0-9_]+)=CHANGEME_\w*\s*$", line)
    if not m:
        out.append(line)
        continue
    name = m.group(1)
    if name == "AIRFLOW_FERNET_KEY":
        value = base64.urlsafe_b64encode(os.urandom(32)).decode()
    elif name == "AIRFLOW_SECRET_KEY":
        value = secrets.token_hex(32)
    else:
        value = "".join(secrets.choice(alnum) for _ in range(32))
    out.append(f"{name}={value}\n")
    generated.append(name)

body = "".join(out)
if "CHANGEME" in "".join(l for l in out if not l.lstrip().startswith("#")):
    sys.exit("a CHANGEME placeholder survived generation - template line not in NAME=CHANGEME_x form")

tmp = target + ".tmp"
fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as fh:
    fh.write(body)
os.replace(tmp, target)
print(f"    generated {len(generated)} secret value(s): " + ", ".join(generated))
PY
  chmod 600 "$ENV_FILE"
  echo "    wrote $ENV_FILE (0600, gitignored; values not shown)"
}

# The SFTP server's host keys are generated HERE and mounted into the server,
# rather than letting the container invent new ones on every recreate. That is
# what makes pinning possible: known_hosts is written from the same key, so a
# client that connects to anything else is refused instead of trusting it.
write_keys() {
  umask 077
  mkdir -p "$KEY_DIR"
  local made=0
  if [ ! -f "$KEY_DIR/ssh_host_ed25519_key" ]; then
    ssh-keygen -q -t ed25519 -N '' -C nfl-sftp-host -f "$KEY_DIR/ssh_host_ed25519_key"; made=1
  fi
  if [ ! -f "$KEY_DIR/ssh_host_rsa_key" ]; then
    ssh-keygen -q -t rsa -b 4096 -N '' -C nfl-sftp-host -f "$KEY_DIR/ssh_host_rsa_key"; made=1
  fi
  if [ ! -f "$KEY_DIR/check_ed25519" ]; then
    ssh-keygen -q -t ed25519 -N '' -C nfl_check_delivery_sftp -f "$KEY_DIR/check_ed25519"; made=1
  fi

  # Both names a client may dial: the compose hostname (port 22) and the
  # published host port.
  local port="2225"
  if [ -f "$ENV_FILE" ]; then
    port="$(sed -n 's/^NFL_SFTP_PORT=//p' "$ENV_FILE" | head -1)"; port="${port:-2225}"
  fi
  local ed rsa
  ed="$(cut -d' ' -f1,2 "$KEY_DIR/ssh_host_ed25519_key.pub")"
  rsa="$(cut -d' ' -f1,2 "$KEY_DIR/ssh_host_rsa_key.pub")"
  {
    echo "nfl-sftp $ed"
    echo "nfl-sftp $rsa"
    echo "[localhost]:$port $ed"
    echo "[localhost]:$port $rsa"
    echo "[127.0.0.1]:$port $ed"
    echo "[127.0.0.1]:$port $rsa"
  } > "$KEY_DIR/known_hosts"

  chmod 600 "$KEY_DIR/ssh_host_ed25519_key" "$KEY_DIR/ssh_host_rsa_key" "$KEY_DIR/check_ed25519"
  chmod 644 "$KEY_DIR"/*.pub "$KEY_DIR/known_hosts"
  chmod 755 "$KEY_DIR"
  if [ "$made" = 1 ]; then
    echo "    generated SFTP host keys + checker key pair in $KEY_DIR (gitignored)"
  else
    echo "    SFTP keys already present in $KEY_DIR"
  fi
  echo "    host key fingerprint: $(ssh-keygen -lf "$KEY_DIR/ssh_host_ed25519_key.pub" | cut -d' ' -f2)"
}

if [ "$KEYS_ONLY" != yes ]; then
  say "Generating infra/nfl.env"
  write_env
fi

say "Ensuring SFTP keys"
write_keys

if [ "$KEYS_ONLY" != yes ]; then
  say "Initialised. Next: scripts/up.sh"
fi
