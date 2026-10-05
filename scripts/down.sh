#!/usr/bin/env bash
# Stop the NFL stack. Containers and the network go; named volumes STAY, so
# data survives a down/up cycle. Use scripts/reset.sh to wipe data.
#
# Scoped to the "nfl" compose project only: every docker command here either
# goes through that project's compose file or filters on its project label.
# No other project's containers, volumes or networks, and not floci.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="$REPO_ROOT/infra"
ENV_FILE="$INFRA/nfl.env"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# down needs no real secret, but compose still interpolates the file. Fall back
# to the template so `down` never blocks on a missing env file.
if [ ! -f "$ENV_FILE" ]; then
  say "No infra/nfl.env - using env.example.txt for interpolation only"
  ENV_FILE="$INFRA/env.example.txt"
fi

COMPOSE=("docker" "compose" "--project-directory" "$INFRA" "-f" "$INFRA/docker-compose.yml" "--env-file" "$ENV_FILE")

# Some images declare VOLUMEs the compose file does not name (Kafka Connect's
# /kafka/config, /kafka/data, /kafka/logs, ...). Docker gives each container an
# anonymous volume for those, and a plain `down` leaves them behind, so every
# down/up cycle would leak a few. They hold nothing that must survive (Connect
# keeps its state in Kafka topics). Collected here from this project's own
# containers and removed by exact name after the containers are gone.
ANON_VOLUMES="$(docker ps -aq --filter 'label=com.docker.compose.project=nfl' \
  | xargs -n1 docker inspect -f '{{range .Mounts}}{{if eq .Type "volume"}}{{println .Name}}{{end}}{{end}}' 2>/dev/null \
  | grep -E '^[0-9a-f]{64}$' || true)"

say "Stopping nfl stack (named volumes preserved)"
"${COMPOSE[@]}" down --remove-orphans

if [ -n "$ANON_VOLUMES" ]; then
  say "Removing this stack's anonymous volumes"
  printf '%s\n' "$ANON_VOLUMES" | while read -r v; do
    docker volume rm "$v" >/dev/null && echo "    removed ${v:0:12}"
  done
fi

say "Remaining nfl containers (expect none)"
docker ps -a --filter 'label=com.docker.compose.project=nfl' --format '    {{.Names}}  {{.Status}}' || true

say "Volumes kept (use scripts/reset.sh to delete)"
docker volume ls --filter 'label=com.docker.compose.project=nfl' --format '    {{.Name}}' || true

say "Done. Only the nfl compose project was touched."
