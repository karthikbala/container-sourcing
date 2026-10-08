#!/usr/bin/env bash
# Keep this command open during manual enrollment; Ctrl-C closes the tunnel.
set -euo pipefail
: "${SOURCING_SSH_HOST:?Set the authorized SSH user@host}"
: "${SOURCING_SSH_KEY:?Set the existing SSH private-key path}"
sourcing_db_address=$(ssh -i "$SOURCING_SSH_KEY" -o BatchMode=yes "$SOURCING_SSH_HOST" \
  'cd /opt/porttrack && docker inspect --format '\''{{(index .NetworkSettings.Networks "porttrack_default").IPAddress}}'\'' "$(docker compose ps -q postgres)"')
[[ "$sourcing_db_address" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'Database address could not be resolved' >&2; exit 1; }
exec ssh -i "$SOURCING_SSH_KEY" -o BatchMode=yes -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 -N -L "127.0.0.1:55432:$sourcing_db_address:5432" "$SOURCING_SSH_HOST"
