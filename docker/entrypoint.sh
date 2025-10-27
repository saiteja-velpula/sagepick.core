#!/bin/sh
set -euo pipefail

log() {
  printf '%s %s\n' "[entrypoint]" "$1"
}

DB_ATTEMPTS=${DB_ATTEMPTS:-10}
DB_SLEEP=${DB_SLEEP:-5}

attempt=1
while [ "$attempt" -le "$DB_ATTEMPTS" ]; do
  log "Running alembic upgrade (attempt $attempt/$DB_ATTEMPTS)"
  if alembic upgrade head; then
    log "Database migrations complete"
    break
  fi

  if [ "$attempt" -eq "$DB_ATTEMPTS" ]; then
    log "Alembic upgrade failed after $DB_ATTEMPTS attempts"
    exit 1
  fi

  attempt=$((attempt + 1))
  log "Alembic upgrade failed; retrying in ${DB_SLEEP}s"
  sleep "$DB_SLEEP"
done

exec "$@"
