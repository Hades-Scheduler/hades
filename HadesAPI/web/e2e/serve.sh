#!/usr/bin/env bash
# Boots the full stack for the Playwright e2e suite: a NATS (JetStream) container
# plus HadesAPI with the dashboard enabled and the SPA embedded. Runs the API in
# the foreground so Playwright's `webServer` can manage its lifecycle; the NATS
# container is torn down by e2e/global-teardown.ts.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEB_DIR="$(cd "$HERE/.." && pwd)"       # HadesAPI/web
API_DIR="$(cd "$WEB_DIR/.." && pwd)"    # HadesAPI

NATS_NAME="hades-e2e-nats"
API_PORT="${E2E_API_PORT:-8099}"
NATS_PORT="${E2E_NATS_PORT:-4223}"

echo "[e2e] (re)starting NATS container ($NATS_NAME) on :$NATS_PORT"
docker rm -f "$NATS_NAME" >/dev/null 2>&1 || true
# Retry the run: a just-removed container can briefly hold the published port.
for attempt in $(seq 1 20); do
  if docker run -d --rm --name "$NATS_NAME" -p "${NATS_PORT}:4222" nats:2.11.4 -js >/dev/null 2>&1; then
    break
  fi
  echo "[e2e] NATS start attempt $attempt failed (port busy?), retrying..."
  sleep 1
done

# Wait for NATS to accept TCP connections (bash /dev/tcp is portable, no nc needed).
nats_ready=0
for _ in $(seq 1 60); do
  if (exec 3<>"/dev/tcp/localhost/${NATS_PORT}") 2>/dev/null; then
    exec 3>&- 3<&- 2>/dev/null || true
    nats_ready=1
    break
  fi
  sleep 0.5
done
if [ "$nats_ready" -ne 1 ]; then
  echo "[e2e] ERROR: NATS did not become ready on :$NATS_PORT" >&2
  docker logs "$NATS_NAME" 2>&1 | tail -20 >&2 || true
  exit 1
fi

echo "[e2e] building dashboard SPA"
(cd "$WEB_DIR" && npm run build)

echo "[e2e] building HadesAPI binary"
API_BIN="$(mktemp -t hades-e2e-api.XXXXXX)"
(cd "$API_DIR" && go build -o "$API_BIN" .)

echo "[e2e] starting HadesAPI on :$API_PORT"
export NATS_URL="nats://localhost:${NATS_PORT}"
export API_PORT
export ALLOW_UNAUTHENTICATED=true
export DASHBOARD_USERNAME="admin"
# bcrypt hash of "test-password" (see e2e/fixtures.ts).
export DASHBOARD_PASSWORD_HASH='$2y$10$oKaVlKDJczMUQJyWZxMIGenHvvP.4mIOTPSOHLcIVewfZi6JioAii'
export DASHBOARD_SESSION_SECRET="e2e-session-secret-0123456789abcdef"
export DASHBOARD_JOB_RETENTION="1h"
# No log manager in this stack: the logs proxy will return 503, which the suite
# asserts as graceful degradation.
export LOG_MANAGER_URL="http://127.0.0.1:1"
exec "$API_BIN"
