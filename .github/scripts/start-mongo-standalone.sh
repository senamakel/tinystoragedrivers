#!/usr/bin/env bash
# Start a plain MongoDB server for standalone capability tests.
set -euo pipefail

port="${1:-27018}"
image="${2:-mongo:7}"
name="tsd-mongo-standalone"

cleanup() {
  if [[ $? -ne 0 ]]; then
    docker rm -f "$name" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker run -d --rm --name "$name" -p "127.0.0.1:${port}:27017" "$image" \
  --bind_ip_all >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$name" mongosh --quiet --eval 'db.adminCommand({ping: 1}).ok' >/dev/null 2>&1; then
    echo "Standalone MongoDB ready on port ${port}"
    exit 0
  fi
  sleep 1
done

echo "Standalone MongoDB did not become ready" >&2
exit 1
