#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAX_WAIT=60

cd "$SCRIPT_DIR"

# Start all containers
echo "Starting Neo4j + Redis containers..."
podman compose up -d --remove-orphans

wait_for_neo4j() {
  local container="$1"
  local bolt_port="$2"
  local http_port="$3"
  echo -n "Waiting for $container to be ready"
  local elapsed=0
  until podman exec "$container" cypher-shell -u neo4j -p tesis_password "RETURN 1" &>/dev/null; do
    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
      echo ""
      echo "ERROR: $container did not become ready within ${MAX_WAIT}s"
      exit 1
    fi
    echo -n "."
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo ""
  echo "$container is ready."
  echo "  Browser : http://localhost:${http_port}"
  echo "  Bolt    : bolt://localhost:${bolt_port}"
  echo "  User    : neo4j"
  echo "  Password: tesis_password"
}

wait_for_neo4j "tesis-neo4j-es" 7687 7474
wait_for_neo4j "tesis-neo4j-ch" 7688 7475

echo -n "Waiting for Redis to be ready"
elapsed=0
until podman exec tesis-redis redis-cli ping 2>/dev/null | grep -q PONG; do
  if [ "$elapsed" -ge "$MAX_WAIT" ]; then
    echo ""
    echo "ERROR: Redis did not become ready within ${MAX_WAIT}s"
    exit 1
  fi
  echo -n "."
  sleep 1
  elapsed=$((elapsed + 1))
done
echo ""
echo "Redis is ready."
echo "  Endpoint: redis://localhost:6379"
