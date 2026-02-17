#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_NAME="tesis-neo4j"
BOLT_URL="bolt://localhost:7687"
MAX_WAIT=60

cd "$SCRIPT_DIR"

# Start the container
echo "Starting Neo4j..."
docker compose up -d

# Wait until Bolt port is accepting connections
echo -n "Waiting for Neo4j to be ready"
elapsed=0
until docker exec "$CONTAINER_NAME" cypher-shell -u neo4j -p tesis_password "RETURN 1" &>/dev/null; do
  if [ "$elapsed" -ge "$MAX_WAIT" ]; then
    echo ""
    echo "ERROR: Neo4j did not become ready within ${MAX_WAIT}s"
    exit 1
  fi
  echo -n "."
  sleep 2
  elapsed=$((elapsed + 2))
done

echo ""
echo "Neo4j is ready."
echo "  Browser : http://localhost:7474"
echo "  Bolt    : $BOLT_URL"
echo "  User    : neo4j"
echo "  Password: tesis_password"
