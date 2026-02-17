#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${1:-$SCRIPT_DIR/backups}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
ARCHIVE="$OUTPUT_DIR/neo4j-data-${TIMESTAMP}.tar.gz"

mkdir -p "$OUTPUT_DIR"

# Make sure the container is stopped so data is consistent
if docker ps --format '{{.Names}}' | grep -q '^tesis-neo4j$'; then
  echo "Stopping Neo4j for a consistent backup..."
  cd "$SCRIPT_DIR" && docker compose down
  WAS_RUNNING=1
else
  WAS_RUNNING=0
fi

echo "Exporting volume 'tesis-neo4j-data' -> $ARCHIVE"
docker run --rm \
  -v tesis-neo4j-data:/data \
  -v "$OUTPUT_DIR":/backup \
  alpine tar czf "/backup/neo4j-data-${TIMESTAMP}.tar.gz" -C /data .

echo "Export complete: $ARCHIVE"

if [ "$WAS_RUNNING" -eq 1 ]; then
  echo "Restarting Neo4j..."
  cd "$SCRIPT_DIR" && docker compose up -d
fi
