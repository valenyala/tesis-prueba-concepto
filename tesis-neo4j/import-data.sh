#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCHIVE="${1:?Usage: $0 <path-to-backup.tar.gz>}"

if [ ! -f "$ARCHIVE" ]; then
  echo "ERROR: File not found: $ARCHIVE"
  exit 1
fi

# Stop Neo4j if running
if docker ps --format '{{.Names}}' | grep -q '^tesis-neo4j$'; then
  echo "Stopping Neo4j..."
  cd "$SCRIPT_DIR" && docker compose down
fi

ARCHIVE_ABS="$(cd "$(dirname "$ARCHIVE")" && pwd)/$(basename "$ARCHIVE")"

echo "Importing $ARCHIVE_ABS -> volume 'tesis-neo4j-data'"
docker run --rm \
  -v tesis-neo4j-data:/data \
  -v "$(dirname "$ARCHIVE_ABS")":/backup \
  alpine sh -c "rm -rf /data/* && tar xzf /backup/$(basename "$ARCHIVE_ABS") -C /data"

echo "Import complete. Run ./start.sh to start Neo4j with the restored data."
