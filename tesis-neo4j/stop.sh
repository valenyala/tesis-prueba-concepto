#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

echo "Stopping Neo4j..."
docker compose down
echo "Neo4j stopped. Data volume 'tesis-neo4j-data' is preserved."
