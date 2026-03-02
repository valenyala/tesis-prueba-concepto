#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

echo "Stopping Neo4j containers..."
podman compose down
echo "Neo4j stopped. Data volumes 'tesis-neo4j-es-data' and 'tesis-neo4j-ch-data' are preserved."
