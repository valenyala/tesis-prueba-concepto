#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${1:-$SCRIPT_DIR/exports}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
EXPORT_DIR="$OUTPUT_DIR/graph-export-${TIMESTAMP}"
CONTAINER_NAME="tesis-neo4j"
NEO4J_USER="neo4j"
NEO4J_PASSWORD="tesis_password"

cypher() {
  podman exec "$CONTAINER_NAME" cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" --format plain "$1"
}

# Check that the container is running
if ! podman ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "ERROR: Container '$CONTAINER_NAME' is not running. Start it first with ./start.sh"
  exit 1
fi

mkdir -p "$EXPORT_DIR"

echo "Exporting nodes..."
cypher "
MATCH (p:Page)
RETURN p.url AS url, p.label AS label, p.title AS title,
       p.type AS type, p.depth AS depth,
       p.links_count AS links_count, p.external_hops AS external_hops,
       p.error AS error
" > "$EXPORT_DIR/nodes.csv"

echo "Exporting relationships..."
cypher "
MATCH (src:Page)-[r:LINKS_TO]->(tgt:Page)
RETURN src.url AS source, tgt.url AS target, r.type AS type, r.text AS text
" > "$EXPORT_DIR/edges.csv"

# Print summary
NODE_COUNT=$(cypher "MATCH (n) RETURN count(n) AS c" | tail -1)
EDGE_COUNT=$(cypher "MATCH ()-[r]->() RETURN count(r) AS c" | tail -1)

echo ""
echo "Export complete: $EXPORT_DIR"
echo "  nodes.csv  ($NODE_COUNT nodes)"
echo "  edges.csv  ($EDGE_COUNT relationships)"
