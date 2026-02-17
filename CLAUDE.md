# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Thesis proof-of-concept analyzing the web structure of geographic/spatial data catalogs (GeoNetwork-based). The project crawls metadata catalog sites, builds directed graphs of their link structure, and stores/visualizes the results.

Two target catalogs:
- **Spain (IDEE):** `https://www.idee.es/csw-inspire-idee/srv/spa/catalog.search` — primary, more developed
- **Uruguay:** similar structure, less developed (in `uy/`)

## Repository Structure

- **tesis-neo4j/** — Docker-based Neo4j 5 setup for graph storage
- **tesis-playwrightCrawler/** — Python async crawlers using Playwright
  - **es/** — Spain-specific crawlers and data
  - **uy/** — Uruguay-specific crawlers and data
  - **notas/** — Research notes

## Commands

### Neo4j Database

```bash
# Start Neo4j container (waits for readiness)
cd tesis-neo4j && ./start.sh

# Stop Neo4j container
cd tesis-neo4j && ./stop.sh

# Export/import data backups
cd tesis-neo4j && ./export-data.sh [output-dir]
cd tesis-neo4j && ./import-data.sh <path-to-backup.tar.gz>
```

Neo4j credentials: `neo4j` / `tesis_password`
Ports: 7474 (browser), 7687 (Bolt)

### Running Crawlers

All Python scripts are run directly (no virtual env or package manager detected):

```bash
# Discover metadata links from search results
python tesis-playwrightCrawler/es/buscador-item.py

# Crawl pages and build graph (NetworkX → JSON + PNG)
python tesis-playwrightCrawler/es/graph_crawler.py

# Crawl pages and build graph (Neo4j storage)
python tesis-playwrightCrawler/es/graph_crawler_neo4j.py

# Visualize existing JSON graph data
python tesis-playwrightCrawler/es/visualize_graph.py
```

Crawlers launch a visible Chromium browser (`headless=False`).

## Architecture

### Pipeline

1. **Link Discovery** (`buscador-item.py`) — Navigates paginated GeoNetwork search results, extracts `#/metadata/{uuid}` links, writes them to `*_metadata_links.txt`
2. **Graph Crawling** — Two parallel implementations that share the same crawling logic:
   - `graph_crawler.py` — Stores graph in-memory with NetworkX, exports to `web_graph.json` and `web_structure.png`
   - `graph_crawler_neo4j.py` — Stores graph directly in Neo4j via Bolt protocol using MERGE-based upserts
3. **Visualization** (`visualize_graph.py`) — Loads `web_graph.json` and renders a color-coded network diagram

### Crawler Design

Both crawler classes (`WebGraphCrawler`, `WebGraphCrawlerNeo4j`) share the same core logic:
- Recursive depth-limited crawling (`max_depth`, default 2)
- External hop limiting (`external_max_hops`, default 2) to avoid crawling too far outside the catalog
- Link categorization: `metadata`, `search`, `download`, `external`, `internal`, `action`
- URL normalization handling hash fragments (`#/metadata/...`, `#/search`)
- Skip patterns for auth/admin pages
- 2-second delay + `networkidle` wait for JS-heavy GeoNetwork pages
- Per-page link follow limit of 10 to prevent combinatorial explosion

### Neo4j Schema

- **Node label:** `Page` with properties: `url` (unique constraint), `label`, `title`, `depth`, `type`, `links_count`, `external_hops`, `error`
- **Relationship:** `LINKS_TO` with properties: `type`, `text`

### Key Dependencies

Python: `playwright`, `networkx`, `matplotlib`, `neo4j` (Python driver)
Infrastructure: Docker, Neo4j 5 with APOC plugin
