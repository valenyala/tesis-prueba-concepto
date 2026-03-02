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
  - **log/** — Run log (`run.log`) tracking every crawl run with timestamps and node counts
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

# Crawl pages and build page-level graph (Neo4j storage)
python tesis-playwrightCrawler/es/graph_crawler_neo4j.py --run-id <name> [--max-depth N]

# Crawl pages and build site-level graph (Neo4j storage, one node per domain)
python tesis-playwrightCrawler/es/site_graph_crawler_neo4j.py --run-id <name> [--max-depth N] [--external-max-hops N]

# Visualize existing JSON graph data
python tesis-playwrightCrawler/es/visualize_graph.py
```

All Neo4j crawlers support `--run-id` to store multiple independent graphs in the same database. Each run is logged to `tesis-playwrightCrawler/log/run.log`.

Crawlers launch Chromium in headless mode (`headless=True`).

## Architecture

### Pipeline

1. **Link Discovery** (`buscador-item.py`) — Navigates paginated GeoNetwork search results, extracts `#/metadata/{uuid}` links, writes them to `*_metadata_links.txt`
2. **Graph Crawling** — Three implementations:
   - `graph_crawler.py` — Stores graph in-memory with NetworkX, exports to `web_graph.json` and `web_structure.png`
   - `graph_crawler_neo4j.py` — Page-level graph in Neo4j (one node per URL), via Bolt protocol using MERGE-based upserts
   - `site_graph_crawler_neo4j.py` — Site-level graph in Neo4j (one node per domain), aggregates link counts between sites
3. **Visualization** (`visualize_graph.py`) — Loads `web_graph.json` and renders a color-coded network diagram

### Crawler Design

All crawler classes share core logic:
- Recursive depth-limited crawling (`max_depth`)
- External hop limiting (`external_max_hops`, default 2) to avoid crawling too far outside the catalog
- Link categorization: `metadata`, `search`, `download`, `external`, `internal`, `action`
- URL normalization handling hash fragments (`#/metadata/...`, `#/search`)
- Skip patterns for auth/admin pages
- 2-second delay + `networkidle` wait for JS-heavy GeoNetwork pages
- Per-page link follow limit of 10 to prevent combinatorial explosion
- `--run-id` support for storing multiple independent graphs in the same Neo4j instance
- Run logging to `tesis-playwrightCrawler/log/run.log` (timestamp, crawler type, run_id, node count)

### Neo4j Schema

**Page-level graph** (`graph_crawler_neo4j.py`):
- **Node label:** `Page` with properties: `url`, `run_id` (composite unique constraint), `label`, `title`, `depth`, `type`, `links_count`, `external_hops`, `error`
- **Relationship:** `LINKS_TO` with properties: `run_id`, `type`, `text`

**Site-level graph** (`site_graph_crawler_neo4j.py`):
- **Node label:** `Site` with properties: `url`, `run_id` (composite unique constraint), `label` (domain), `pages_crawled`
- **Relationship:** `LINKS_TO` with properties: `run_id`, `link_count` (aggregated page-level links between sites)

### Key Dependencies

Python: `playwright`, `networkx`, `matplotlib`, `neo4j` (Python driver)
Infrastructure: Docker, Neo4j 5 with APOC plugin
