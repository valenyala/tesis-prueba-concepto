# Project Memory

## Catalogs
- **ES (Spain IDEE):** idee.es, Neo4j at bolt://localhost:7687 (port 7474 browser)
- **CH (Switzerland geocat.ch):** geocat.ch, Neo4j at bolt://localhost:7688 (port 7475 browser)
- Neo4j credentials: neo4j / tesis_password
- Containers: `tesis-neo4j-es`, `tesis-neo4j-ch` (Podman/Docker via tesis-neo4j/docker-compose.yml)

## Latest Run IDs
- **ES site-level (latest):** `sites-es-2026-02-23` — 361 seeds, ~64 site nodes
- **CH site-level (latest):** `sites-ch-2026-02-23` — 1141 seeds, ~181 site nodes
- Also in run.log: `es_20260223` (64 nodes), `run_20260223_17:06:48` (171 nodes) — earlier CH page-level runs
- CH runs in run.log: `run_20260223` (166), `run_20260223_16:11:01` (146), `run_20260223_17:06:48` (171)

## Analysis Scripts
- `analysis/compare_catalogs.py` — WSM cross-catalog comparison (ES vs CH)
  - Default run IDs: ES=`sites-es-2026-02-23`, CH=`sites-ch-2026-02-23`
  - Metrics: structural, degree distribution (power-law α), bow-tie, PageRank, HITS, betweenness, Gini, Jaccard
  - Output: markdown report to `analysis/catalog_comparison_<timestamp>.md`
  - Run: `python analysis/compare_catalogs.py [--path-length] [--exclude-es LABEL...] [--exclude-ch LABEL...]`
- `tesis-playwrightCrawler/ch/analyze_runs.py` — compares 3 CH page-level runs (ports 7688)
- `tesis-playwrightCrawler/es/analyze_graph.py` — single ES site-level run analysis (port 7687)
- `tesis-playwrightCrawler/ch/graph_analysis.py` — CH site-level analysis (port 7688)

## Key Findings (from past analysis reports)
- ES: www.idee.es dominates (362/363 pages crawled), almost pure hub-and-spoke star
- CH: www.geocat.ch dominant hub; broader ecosystem with swisstopo, geo.admin.ch, etc.
- CH Inland Waters run: 181 sites, 242 edges, www.geocat.ch PageRank=0.028
