# Web Structure Mining — Catalog Comparison

Generated: 2026-03-02 16:38:43

| | ES (IDEE) | CH (geocat.ch) |
|---|---|---|
| Run ID | `run_20260223` | `run_20260223_17:06:48` |
| Neo4j port | 7687 | 7688 |

## 1. Structural Overview

> Site-level graph: one node per domain, weighted directed edges = page-level link counts between domains.

| Metric                               | ES       | CH        |
| ------------------------------------ | -------- | --------- |
| Nodes (sites)                        | 65       | 168       |
| Edges (site-to-site links)           | 64       | 185       |
| Total link weight (page-level links) | 7,368    | 8,087     |
| Total pages crawled                  | 363      | 2697      |
| Graph density                        | 0.015385 | 0.006594  |
| Reciprocity (mutual edges)           | 0.0000   | 0.0432    |
| Self-loops (sites → themselves)      | 0        | 0         |
| Avg clustering coeff (undirected)    | 0.000000 | 0.050999  |
| Degree assortativity                 | nan      | -0.317107 |

## 2. Degree Distribution

> Web graphs (Barabási-Albert model) typically show power-law in-degree distributions (α ≈ 2–3). Power-law α estimated by maximum-likelihood (Clauset et al.).

| Metric                             | ES       | CH       |
| ---------------------------------- | -------- | -------- |
| Avg weighted in-degree             | 113.35   | 48.14    |
| Avg weighted out-degree            | 113.35   | 48.14    |
| Max weighted in-degree             | 854      | 1105     |
| Max weighted out-degree            | 7368     | 7814     |
| Avg unweighted in-degree           | 0.98     | 1.10     |
| Max unweighted in-degree           | 1        | 9        |
| Max unweighted out-degree          | 64       | 77       |
| In-degree variance (w)             | 38852.44 | 26706.34 |
| Power-law α (unweighted in-degree) | N/A      | 10.558   |
| Gini (in-degree)                   | 0.0154   | 0.2361   |
| C10 in-degree (top 10% hold)       | 10.94%   | 26.49%   |

### Top sites by weighted in-degree

**ES:**

| #  | Site                           | Incoming link weight |
| -- | ------------------------------ | -------------------- |
| 1  | www.mapama.gob.es              | 854                  |
| 2  | www.geo.euskadi.eus            | 740                  |
| 3  | www.mitma.gob.es               | 724                  |
| 4  | idena.navarra.es               | 504                  |
| 5  | mapas-gis-inter.carm.es        | 410                  |
| 6  | icearagon.aragon.es            | 366                  |
| 7  | inspire-geoportal.ec.europa.eu | 362                  |
| 8  | catalogo.idecanarias.es        | 362                  |
| 9  | idecyl.jcyl.es                 | 362                  |
| 10 | ide.cat                        | 362                  |
| 11 | idem.madrid.org                | 362                  |
| 12 | ideas.asturias.es              | 362                  |
| 13 | github.com                     | 362                  |
| 14 | wms.mapama.gob.es              | 136                  |
| 15 | www.juntadeandalucia.es        | 106                  |

**CH:**

| #  | Site                    | Incoming link weight |
| -- | ----------------------- | -------------------- |
| 1  | github.com              | 1105                 |
| 2  | info.geocat.ch          | 1097                 |
| 3  | www.geocat.admin.ch     | 1095                 |
| 4  | www.gis-daten.ch        | 706                  |
| 5  | geodienste.ch           | 445                  |
| 6  | map.geo.admin.ch        | 361                  |
| 7  | geobasisdaten.ch        | 315                  |
| 8  | geo.jura.ch             | 237                  |
| 9  | data.geo.admin.ch       | 222                  |
| 10 | geoshop.lisag.ch        | 221                  |
| 11 | www.jura.ch             | 203                  |
| 12 | maps.fr.ch              | 158                  |
| 13 | geoservices.jura.ch     | 112                  |
| 14 | wms.geo.admin.ch        | 109                  |
| 15 | www.meteoswiss.admin.ch | 104                  |

### Top sites by weighted out-degree

**ES:**

| #  | Site                           | Outgoing link weight |
| -- | ------------------------------ | -------------------- |
| 1  | www.idee.es                    | 7368                 |
| 2  | www.mitma.gob.es               | 0                    |
| 3  | inspire-geoportal.ec.europa.eu | 0                    |
| 4  | www.mapama.gob.es              | 0                    |
| 5  | icearagon.aragon.es            | 0                    |
| 6  | catalogo.idecanarias.es        | 0                    |
| 7  | idecyl.jcyl.es                 | 0                    |
| 8  | ide.cat                        | 0                    |
| 9  | idena.navarra.es               | 0                    |
| 10 | idem.madrid.org                | 0                    |
| 11 | www.geo.euskadi.eus            | 0                    |
| 12 | ideas.asturias.es              | 0                    |
| 13 | mapas-gis-inter.carm.es        | 0                    |
| 14 | github.com                     | 0                    |
| 15 | csr.seadatanet.org             | 0                    |

**CH:**

| #  | Site                   | Outgoing link weight |
| -- | ---------------------- | -------------------- |
| 1  | www.geocat.ch          | 7814                 |
| 2  | www.openstreetmap.org  | 42                   |
| 3  | www.geo.admin.ch       | 37                   |
| 4  | map.koeniz.ch          | 28                   |
| 5  | leafletjs.com          | 27                   |
| 6  | raster.sitg.ge.ch      | 23                   |
| 7  | www.swissgeol.ch       | 22                   |
| 8  | map.geo.admin.ch       | 20                   |
| 9  | survey123.arcgis.com   | 20                   |
| 10 | data.geo.admin.ch      | 19                   |
| 11 | cesium.com             | 11                   |
| 12 | info.geocat.ch         | 8                    |
| 13 | viewer.swissgeol.ch    | 6                    |
| 14 | spatialreference.org   | 5                    |
| 15 | www.swisstopo.admin.ch | 5                    |

## 3. Connectivity Analysis

> The **bow-tie model** (Broder et al. 2000) partitions the Web into: CORE (giant SCC), IN (nodes reaching the core), OUT (nodes reachable from core), tendrils, and disconnected nodes.

| Metric                              | ES     | CH    |
| ----------------------------------- | ------ | ----- |
| Isolates (no edges)                 | 0      | 15    |
| Weakly connected components (WCC)   | 1      | 16    |
| Largest WCC (nodes)                 | 65     | 153   |
| WCC coverage (% of nodes)           | 100.0% | 91.1% |
| Strongly connected components (SCC) | 65     | 155   |
| Largest SCC / CORE (nodes)          | 1      | 14    |
| SCC coverage (% of nodes)           | 1.5%   | 8.3%  |

### Bow-tie decomposition

| Component        | ES nodes | CH nodes |
| ---------------- | -------- | -------- |
| CORE (giant SCC) | 1        | 14       |
| IN component     | 1        | 0        |
| OUT component    | 0        | 139      |
| Tendrils         | 63       | 0        |
| Disconnected     | 0        | 15       |

## 4. PageRank (α = 0.85, weighted)

> PageRank models a random web surfer following links. Higher score = more globally influential site. Gini measures inequality of influence distribution (1 = all influence concentrated in one node).

| Metric                      | ES     | CH     |
| --------------------------- | ------ | ------ |
| Gini coefficient (PageRank) | 0.0097 | 0.0784 |
| C10: top 10% sites hold     | 11.32% | 15.61% |
| C20: top 20% sites hold     | 20.82% | 26.32% |

**Top 15 by PageRank — ES:**

| #  | Site                           | PageRank |
| -- | ------------------------------ | -------- |
| 1  | www.mapama.gob.es              | 0.016682 |
| 2  | www.geo.euskadi.eus            | 0.016482 |
| 3  | www.mitma.gob.es               | 0.016454 |
| 4  | idena.navarra.es               | 0.016069 |
| 5  | mapas-gis-inter.carm.es        | 0.015904 |
| 6  | icearagon.aragon.es            | 0.015827 |
| 7  | inspire-geoportal.ec.europa.eu | 0.015820 |
| 8  | catalogo.idecanarias.es        | 0.015820 |
| 9  | idecyl.jcyl.es                 | 0.015820 |
| 10 | ide.cat                        | 0.015820 |
| 11 | idem.madrid.org                | 0.015820 |
| 12 | ideas.asturias.es              | 0.015820 |
| 13 | github.com                     | 0.015820 |
| 14 | wms.mapama.gob.es              | 0.015424 |
| 15 | www.juntadeandalucia.es        | 0.015372 |

**Top 15 by PageRank — CH:**

| #  | Site                   | PageRank |
| -- | ---------------------- | -------- |
| 1  | www.geocat.ch          | 0.024310 |
| 2  | github.com             | 0.013239 |
| 3  | www.geo.admin.ch       | 0.010307 |
| 4  | www.xing.com           | 0.008435 |
| 5  | api.whatsapp.com       | 0.008435 |
| 6  | geoportal.koeniz.ch    | 0.008401 |
| 7  | info.geocat.ch         | 0.008232 |
| 8  | www.geocat.admin.ch    | 0.008227 |
| 9  | www.openstreetmap.org  | 0.008182 |
| 10 | cesium.com             | 0.007687 |
| 11 | www.swissgeol.ch       | 0.007687 |
| 12 | www.swisstopo.admin.ch | 0.007540 |
| 13 | leafletjs.com          | 0.007476 |
| 14 | map.geo.admin.ch       | 0.007318 |
| 15 | www.gis-daten.ch       | 0.007198 |

## 5. HITS — Hubs and Authorities

> **Hubs** link to many authoritative sites (aggregators/directories). **Authorities** are cited by many hubs (trusted data/service endpoints).

**ES — Top Hubs:**

| #  | Site                           | Hub score |
| -- | ------------------------------ | --------- |
| 1  | www.idee.es                    | 1.000000  |
| 2  | www.mitma.gob.es               | 0.000000  |
| 3  | inspire-geoportal.ec.europa.eu | 0.000000  |
| 4  | www.mapama.gob.es              | 0.000000  |
| 5  | icearagon.aragon.es            | 0.000000  |
| 6  | catalogo.idecanarias.es        | 0.000000  |
| 7  | idecyl.jcyl.es                 | 0.000000  |
| 8  | ide.cat                        | 0.000000  |
| 9  | idena.navarra.es               | 0.000000  |
| 10 | idem.madrid.org                | 0.000000  |
| 11 | www.geo.euskadi.eus            | 0.000000  |
| 12 | ideas.asturias.es              | 0.000000  |
| 13 | mapas-gis-inter.carm.es        | 0.000000  |
| 14 | github.com                     | 0.000000  |
| 15 | csr.seadatanet.org             | 0.000000  |

**ES — Top Authorities:**

| #  | Site                           | Authority score |
| -- | ------------------------------ | --------------- |
| 1  | www.mapama.gob.es              | 1.000000        |
| 2  | www.geo.euskadi.eus            | 0.866511        |
| 3  | www.mitma.gob.es               | 0.847775        |
| 4  | idena.navarra.es               | 0.590164        |
| 5  | mapas-gis-inter.carm.es        | 0.480094        |
| 6  | icearagon.aragon.es            | 0.428571        |
| 7  | inspire-geoportal.ec.europa.eu | 0.423888        |
| 8  | catalogo.idecanarias.es        | 0.423888        |
| 9  | idecyl.jcyl.es                 | 0.423888        |
| 10 | ide.cat                        | 0.423888        |
| 11 | idem.madrid.org                | 0.423888        |
| 12 | ideas.asturias.es              | 0.423888        |
| 13 | github.com                     | 0.423888        |
| 14 | wms.mapama.gob.es              | 0.159251        |
| 15 | www.juntadeandalucia.es        | 0.124122        |

**CH — Top Hubs:**

| #  | Site                   | Hub score |
| -- | ---------------------- | --------- |
| 1  | www.geocat.ch          | 1.000000  |
| 2  | leafletjs.com          | 0.002475  |
| 3  | data.geo.admin.ch      | 0.001575  |
| 4  | cesium.com             | 0.000225  |
| 5  | info.geocat.ch         | 0.000092  |
| 6  | map.koeniz.ch          | 0.000078  |
| 7  | map.geo.admin.ch       | 0.000063  |
| 8  | survey123.arcgis.com   | 0.000033  |
| 9  | www.openstreetmap.org  | 0.000026  |
| 10 | www.swissgeol.ch       | 0.000016  |
| 11 | raster.sitg.ge.ch      | 0.000003  |
| 12 | www.geo.admin.ch       | 0.000000  |
| 13 | spatialreference.org   | 0.000000  |
| 14 | www.swisstopo.admin.ch | 0.000000  |
| 15 | viewer.swissgeol.ch    | 0.000000  |

**CH — Top Authorities:**

| #  | Site                    | Authority score |
| -- | ----------------------- | --------------- |
| 1  | info.geocat.ch          | 1.000000        |
| 2  | www.geocat.admin.ch     | 0.998177        |
| 3  | github.com              | 0.990008        |
| 4  | www.gis-daten.ch        | 0.643573        |
| 5  | geodienste.ch           | 0.405652        |
| 6  | map.geo.admin.ch        | 0.327256        |
| 7  | geobasisdaten.ch        | 0.287147        |
| 8  | geo.jura.ch             | 0.216044        |
| 9  | data.geo.admin.ch       | 0.202370        |
| 10 | geoshop.lisag.ch        | 0.201459        |
| 11 | www.jura.ch             | 0.185050        |
| 12 | maps.fr.ch              | 0.144029        |
| 13 | geoservices.jura.ch     | 0.102097        |
| 14 | wms.geo.admin.ch        | 0.099362        |
| 15 | www.meteoswiss.admin.ch | 0.094804        |

## 6. Betweenness Centrality

> Betweenness identifies **bridge nodes** — sites that lie on many shortest paths between other sites. High betweenness = structural broker or gateway in the catalog's link network.

**ES — Top 15 by betweenness:**

| #  | Site                           | Betweenness (norm.) |
| -- | ------------------------------ | ------------------- |
| 1  | www.idee.es                    | 0.000000            |
| 2  | www.mitma.gob.es               | 0.000000            |
| 3  | inspire-geoportal.ec.europa.eu | 0.000000            |
| 4  | www.mapama.gob.es              | 0.000000            |
| 5  | icearagon.aragon.es            | 0.000000            |
| 6  | catalogo.idecanarias.es        | 0.000000            |
| 7  | idecyl.jcyl.es                 | 0.000000            |
| 8  | ide.cat                        | 0.000000            |
| 9  | idena.navarra.es               | 0.000000            |
| 10 | idem.madrid.org                | 0.000000            |
| 11 | www.geo.euskadi.eus            | 0.000000            |
| 12 | ideas.asturias.es              | 0.000000            |
| 13 | mapas-gis-inter.carm.es        | 0.000000            |
| 14 | github.com                     | 0.000000            |
| 15 | csr.seadatanet.org             | 0.000000            |

**CH — Top 15 by betweenness:**

| #  | Site                   | Betweenness (norm.) |
| -- | ---------------------- | ------------------- |
| 1  | www.geocat.ch          | 0.062333            |
| 2  | data.geo.admin.ch      | 0.015186            |
| 3  | leafletjs.com          | 0.012301            |
| 4  | www.openstreetmap.org  | 0.012229            |
| 5  | www.swisstopo.admin.ch | 0.011471            |
| 6  | survey123.arcgis.com   | 0.008982            |
| 7  | map.koeniz.ch          | 0.008477            |
| 8  | cesium.com             | 0.008477            |
| 9  | viewer.swissgeol.ch    | 0.005880            |
| 10 | map.geo.admin.ch       | 0.003968            |
| 11 | www.geo.admin.ch       | 0.003752            |
| 12 | www.swissgeol.ch       | 0.001948            |
| 13 | spatialreference.org   | 0.000974            |
| 14 | raster.sitg.ge.ch      | 0.000938            |
| 15 | info.geocat.ch         | 0.000469            |

## 7. Inequality & Link Concentration

> Measures whether the catalog's link graph is dominated by a few highly-linked sites (hub-and-spoke topology) or has a more distributed structure.

**Strongest site-to-site connections — ES:**

| Source      | Target                         | Link count |
| ----------- | ------------------------------ | ---------- |
| www.idee.es | www.mapama.gob.es              | 854        |
| www.idee.es | www.geo.euskadi.eus            | 740        |
| www.idee.es | www.mitma.gob.es               | 724        |
| www.idee.es | idena.navarra.es               | 504        |
| www.idee.es | mapas-gis-inter.carm.es        | 410        |
| www.idee.es | icearagon.aragon.es            | 366        |
| www.idee.es | inspire-geoportal.ec.europa.eu | 362        |
| www.idee.es | catalogo.idecanarias.es        | 362        |
| www.idee.es | idecyl.jcyl.es                 | 362        |
| www.idee.es | ide.cat                        | 362        |
| www.idee.es | idem.madrid.org                | 362        |
| www.idee.es | ideas.asturias.es              | 362        |
| www.idee.es | github.com                     | 362        |
| www.idee.es | wms.mapama.gob.es              | 136        |
| www.idee.es | www.juntadeandalucia.es        | 106        |

**Strongest site-to-site connections — CH:**

| Source        | Target                  | Link count |
| ------------- | ----------------------- | ---------- |
| www.geocat.ch | info.geocat.ch          | 1097       |
| www.geocat.ch | www.geocat.admin.ch     | 1095       |
| www.geocat.ch | github.com              | 1086       |
| www.geocat.ch | www.gis-daten.ch        | 706        |
| www.geocat.ch | geodienste.ch           | 445        |
| www.geocat.ch | map.geo.admin.ch        | 359        |
| www.geocat.ch | geobasisdaten.ch        | 315        |
| www.geocat.ch | geo.jura.ch             | 237        |
| www.geocat.ch | data.geo.admin.ch       | 222        |
| www.geocat.ch | geoshop.lisag.ch        | 221        |
| www.geocat.ch | www.jura.ch             | 203        |
| www.geocat.ch | maps.fr.ch              | 158        |
| www.geocat.ch | geoservices.jura.ch     | 112        |
| www.geocat.ch | wms.geo.admin.ch        | 109        |
| www.geocat.ch | www.meteoswiss.admin.ch | 104        |

## 8. Crawl Coverage

> Pages actively crawled per site (depth-limited traversal). Sites with 0 pages_crawled were discovered via outgoing links but not entered.

| Metric                                    | ES      | CH     |
| ----------------------------------------- | ------- | ------ |
| Total pages crawled                       | 363     | 2697   |
| Avg pages per site                        | 5.6     | 16.1   |
| Internal link ratio (to/from seed domain) | 100.00% | 97.39% |

**Top sites by pages crawled — ES:**

| # | Site             | Pages crawled |
| - | ---------------- | ------------- |
| 1 | www.idee.es      | 362           |
| 2 | www.mitma.gob.es | 1             |

**Top sites by pages crawled — CH:**

| #  | Site                   | Pages crawled |
| -- | ---------------------- | ------------- |
| 1  | www.geocat.ch          | 2657          |
| 2  | raster.sitg.ge.ch      | 11            |
| 3  | data.geo.admin.ch      | 7             |
| 4  | map.geo.admin.ch       | 4             |
| 5  | www.geo.admin.ch       | 4             |
| 6  | viewer.swissgeol.ch    | 3             |
| 7  | info.geocat.ch         | 1             |
| 8  | wms.geo.admin.ch       | 1             |
| 9  | leafletjs.com          | 1             |
| 10 | www.openstreetmap.org  | 1             |
| 11 | spatialreference.org   | 1             |
| 12 | map.koeniz.ch          | 1             |
| 13 | survey123.arcgis.com   | 1             |
| 14 | www.swisstopo.admin.ch | 1             |
| 15 | cesium.com             | 1             |

## 9. Cross-Catalog Comparison

> Sites (domains) appearing in both catalogs indicate shared infrastructure, standards bodies, or common external resources.

| Metric                                      | Value   |
| ------------------------------------------- | ------- |
| Sites in ES only                            | 62      |
| Sites in CH only                            | 165     |
| Sites in both catalogs                      | 3       |
| Total union of sites                        | 230     |
| Jaccard similarity of site sets             | 0.0130  |
| PageRank Spearman rank corr. (shared sites) | -1.0000 |

### Sites appearing in both catalogs (by avg PageRank)

| # | Site                           | PageRank (ES) | PageRank (CH) |
| - | ------------------------------ | ------------- | ------------- |
| 1 | github.com                     | 0.015820      | 0.013239      |
| 2 | inspire-geoportal.ec.europa.eu | 0.015820      | 0.006206      |
| 3 | creativecommons.org            | 0.015351      | 0.006380      |

## 10. Interpretation

### ES (IDEE — Spain)

- **Most influential site (PageRank):** `www.mapama.gob.es` — receives the most link authority in the ES catalog network.
- **Top hub:** `www.idee.es` — best aggregator pointing to authoritative ES data sources.
- **Top authority:** `www.mapama.gob.es` — most cited data/service endpoint within ES.
- **Key bridge (betweenness):** `www.idee.es` — structural gateway connecting sub-clusters.
- Graph density 0.015385 and Gini 0.0097 indicate moderate distribution of link authority.

### CH (geocat.ch — Switzerland)

- **Most influential site (PageRank):** `www.geocat.ch` — receives the most link authority in the CH catalog network.
- **Top hub:** `www.geocat.ch` — best aggregator pointing to authoritative CH data sources.
- **Top authority:** `info.geocat.ch` — most cited data/service endpoint within CH.
- **Key bridge (betweenness):** `www.geocat.ch` — structural gateway connecting sub-clusters.
- Graph density 0.006594 and Gini 0.0784 indicate moderate distribution of link authority.

### Cross-catalog

- Jaccard similarity of site sets: **0.0130** — low overlap between the two catalog ecosystems.
- **3 shared sites** appear in both catalogs. These likely represent pan-European standards bodies, INSPIRE infrastructure, or common data services.
- PageRank rank correlation among shared sites: **-1.0000** — sites are ranked similarly.
