# Metrics Explained — Web Structure Mining of Geographic Data Catalogs

This document explains every metric computed in the analysis scripts (`compare_catalogs.py`, `analyze_graph.py`, `analyze_runs.py`) in plain language, as if you had never studied graph theory or web mining.

---

## What are we analyzing?

We crawl websites (geographic data catalogs) and record which site links to which other site. The result is a **directed graph**:

- **Node** = one website domain (e.g., `www.idee.es`, `www.geocat.ch`)
- **Edge (arrow)** = site A has pages that link to site B
- **Edge weight** = how many page-level links were found between those two sites

All metrics below describe properties of this link graph.

---

## 1. Basic Structural Metrics

### Nodes (N)
**What it is:** The total number of distinct websites (domains) found in the crawl.

**Plain meaning:** How many different sites exist in the catalog's link network. If N=64, the crawler found 64 unique domains.

---

### Edges (E)
**What it is:** The total number of directed connections between sites.

**Plain meaning:** How many site-to-site link relationships exist. If site A links to site B and site B also links to site A, that counts as 2 edges (one in each direction).

---

### Total link weight (page-level links)
**What it is:** The sum of all edge weights. Each edge weight = number of individual page-level hyperlinks found between those two sites.

**Plain meaning:** The total volume of links across the whole crawl. A site pair may have one edge but that edge might represent dozens of actual `<a href>` links across multiple pages.

---

### Total pages crawled
**What it is:** The number of individual HTML pages the crawler actually visited and read.

**Plain meaning:** How much of the web the crawler actually explored. A site with 0 pages crawled was only "seen" in a link from another page, but the crawler never entered it.

---

### Graph density
**What it is:** The ratio of actual edges to the maximum possible edges in the graph.

Formula: `density = E / (N × (N - 1))`

**Plain meaning:** How "interconnected" the graph is on a 0-to-1 scale. Density = 1 means every site links to every other site. Density ≈ 0 means sites are mostly isolated from each other. Real web graphs are very sparse (density << 0.01).

---

### Reciprocity (mutual edges)
**What it is:** The fraction of directed edges that have a matching edge in the opposite direction.

**Plain meaning:** If site A links to site B, what are the chances that B also links back to A? A value of 0.3 means 30% of link relationships are two-way. High reciprocity suggests collaborative or closely-coupled sites.

---

### Self-loops (sites linking to themselves)
**What it is:** Edges where the source and destination are the same site.

**Plain meaning:** A page on site A links to another page also on site A. This happens when a site's pages cross-reference each other. It's technically a link, but it stays "inside" the same domain.

---

### Average clustering coefficient (undirected)
**What it is:** For each node, measures how often its neighbors are also connected to each other. The average is taken over all nodes.

**Plain meaning:** If site A links to B and C, and B also links to C, those three form a "triangle" (tight cluster). A high clustering coefficient means the network has many such tight-knit groups — sites that all reference each other. A low value means the network is more like a tree (hub-and-spoke) without many triangles.

---

### Degree assortativity
**What it is:** A correlation coefficient (-1 to +1) measuring whether highly-connected nodes tend to link to other highly-connected nodes.

**Plain meaning:**
- **Positive value:** Big sites mostly link to other big sites (rich-club pattern).
- **Negative value:** Big sites mostly link to small sites (hub-and-spoke pattern, common in the Web).
- **Near zero:** No clear preference.

---

## 2. Degree Distribution

"Degree" is the number of connections a node has. In directed graphs there are two kinds.

### In-degree
**What it is:** The number of other sites that link **to** this site.

**Weighted in-degree:** Counts total link volume (many pages from one site each count).
**Unweighted in-degree:** Counts distinct domains linking in (one count per source domain, regardless of how many pages link).

**Plain meaning:** How popular a site is. High in-degree = many other sites point to it.

---

### Out-degree
**What it is:** The number of other sites this site links **to**.

**Weighted out-degree:** Total outgoing link volume.
**Unweighted out-degree:** Number of distinct destination domains.

**Plain meaning:** How "outward-looking" a site is. High out-degree = the site points its visitors to many other places.

---

### Average / Max / Variance of degree
- **Average:** Typical degree in the network.
- **Max:** The single most connected node (the hub).
- **Variance:** How spread out the degree values are. High variance = a few nodes are extremely well-connected while most have very few connections.

---

### Power-law exponent α (alpha)
**What it is:** A parameter that describes the shape of the degree distribution. Estimated using the method of Clauset et al. (maximum-likelihood).

**The math:** In a power-law distribution, the number of nodes with degree k is proportional to k^(-α).

**Plain meaning:** Real-world web graphs don't have nodes with similar degrees (like a normal bell curve). Instead, a tiny number of sites have enormous numbers of links, and most sites have just a few. This is called a "scale-free" distribution and it looks like a straight line on a log-log plot. The exponent α tells you how steep that line is:

- **α ≈ 2:** Very unequal — the top sites dominate hugely.
- **α ≈ 3:** More moderate inequality, but still heavily skewed.
- **Typical web graphs have α between 2 and 3.**

A lower α means the rich get richer faster (more extreme inequality).

---

### Gini coefficient (in-degree or PageRank)
**What it is:** A measure of inequality borrowed from economics, ranging from 0 to 1.

**Plain meaning:** Imagine sorting all sites from poorest (fewest links) to richest (most links). The Gini coefficient measures how unequal the link distribution is:

- **Gini = 0:** Every site has exactly the same number of incoming links (perfect equality).
- **Gini = 1:** One site has all the links and everyone else has none (perfect inequality).

A Gini of 0.8 means the link distribution is very skewed — a few dominant hubs. A Gini of 0.4 is more distributed.

---

### C10 / C20 (concentration)
**What it is:** The fraction of the total (links or PageRank) held by the top 10% or top 20% of sites.

**Plain meaning:** If C10 = 80%, it means the top 10% of sites receive 80% of all incoming links (or PageRank). This is a direct measure of how concentrated power is in the network — similar to the Pareto principle ("20% hold 80%").

---

## 3. Connectivity Analysis

### Weakly connected components (WCC)
**What it is:** Groups of nodes where every node can reach every other node if you ignore the direction of the arrows.

**Plain meaning:** Imagine the links as roads (either direction allowed). A WCC is a group of sites that are all reachable from each other. If the whole graph is one WCC, every site is at least indirectly connected to every other. Multiple WCCs mean some sites are completely isolated "islands."

**WCC coverage:** The percentage of all nodes that are in the largest WCC. A coverage of 95% means 95% of sites are in the main connected component.

---

### Strongly connected components (SCC)
**What it is:** Groups of nodes where every node can reach every other node following the direction of the arrows.

**Plain meaning:** A set of sites where, if you follow links (always in the direction of the arrow), you can get from any site to any other site in the group. This requires mutual pathways (not just that A→B, but that there is a path from B back to A through some chain of links). Strongly connected components are rarer and smaller than WCCs because direction matters.

**SCC coverage:** The percentage of nodes in the largest SCC.

---

### Isolates
**What it is:** Nodes with zero edges — no links in or out.

**Plain meaning:** Sites that the crawler recorded but that have no connections whatsoever to any other site in the graph. They are completely disconnected.

---

### Average shortest path length
**What it is:** The average number of hops needed to get from any node to any other node (on the undirected version of the graph).

**Plain meaning:** On average, how many "jumps" through links does it take to get from site A to site B? The famous "six degrees of separation" concept. A short average path length means the network is a "small world" — you can reach any site from any other in just a few clicks.

> **Note:** This is slow to compute for large graphs, so it is optional in the scripts (`--path-length` flag).

---

### Diameter
**What it is:** The longest of all shortest paths between any two nodes in the graph.

**Plain meaning:** In the worst case, how many hops does it take to get from one end of the network to the other? A small diameter means the network is tightly interconnected. A large diameter means there are distant corners that are hard to reach.

---

## 4. Bow-tie Decomposition (Broder et al. 2000)

A famous model of web graph structure. The Web (and by extension, catalog link graphs) tends to have a specific shape resembling a bow-tie. Every node is assigned to exactly one of these zones:

```
   ┌──────┐       ┌────────┐       ┌───────┐
   │  IN  │──────►│  CORE  │──────►│  OUT  │
   └──────┘       └────────┘       └───────┘
      │                                │
      └──[Tendrils]────────[Tendrils]──┘
                [Disconnected]
```

### CORE (giant SCC)
**What it is:** The largest strongly connected component — the "heart" of the graph.

**Plain meaning:** The group of sites that are so mutually interlinked that you can navigate from any one of them to any other by following links. This is the densely interconnected core of the catalog ecosystem.

---

### IN component
**What it is:** Nodes that can reach the CORE but are not part of it.

**Plain meaning:** Sites that link into the core (you can follow links from them and eventually reach the core) but the core doesn't link back to them. Think of these as "feeder" or "entry-point" sites — they point inward but are not themselves reachable from the core.

---

### OUT component
**What it is:** Nodes reachable from the CORE but that cannot reach the CORE.

**Plain meaning:** Sites you can reach by following links from the core, but once you're there, you can't get back to the core. These are "downstream" sites — endpoints, leaf pages, or external destinations that the core sends traffic to but doesn't receive traffic from.

---

### Tubes
**What it is:** Paths connecting IN to OUT that don't go through the CORE.

**Plain meaning:** Shortcuts that let you get from an IN-zone site to an OUT-zone site while bypassing the central core. These are relatively rare.

---

### Tendrils
**What it is:** Nodes hanging off IN or OUT that don't connect to the core in either direction.

**Plain meaning:** Dead-ends or entry-points attached to the IN/OUT wings but not connected to the core at all. They are like branches off the bow-tie wings.

---

### Disconnected
**What it is:** Nodes with no path to or from any other component.

**Plain meaning:** Completely isolated sites — not connected to anything in the bow-tie structure.

---

## 5. Centrality Metrics

### PageRank (α = 0.85)
**What it is:** An algorithm originally invented by Google (Larry Page) to rank web pages. It simulates a "random surfer" who keeps clicking links at random, and occasionally teleports to a random site (the teleportation probability is 1 - α = 0.15).

**The math:** PageRank is the stationary probability distribution of this random walk. A site gets more PageRank by being linked to by other sites that themselves have high PageRank.

**The parameter α = 0.85** (also called the damping factor) means the surfer follows a link 85% of the time and teleports 15% of the time.

**Plain meaning:** PageRank captures **global importance**. A site that is linked to by many other important sites will have a high PageRank, even if it doesn't have many raw links. Think of it as "who do the most influential people trust?" A high PageRank site is one that the rest of the network treats as authoritative.

> **Weighted PageRank:** Here we use edge weights (number of page-level links) so that a site with 50 linking pages counts more than one with just 1 linking page.

---

### HITS — Hubs and Authorities
**What it is:** Another link-analysis algorithm (Kleinberg, 1999) that assigns two scores to each site: a **hub score** and an **authority score**. These are computed simultaneously and reinforce each other.

**The idea:**
- A good **authority** is a site that many good hubs point to.
- A good **hub** is a site that points to many good authorities.

**Plain meaning:**
- **Hub score:** How good is this site as a "directory" or "aggregator" that curates links to trusted sources? Sites with high hub scores are portals that guide you to quality content.
- **Authority score:** How trusted is this site as a **source of content**? Sites with high authority scores are the destinations that hubs recommend.

A site can be both a hub and an authority, but typically hub-heavy sites (like catalog portals) and authority-heavy sites (like data service endpoints) are different.

---

### Betweenness centrality
**What it is:** For each node, the fraction of all shortest paths in the graph that pass through it. Values are normalized so the maximum possible is 1.

**Plain meaning:** Which sites are the most important **bridges** or **gateways** in the network? A site with high betweenness sits on many of the shortest routes between other pairs of sites. If it were removed, many connections would become much longer or impossible.

Think of it as the "traffic controller" or "hub airport" of the network — not necessarily the most popular destination, but the node you pass through most often when traveling across the graph.

---

## 6. Link Ecology

### Internal link ratio
**What it is:** The fraction of total link weight (page-level links) that involves the catalog's own seed domain (e.g., `www.idee.es` for Spain, `www.geocat.ch` for Switzerland).

**Plain meaning:** What proportion of the links in the graph connect back to (or originate from) the main catalog site? A high ratio means the catalog is very self-contained. A low ratio means much of the linking activity is between external sites, not the catalog itself.

---

## 7. Cross-Catalog Comparison Metrics

### Jaccard similarity (of site sets)
**What it is:** A standard measure of overlap between two sets.

Formula: `Jaccard = |A ∩ B| / |A ∪ B|`

Where A = set of sites in ES graph, B = set of sites in CH graph.

**Plain meaning:** What fraction of all discovered sites appear in **both** catalogs? A Jaccard of 0 means the two catalogs have zero overlap (totally different ecosystems). A Jaccard of 1 means they discovered exactly the same sites. In practice:
- Low (~0.01–0.05): The catalogs operate in mostly separate site ecosystems.
- Moderate (~0.05–0.15): Some shared infrastructure (e.g., European INSPIRE nodes).
- High (>0.15): Strong overlap — common platforms, standards bodies, shared services.

---

### PageRank Spearman rank correlation (on shared sites)
**What it is:** A correlation measure (ranging from -1 to +1) that compares the *ranking* of shared sites by their PageRank in each catalog.

**The method:** For sites that appear in both ES and CH graphs, compute their PageRank in each graph, rank them (1st, 2nd, 3rd...) within each graph, then measure how similar the two rankings are using Spearman's formula.

**Plain meaning:** For the sites that both catalogs link to, do both catalogs agree on which ones are most important?

- **Close to +1:** Both catalogs rank the shared sites in nearly the same order — similar perception of importance.
- **Close to 0:** No agreement — Spain might consider site X very important but Switzerland doesn't (or vice versa).
- **Close to -1:** The rankings are completely reversed (highly unusual in practice).

---

## Summary Table

| Metric | Category | What it tells you |
|--------|----------|-------------------|
| N, E | Structure | How big is the graph |
| Density | Structure | How interconnected (sparse vs. dense) |
| Reciprocity | Structure | How often links are mutual |
| Self-loops | Structure | Sites that link to themselves |
| Clustering coeff. | Structure | How many triangles / tight groups |
| Assortativity | Structure | Do big nodes link to big nodes? |
| In-degree | Degree | How popular/cited a site is |
| Out-degree | Degree | How many sites a site points to |
| Power-law α | Degree | Shape of the inequality distribution |
| Gini (in-deg/PR) | Inequality | How concentrated links/importance is |
| C10/C20 | Inequality | Top 10–20% hold what fraction of links |
| WCC | Connectivity | Reachability ignoring direction |
| SCC | Connectivity | Reachability following direction |
| Bow-tie CORE | Connectivity | Densely mutually linked central sites |
| Bow-tie IN/OUT | Connectivity | Feeder and downstream sites |
| Avg path length | Connectivity | Avg hops between any two sites |
| Diameter | Connectivity | Longest shortest path |
| PageRank | Centrality | Global link-authority / influence |
| HITS hubs | Centrality | Best aggregator/directory sites |
| HITS authorities | Centrality | Most trusted content endpoints |
| Betweenness | Centrality | Key bridges / structural gateways |
| Internal link ratio | Link ecology | How self-contained the catalog is |
| Jaccard | Cross-catalog | Overlap between the two catalog site sets |
| Spearman rank corr. | Cross-catalog | Agreement on which shared sites matter |
