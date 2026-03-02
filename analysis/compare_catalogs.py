"""
Web Structure Mining — Cross-Catalog Comparison
================================================
Compares the site-level link graphs of two geographic data catalogs:
  - Spain IDEE  (idee.es)    → Neo4j on bolt://localhost:7687
  - Switzerland geocat.ch   → Neo4j on bolt://localhost:7688

Default run IDs (latest site-level crawls from Feb 23, 2026):
  ES: sites-es-2026-02-23   (361 seed pages)
  CH: sites-ch-2026-02-23   (1141 seed pages)

Metrics computed (WSM perspective):
  Structural      — N, E, density, reciprocity, self-loops
  Degree          — avg/max/variance of in/out-degree, power-law α
  Connectivity    — WCC, SCC, bow-tie decomposition (Broder et al.)
  Centrality      — PageRank, HITS (hubs/authorities), betweenness
  Inequality      — Gini coefficient (PageRank & in-degree), C10 link concentration
  Assortativity   — degree-degree correlation
  Link ecology    — internal vs external links, crawl coverage
  Cross-catalog   — Jaccard overlap, shared sites, PageRank rank correlation

Usage:
  python analysis/compare_catalogs.py [options]

Options:
  --es-run-id    Run ID for ES Neo4j  (default: sites-es-2026-02-23)
  --ch-run-id    Run ID for CH Neo4j  (default: sites-ch-2026-02-23)
  --es-port      Bolt port for ES     (default: 7687)
  --ch-port      Bolt port for CH     (default: 7688)
  --no-report    Print to console only, skip writing markdown file
  --output       Output path for markdown report
                 (default: analysis/catalog_comparison_<timestamp>.md)
  --top          Number of top nodes to show in rankings (default: 15)
  --exclude-es   Space-separated site labels to exclude from ES graph
  --exclude-ch   Space-separated site labels to exclude from CH graph
  --path-length  Compute average path length (slow for large graphs, default: off)
"""

import argparse
import math
import os
import sys
from datetime import datetime

import networkx as nx
import numpy as np

try:
    from neo4j import GraphDatabase
except ImportError:
    print("ERROR: neo4j package not found. Install with: pip install neo4j")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
AUTH = ("neo4j", "tesis_password")

# ── Defaults ────────────────────────────────────────────────────────────────

DEFAULT_ES_RUN = "sites-es-2026-02-23"
DEFAULT_CH_RUN = "sites-ch-2026-02-23"
DEFAULT_ES_PORT = 7687
DEFAULT_CH_PORT = 7688

# Catalog seed domains (for link ecology analysis)
CATALOG_DOMAINS = {
    "es": {"www.idee.es", "idee.es"},
    "ch": {"www.geocat.ch", "geocat.ch"},
}


# ── Neo4j data fetching ──────────────────────────────────────────────────────

def connect(port, label):
    uri = f"bolt://localhost:{port}"
    try:
        drv = GraphDatabase.driver(uri, auth=AUTH)
        drv.verify_connectivity()
        return drv
    except Exception as e:
        print(f"ERROR: Cannot connect to {label} Neo4j at {uri}: {e}")
        sys.exit(1)


def fetch_graph(driver, run_id, exclude=None):
    """Fetch Site nodes + LINKS_TO edges for a run from Neo4j."""
    exclude = set(exclude or [])
    with driver.session() as s:
        nodes = s.run(
            "MATCH (n:Site {run_id: $rid}) "
            "RETURN n.label AS label, n.url AS url, n.pages_crawled AS pages_crawled",
            rid=run_id,
        ).data()
        edges = s.run(
            "MATCH (a:Site {run_id: $rid})-[r:LINKS_TO {run_id: $rid}]->(b:Site {run_id: $rid}) "
            "RETURN a.label AS src, b.label AS dst, r.link_count AS weight",
            rid=run_id,
        ).data()
    nodes = [n for n in nodes if n["label"] not in exclude]
    labels = {n["label"] for n in nodes}
    edges = [e for e in edges if e["src"] in labels and e["dst"] in labels]
    return nodes, edges


def build_graph(nodes, edges):
    G = nx.DiGraph()
    for n in nodes:
        G.add_node(n["label"], url=n.get("url", ""), pages_crawled=n.get("pages_crawled") or 0)
    for e in edges:
        G.add_edge(e["src"], e["dst"], weight=e.get("weight") or 1)
    return G


# ── Statistical helpers ──────────────────────────────────────────────────────

def gini(values):
    """Gini coefficient of a list of non-negative values."""
    arr = np.array(sorted(values), dtype=float)
    if arr.sum() == 0 or len(arr) == 0:
        return 0.0
    n = len(arr)
    cumsum = np.cumsum(arr)
    return float((2 * np.sum((np.arange(1, n + 1)) * arr) / (n * cumsum[-1])) - (n + 1) / n)


def concentration(values_dict, top_frac=0.1):
    """
    C_top_frac: fraction of total value held by top (top_frac * 100)% of nodes.
    E.g. top 10% of sites hold X% of total PageRank mass.
    """
    vals = sorted(values_dict.values(), reverse=True)
    if not vals or sum(vals) == 0:
        return 0.0
    k = max(1, int(math.ceil(len(vals) * top_frac)))
    return sum(vals[:k]) / sum(vals)


def power_law_alpha(degree_seq):
    """
    Estimate power-law exponent α using maximum-likelihood (Clauset et al.):
      α ≈ 1 + n * (Σ ln(x_i / x_min))^{-1}
    Only uses values >= x_min=1 (excluding zero-degree nodes).
    Returns (alpha, x_min, n_tail).
    """
    seq = [d for d in degree_seq if d >= 1]
    if len(seq) < 5:
        return None, None, 0
    x_min = 1
    n = len(seq)
    log_sum = sum(math.log(x / x_min) for x in seq)
    if log_sum == 0:
        return None, None, 0
    alpha = 1 + n / log_sum
    return round(alpha, 3), x_min, n


def reciprocity(G):
    """Fraction of directed edges (u→v) that have a reverse (v→u)."""
    edges = set(G.edges())
    if not edges:
        return 0.0
    mutual = sum(1 for u, v in edges if (v, u) in edges)
    return mutual / len(edges)


# ── PageRank (custom power-iteration for robustness) ────────────────────────

def pagerank(G, alpha=0.85, max_iter=300, tol=1e-10):
    nodes = list(G.nodes())
    n = len(nodes)
    if n == 0:
        return {}
    idx = {v: i for i, v in enumerate(nodes)}
    W = np.zeros((n, n), dtype=float)
    for u, v, d in G.edges(data=True):
        W[idx[v], idx[u]] += d.get("weight", 1)
    col_sum = W.sum(axis=0)
    dangling = col_sum == 0
    col_sum[dangling] = 1
    W /= col_sum
    r = np.full(n, 1.0 / n)
    d_w = np.full(n, 1.0 / n)
    for _ in range(max_iter):
        r_new = alpha * (W @ r + d_w * (dangling @ r)) + (1 - alpha) / n
        if np.linalg.norm(r_new - r, 1) < tol:
            r = r_new
            break
        r = r_new
    return {nodes[i]: float(r[i]) for i in range(n)}


# ── HITS (power-iteration) ────────────────────────────────────────────────────

def hits(G, max_iter=500, tol=1e-9):
    """Returns (converged, hubs_dict, authorities_dict)."""
    nodes = list(G.nodes())
    n = len(nodes)
    if n == 0:
        return True, {}, {}
    idx = {v: i for i, v in enumerate(nodes)}
    A = np.zeros((n, n), dtype=float)
    for u, v, d in G.edges(data=True):
        A[idx[u], idx[v]] += d.get("weight", 1)
    h = np.ones(n)
    for _ in range(max_iter):
        a_new = A.T @ h
        h_new = A @ a_new
        a_norm = np.linalg.norm(a_new)
        h_norm = np.linalg.norm(h_new)
        a_new = a_new / a_norm if a_norm else a_new
        h_new = h_new / h_norm if h_norm else h_new
        if np.linalg.norm(h_new - h, 1) < tol:
            h, a = h_new, a_new
            break
        h, a = h_new, a_new
    else:
        return False, {}, {}
    h = h / h.max() if h.max() else h
    a = a / a.max() if a.max() else a
    return True, {nodes[i]: float(h[i]) for i in range(n)}, {nodes[i]: float(a[i]) for i in range(n)}


# ── Bow-tie decomposition (Broder et al. 2000) ───────────────────────────────

def bow_tie(G):
    """
    Classic bow-tie decomposition of a directed web graph.
    Returns dict with keys: core, in_comp, out_comp, tubes, tendrils, disconnected.
    'core' = nodes in the giant SCC.
    'in_comp' = nodes that can reach core but are not in it.
    'out_comp' = nodes reachable from core but not in it.
    'tubes' = paths from IN to OUT not through CORE.
    'tendrils' = nodes hanging off IN or OUT, not reaching core.
    'disconnected' = nodes with no path to/from any of the above.
    """
    if G.number_of_nodes() == 0:
        return {k: set() for k in ("core", "in_comp", "out_comp", "tubes", "tendrils", "disconnected")}

    sccs = list(nx.strongly_connected_components(G))
    giant_scc = max(sccs, key=len)

    # CORE
    core = giant_scc

    # IN: can reach core (predecessors) but not in core
    core_node = next(iter(core))
    reachable_to_core = set(nx.ancestors(G, core_node)) | {core_node}
    # More precisely: all nodes that have a path to *any* core node
    # Approximate via reverse graph reachability from core set
    G_rev = G.reverse(copy=False)
    reachable_from_core_rev = set()
    for c in core:
        reachable_from_core_rev |= nx.descendants(G_rev, c)
    in_comp = (reachable_from_core_rev - core)

    # OUT: reachable from core but not in core
    reachable_from_core = set()
    for c in core:
        reachable_from_core |= nx.descendants(G, c)
    out_comp = reachable_from_core - core

    # Tubes: IN → OUT paths not through CORE (nodes in both in_comp's forward and out_comp's backward reach)
    # Approximation: nodes that are reachable from IN and can reach OUT, not in CORE, IN, or OUT
    in_out_union = core | in_comp | out_comp

    # Tendrils + disconnected: everything else
    rest = set(G.nodes()) - in_out_union

    # Among rest: nodes reachable from IN (tendrils hanging off IN)
    tendril_from_in = set()
    for node in in_comp:
        fwd = nx.descendants(G, node)
        tendril_from_in |= fwd & rest

    # Among rest: nodes that can reach OUT (tendrils flowing to OUT)
    tendril_to_out = set()
    for node in out_comp:
        bwd = nx.ancestors(G, node)
        tendril_to_out |= bwd & rest

    tendrils = (tendril_from_in | tendril_to_out)
    disconnected = rest - tendrils

    return {
        "core": core,
        "in_comp": in_comp,
        "out_comp": out_comp,
        "tendrils": tendrils,
        "disconnected": disconnected,
    }


# ── Per-catalog analysis ──────────────────────────────────────────────────────

def analyze_catalog(name, G, catalog_domains, args):
    """Full WSM analysis for one catalog graph. Returns a results dict."""
    print(f"\n[{name}] Analyzing graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    res = {"name": name}

    n = G.number_of_nodes()
    e = G.number_of_edges()
    res["N"] = n
    res["E"] = e
    res["density"] = nx.density(G) if n > 1 else 0.0

    # ── Degree stats ─────────────────────────────────────────────────────────
    in_deg  = dict(G.in_degree(weight="weight"))
    out_deg = dict(G.out_degree(weight="weight"))
    in_deg_uw  = dict(G.in_degree())
    out_deg_uw = dict(G.out_degree())

    in_vals  = list(in_deg.values())
    out_vals = list(out_deg.values())
    in_uw    = list(in_deg_uw.values())
    out_uw   = list(out_deg_uw.values())

    res["avg_in_deg_w"]  = float(np.mean(in_vals))  if in_vals  else 0.0
    res["avg_out_deg_w"] = float(np.mean(out_vals)) if out_vals else 0.0
    res["max_in_deg_w"]  = max(in_vals)  if in_vals  else 0
    res["max_out_deg_w"] = max(out_vals) if out_vals else 0
    res["var_in_deg_w"]  = float(np.var(in_vals))   if in_vals  else 0.0
    res["avg_in_deg_uw"]  = float(np.mean(in_uw))  if in_uw  else 0.0
    res["avg_out_deg_uw"] = float(np.mean(out_uw)) if out_uw else 0.0
    res["max_in_deg_uw"]  = max(in_uw)  if in_uw  else 0
    res["max_out_deg_uw"] = max(out_uw) if out_uw else 0

    # Power-law α on unweighted in-degree (WSM: web in-degree follows power law)
    alpha, xmin, n_tail = power_law_alpha(in_uw)
    res["in_deg_powerlaw_alpha"] = alpha
    res["in_deg_powerlaw_n"]     = n_tail

    res["top_in_deg_w"]  = sorted(in_deg.items(),    key=lambda x: x[1], reverse=True)[:args.top]
    res["top_out_deg_w"] = sorted(out_deg.items(),   key=lambda x: x[1], reverse=True)[:args.top]
    res["top_in_deg_uw"] = sorted(in_deg_uw.items(), key=lambda x: x[1], reverse=True)[:args.top]

    # ── Reciprocity & self-loops ──────────────────────────────────────────────
    res["reciprocity"] = reciprocity(G)
    self_loops = [(u, d["weight"]) for u, v, d in G.edges(data=True) if u == v]
    res["self_loops"] = len(self_loops)
    res["self_loop_ratio"] = len(self_loops) / e if e else 0.0
    res["top_self_loops"] = sorted(self_loops, key=lambda x: x[1], reverse=True)[:5]

    # ── Connectivity ─────────────────────────────────────────────────────────
    wcc = sorted(nx.weakly_connected_components(G), key=len, reverse=True)
    scc = sorted(nx.strongly_connected_components(G), key=len, reverse=True)
    res["n_wcc"] = len(wcc)
    res["largest_wcc"] = len(wcc[0]) if wcc else 0
    res["wcc_coverage"] = len(wcc[0]) / n if wcc and n else 0.0
    res["n_scc"] = len(scc)
    res["largest_scc"] = len(scc[0]) if scc else 0
    res["scc_coverage"] = len(scc[0]) / n if scc and n else 0.0
    res["n_isolates"] = len(list(nx.isolates(G)))

    # ── Bow-tie ──────────────────────────────────────────────────────────────
    print(f"[{name}] Computing bow-tie decomposition...")
    bt = bow_tie(G)
    res["bowtie"] = {k: len(v) for k, v in bt.items()}

    # ── Average path length (optional, on largest WCC) ───────────────────────
    res["avg_path_length"] = None
    res["diameter"] = None
    if args.path_length and wcc:
        wcc_subgraph = G.subgraph(wcc[0]).copy()
        wcc_und = wcc_subgraph.to_undirected()
        print(f"[{name}] Computing average shortest path length on largest WCC ({len(wcc[0])} nodes)...")
        try:
            res["avg_path_length"] = round(nx.average_shortest_path_length(wcc_und), 4)
            res["diameter"] = nx.diameter(wcc_und)
        except Exception as ex:
            print(f"[{name}] Path length computation failed: {ex}")

    # ── Clustering ────────────────────────────────────────────────────────────
    G_und = G.to_undirected()
    res["avg_clustering"] = round(nx.average_clustering(G_und), 6)

    # ── Assortativity (degree-degree correlation) ─────────────────────────────
    try:
        res["assortativity"] = round(nx.degree_assortativity_coefficient(G), 6)
    except Exception:
        res["assortativity"] = None

    # ── PageRank ─────────────────────────────────────────────────────────────
    print(f"[{name}] Computing PageRank...")
    pr = pagerank(G, alpha=0.85)
    res["pagerank"] = pr
    res["top_pagerank"] = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:args.top]
    res["gini_pagerank"] = gini(list(pr.values()))
    res["c10_pagerank"] = concentration(pr, 0.10)   # top 10% of sites
    res["c20_pagerank"] = concentration(pr, 0.20)

    # ── HITS ─────────────────────────────────────────────────────────────────
    print(f"[{name}] Computing HITS...")
    hits_ok, hubs, auths = hits(G)
    res["hits_ok"] = hits_ok
    res["hubs"] = hubs
    res["authorities"] = auths
    if hits_ok:
        res["top_hubs"]   = sorted(hubs.items(),  key=lambda x: x[1], reverse=True)[:args.top]
        res["top_auths"]  = sorted(auths.items(), key=lambda x: x[1], reverse=True)[:args.top]
    else:
        res["top_hubs"] = []
        res["top_auths"] = []

    # ── Betweenness centrality ────────────────────────────────────────────────
    print(f"[{name}] Computing betweenness centrality...")
    bc = nx.betweenness_centrality(G, normalized=True, weight="weight")
    res["top_betweenness"] = sorted(bc.items(), key=lambda x: x[1], reverse=True)[:args.top]

    # ── Gini of in-degree ────────────────────────────────────────────────────
    res["gini_in_deg"] = gini(in_uw)
    res["c10_in_deg"]  = concentration(in_deg_uw, 0.10)

    # ── Link ecology: internal vs external ───────────────────────────────────
    total_w = sum(d["weight"] for _, _, d in G.edges(data=True))
    internal_w = sum(
        d["weight"] for u, v, d in G.edges(data=True)
        if u in catalog_domains or v in catalog_domains
    )
    res["total_link_weight"] = total_w
    res["internal_link_ratio"] = internal_w / total_w if total_w else 0.0

    # ── Crawl coverage ───────────────────────────────────────────────────────
    pages = [d["pages_crawled"] for _, d in G.nodes(data=True)]
    res["total_pages_crawled"] = sum(pages)
    res["avg_pages_per_site"]  = float(np.mean(pages)) if pages else 0.0
    res["max_pages_site"] = max(
        G.nodes(data=True), key=lambda x: x[1]["pages_crawled"], default=(None, {"pages_crawled": 0})
    )
    crawled_sites = [(n, d["pages_crawled"]) for n, d in G.nodes(data=True) if d["pages_crawled"] > 0]
    res["top_crawled"] = sorted(crawled_sites, key=lambda x: x[1], reverse=True)[:args.top]

    # ── Strongest edges ───────────────────────────────────────────────────────
    res["top_edges"] = sorted(
        [(u, v, d["weight"]) for u, v, d in G.edges(data=True)],
        key=lambda x: x[2], reverse=True
    )[:args.top]

    return res


# ── Cross-catalog comparison ─────────────────────────────────────────────────

def cross_compare(r_es, r_ch):
    """Compute cross-catalog metrics between ES and CH results."""
    es_sites = set(r_es["pagerank"].keys())
    ch_sites = set(r_ch["pagerank"].keys())
    common = es_sites & ch_sites
    union  = es_sites | ch_sites
    jaccard = len(common) / len(union) if union else 0.0

    # PageRank rank correlation on shared sites
    rank_corr = None
    if len(common) >= 3:
        es_pr = {s: r_es["pagerank"][s] for s in common}
        ch_pr = {s: r_ch["pagerank"][s] for s in common}
        sites_sorted = sorted(common)
        es_ranks = np.argsort([-es_pr[s] for s in sites_sorted])
        ch_ranks = np.argsort([-ch_pr[s] for s in sites_sorted])
        # Spearman rank correlation
        n = len(sites_sorted)
        d2 = sum((int(er) - int(cr)) ** 2 for er, cr in zip(es_ranks, ch_ranks))
        rank_corr = 1 - 6 * d2 / (n * (n ** 2 - 1)) if n > 1 else 0.0

    # Common sites: top-N by combined average PageRank
    common_pr = sorted(
        [(s, r_es["pagerank"].get(s, 0), r_ch["pagerank"].get(s, 0)) for s in common],
        key=lambda x: (x[1] + x[2]) / 2, reverse=True
    )

    return {
        "es_only": len(es_sites - ch_sites),
        "ch_only": len(ch_sites - es_sites),
        "common":  len(common),
        "union":   len(union),
        "jaccard": jaccard,
        "rank_corr": rank_corr,
        "common_sites_pr": common_pr[:20],
    }


# ── Markdown report builder ───────────────────────────────────────────────────

def tbl(headers, rows, widths=None):
    """Simple markdown table."""
    if widths is None:
        widths = [max(len(str(r[i])) for r in ([headers] + list(rows))) for i in range(len(headers))]
    sep = "| " + " | ".join("-" * w for w in widths) + " |"
    hdr = "| " + " | ".join(str(headers[i]).ljust(widths[i]) for i in range(len(headers))) + " |"
    body = "\n".join(
        "| " + " | ".join(str(row[i]).ljust(widths[i]) for i in range(len(headers))) + " |"
        for row in rows
    )
    return "\n".join([hdr, sep, body])


def build_report(r_es, r_ch, cross, args):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    a = lines.append

    a(f"# Web Structure Mining — Catalog Comparison")
    a(f"")
    a(f"Generated: {now}")
    a(f"")
    a(f"| | ES (IDEE) | CH (geocat.ch) |")
    a(f"|---|---|---|")
    a(f"| Run ID | `{args.es_run_id}` | `{args.ch_run_id}` |")
    a(f"| Neo4j port | {args.es_port} | {args.ch_port} |")
    a(f"")

    # ── Section 1: Structural Overview ───────────────────────────────────────
    a(f"## 1. Structural Overview")
    a(f"")
    a(f"> Site-level graph: one node per domain, weighted directed edges = page-level link counts between domains.")
    a(f"")

    scalar_rows = [
        ("Nodes (sites)",                        r_es["N"],                        r_ch["N"]),
        ("Edges (site-to-site links)",            r_es["E"],                        r_ch["E"]),
        ("Total link weight (page-level links)",  f"{r_es['total_link_weight']:,}", f"{r_ch['total_link_weight']:,}"),
        ("Total pages crawled",                   r_es["total_pages_crawled"],      r_ch["total_pages_crawled"]),
        ("Graph density",                         f"{r_es['density']:.6f}",         f"{r_ch['density']:.6f}"),
        ("Reciprocity (mutual edges)",            f"{r_es['reciprocity']:.4f}",     f"{r_ch['reciprocity']:.4f}"),
        ("Self-loops (sites → themselves)",       r_es["self_loops"],               r_ch["self_loops"]),
        ("Avg clustering coeff (undirected)",     f"{r_es['avg_clustering']:.6f}",  f"{r_ch['avg_clustering']:.6f}"),
        ("Degree assortativity",                  f"{r_es['assortativity']}"        if r_es["assortativity"] is not None else "N/A",
                                                  f"{r_ch['assortativity']}"        if r_ch["assortativity"] is not None else "N/A"),
    ]
    a(tbl(["Metric", "ES", "CH"], scalar_rows))
    a(f"")

    # ── Section 2: Degree Distribution ───────────────────────────────────────
    a(f"## 2. Degree Distribution")
    a(f"")
    a(f"> Web graphs (Barabási-Albert model) typically show power-law in-degree distributions (α ≈ 2–3). "
      f"Power-law α estimated by maximum-likelihood (Clauset et al.).")
    a(f"")

    deg_rows = [
        ("Avg weighted in-degree",    f"{r_es['avg_in_deg_w']:.2f}",  f"{r_ch['avg_in_deg_w']:.2f}"),
        ("Avg weighted out-degree",   f"{r_es['avg_out_deg_w']:.2f}", f"{r_ch['avg_out_deg_w']:.2f}"),
        ("Max weighted in-degree",    r_es["max_in_deg_w"],           r_ch["max_in_deg_w"]),
        ("Max weighted out-degree",   r_es["max_out_deg_w"],          r_ch["max_out_deg_w"]),
        ("Avg unweighted in-degree",  f"{r_es['avg_in_deg_uw']:.2f}", f"{r_ch['avg_in_deg_uw']:.2f}"),
        ("Max unweighted in-degree",  r_es["max_in_deg_uw"],          r_ch["max_in_deg_uw"]),
        ("Max unweighted out-degree", r_es["max_out_deg_uw"],         r_ch["max_out_deg_uw"]),
        ("In-degree variance (w)",    f"{r_es['var_in_deg_w']:.2f}",  f"{r_ch['var_in_deg_w']:.2f}"),
        ("Power-law α (unweighted in-degree)",
         f"{r_es['in_deg_powerlaw_alpha']}" if r_es["in_deg_powerlaw_alpha"] else "N/A",
         f"{r_ch['in_deg_powerlaw_alpha']}" if r_ch["in_deg_powerlaw_alpha"] else "N/A"),
        ("Gini (in-degree)",          f"{r_es['gini_in_deg']:.4f}",   f"{r_ch['gini_in_deg']:.4f}"),
        ("C10 in-degree (top 10% hold)",f"{r_es['c10_in_deg']:.2%}",  f"{r_ch['c10_in_deg']:.2%}"),
    ]
    a(tbl(["Metric", "ES", "CH"], deg_rows))
    a(f"")

    # Top by in-degree
    a(f"### Top sites by weighted in-degree")
    a(f"")
    a(f"**ES:**")
    a(f"")
    a(tbl(["#", "Site", "Incoming link weight"],
          [(i+1, s, w) for i, (s, w) in enumerate(r_es["top_in_deg_w"])]))
    a(f"")
    a(f"**CH:**")
    a(f"")
    a(tbl(["#", "Site", "Incoming link weight"],
          [(i+1, s, w) for i, (s, w) in enumerate(r_ch["top_in_deg_w"])]))
    a(f"")

    # Top by out-degree
    a(f"### Top sites by weighted out-degree")
    a(f"")
    a(f"**ES:**")
    a(f"")
    a(tbl(["#", "Site", "Outgoing link weight"],
          [(i+1, s, w) for i, (s, w) in enumerate(r_es["top_out_deg_w"])]))
    a(f"")
    a(f"**CH:**")
    a(f"")
    a(tbl(["#", "Site", "Outgoing link weight"],
          [(i+1, s, w) for i, (s, w) in enumerate(r_ch["top_out_deg_w"])]))
    a(f"")

    # ── Section 3: Connectivity ───────────────────────────────────────────────
    a(f"## 3. Connectivity Analysis")
    a(f"")
    a(f"> The **bow-tie model** (Broder et al. 2000) partitions the Web into: CORE (giant SCC), "
      f"IN (nodes reaching the core), OUT (nodes reachable from core), tendrils, and disconnected nodes.")
    a(f"")

    conn_rows = [
        ("Isolates (no edges)",                   r_es["n_isolates"],       r_ch["n_isolates"]),
        ("Weakly connected components (WCC)",      r_es["n_wcc"],            r_ch["n_wcc"]),
        ("Largest WCC (nodes)",                    r_es["largest_wcc"],      r_ch["largest_wcc"]),
        ("WCC coverage (% of nodes)",              f"{r_es['wcc_coverage']:.1%}", f"{r_ch['wcc_coverage']:.1%}"),
        ("Strongly connected components (SCC)",    r_es["n_scc"],            r_ch["n_scc"]),
        ("Largest SCC / CORE (nodes)",             r_es["largest_scc"],      r_ch["largest_scc"]),
        ("SCC coverage (% of nodes)",              f"{r_es['scc_coverage']:.1%}", f"{r_ch['scc_coverage']:.1%}"),
    ]
    a(tbl(["Metric", "ES", "CH"], conn_rows))
    a(f"")

    # Bow-tie
    a(f"### Bow-tie decomposition")
    a(f"")
    bt_rows = [
        ("CORE (giant SCC)",    r_es["bowtie"]["core"],         r_ch["bowtie"]["core"]),
        ("IN component",        r_es["bowtie"]["in_comp"],      r_ch["bowtie"]["in_comp"]),
        ("OUT component",       r_es["bowtie"]["out_comp"],     r_ch["bowtie"]["out_comp"]),
        ("Tendrils",            r_es["bowtie"]["tendrils"],     r_ch["bowtie"]["tendrils"]),
        ("Disconnected",        r_es["bowtie"]["disconnected"], r_ch["bowtie"]["disconnected"]),
    ]
    a(tbl(["Component", "ES nodes", "CH nodes"], bt_rows))
    a(f"")

    if args.path_length:
        pl_rows = [
            ("Average shortest path length", r_es["avg_path_length"] or "N/A", r_ch["avg_path_length"] or "N/A"),
            ("Diameter",                     r_es["diameter"] or "N/A",         r_ch["diameter"] or "N/A"),
        ]
        a(tbl(["Metric", "ES", "CH"], pl_rows))
        a(f"")

    # ── Section 4: PageRank ───────────────────────────────────────────────────
    a(f"## 4. PageRank (α = 0.85, weighted)")
    a(f"")
    a(f"> PageRank models a random web surfer following links. Higher score = more globally influential site. "
      f"Gini measures inequality of influence distribution (1 = all influence concentrated in one node).")
    a(f"")

    pr_meta_rows = [
        ("Gini coefficient (PageRank)",          f"{r_es['gini_pagerank']:.4f}",  f"{r_ch['gini_pagerank']:.4f}"),
        ("C10: top 10% sites hold",              f"{r_es['c10_pagerank']:.2%}",   f"{r_ch['c10_pagerank']:.2%}"),
        ("C20: top 20% sites hold",              f"{r_es['c20_pagerank']:.2%}",   f"{r_ch['c20_pagerank']:.2%}"),
    ]
    a(tbl(["Metric", "ES", "CH"], pr_meta_rows))
    a(f"")

    a(f"**Top {args.top} by PageRank — ES:**")
    a(f"")
    a(tbl(["#", "Site", "PageRank"],
          [(i+1, s, f"{v:.6f}") for i, (s, v) in enumerate(r_es["top_pagerank"])]))
    a(f"")
    a(f"**Top {args.top} by PageRank — CH:**")
    a(f"")
    a(tbl(["#", "Site", "PageRank"],
          [(i+1, s, f"{v:.6f}") for i, (s, v) in enumerate(r_ch["top_pagerank"])]))
    a(f"")

    # ── Section 5: HITS ───────────────────────────────────────────────────────
    a(f"## 5. HITS — Hubs and Authorities")
    a(f"")
    a(f"> **Hubs** link to many authoritative sites (aggregators/directories). "
      f"**Authorities** are cited by many hubs (trusted data/service endpoints).")
    a(f"")

    for tag, r in [("ES", r_es), ("CH", r_ch)]:
        if r["hits_ok"]:
            a(f"**{tag} — Top Hubs:**")
            a(f"")
            a(tbl(["#", "Site", "Hub score"],
                  [(i+1, s, f"{v:.6f}") for i, (s, v) in enumerate(r["top_hubs"])]))
            a(f"")
            a(f"**{tag} — Top Authorities:**")
            a(f"")
            a(tbl(["#", "Site", "Authority score"],
                  [(i+1, s, f"{v:.6f}") for i, (s, v) in enumerate(r["top_auths"])]))
            a(f"")
        else:
            a(f"**{tag}:** HITS did not converge.")
            a(f"")

    # ── Section 6: Betweenness Centrality ─────────────────────────────────────
    a(f"## 6. Betweenness Centrality")
    a(f"")
    a(f"> Betweenness identifies **bridge nodes** — sites that lie on many shortest paths between other sites. "
      f"High betweenness = structural broker or gateway in the catalog's link network.")
    a(f"")

    a(f"**ES — Top {args.top} by betweenness:**")
    a(f"")
    a(tbl(["#", "Site", "Betweenness (norm.)"],
          [(i+1, s, f"{v:.6f}") for i, (s, v) in enumerate(r_es["top_betweenness"])]))
    a(f"")
    a(f"**CH — Top {args.top} by betweenness:**")
    a(f"")
    a(tbl(["#", "Site", "Betweenness (norm.)"],
          [(i+1, s, f"{v:.6f}") for i, (s, v) in enumerate(r_ch["top_betweenness"])]))
    a(f"")

    # ── Section 7: Inequality & Link Concentration ────────────────────────────
    a(f"## 7. Inequality & Link Concentration")
    a(f"")
    a(f"> Measures whether the catalog's link graph is dominated by a few highly-linked sites "
      f"(hub-and-spoke topology) or has a more distributed structure.")
    a(f"")

    a(f"**Strongest site-to-site connections — ES:**")
    a(f"")
    a(tbl(["Source", "Target", "Link count"],
          [(u, v, w) for u, v, w in r_es["top_edges"]]))
    a(f"")
    a(f"**Strongest site-to-site connections — CH:**")
    a(f"")
    a(tbl(["Source", "Target", "Link count"],
          [(u, v, w) for u, v, w in r_ch["top_edges"]]))
    a(f"")

    # ── Section 8: Crawl Coverage ─────────────────────────────────────────────
    a(f"## 8. Crawl Coverage")
    a(f"")
    a(f"> Pages actively crawled per site (depth-limited traversal). "
      f"Sites with 0 pages_crawled were discovered via outgoing links but not entered.")
    a(f"")

    cov_rows = [
        ("Total pages crawled",            r_es["total_pages_crawled"],       r_ch["total_pages_crawled"]),
        ("Avg pages per site",             f"{r_es['avg_pages_per_site']:.1f}", f"{r_ch['avg_pages_per_site']:.1f}"),
        ("Internal link ratio (to/from seed domain)",
         f"{r_es['internal_link_ratio']:.2%}", f"{r_ch['internal_link_ratio']:.2%}"),
    ]
    a(tbl(["Metric", "ES", "CH"], cov_rows))
    a(f"")

    a(f"**Top sites by pages crawled — ES:**")
    a(f"")
    a(tbl(["#", "Site", "Pages crawled"],
          [(i+1, s, p) for i, (s, p) in enumerate(r_es["top_crawled"])]))
    a(f"")
    a(f"**Top sites by pages crawled — CH:**")
    a(f"")
    a(tbl(["#", "Site", "Pages crawled"],
          [(i+1, s, p) for i, (s, p) in enumerate(r_ch["top_crawled"])]))
    a(f"")

    # ── Section 9: Cross-Catalog Comparison ──────────────────────────────────
    a(f"## 9. Cross-Catalog Comparison")
    a(f"")
    a(f"> Sites (domains) appearing in both catalogs indicate shared infrastructure, "
      f"standards bodies, or common external resources.")
    a(f"")

    cc_rows = [
        ("Sites in ES only",            cross["es_only"]),
        ("Sites in CH only",            cross["ch_only"]),
        ("Sites in both catalogs",      cross["common"]),
        ("Total union of sites",        cross["union"]),
        ("Jaccard similarity of site sets", f"{cross['jaccard']:.4f}"),
        ("PageRank Spearman rank corr. (shared sites)",
         f"{cross['rank_corr']:.4f}" if cross["rank_corr"] is not None else "N/A"),
    ]
    a(tbl(["Metric", "Value"], cc_rows))
    a(f"")

    if cross["common_sites_pr"]:
        a(f"### Sites appearing in both catalogs (by avg PageRank)")
        a(f"")
        a(tbl(
            ["#", "Site", "PageRank (ES)", "PageRank (CH)"],
            [(i+1, s, f"{es:.6f}", f"{ch:.6f}") for i, (s, es, ch) in enumerate(cross["common_sites_pr"])]
        ))
        a(f"")

    # ── Section 10: Interpretation ────────────────────────────────────────────
    a(f"## 10. Interpretation")
    a(f"")

    top_pr_es = r_es["top_pagerank"][0][0] if r_es["top_pagerank"] else "N/A"
    top_pr_ch = r_ch["top_pagerank"][0][0] if r_ch["top_pagerank"] else "N/A"
    top_hub_es = r_es["top_hubs"][0][0] if r_es["hits_ok"] and r_es["top_hubs"] else "N/A"
    top_hub_ch = r_ch["top_hubs"][0][0] if r_ch["hits_ok"] and r_ch["top_hubs"] else "N/A"
    top_auth_es = r_es["top_auths"][0][0] if r_es["hits_ok"] and r_es["top_auths"] else "N/A"
    top_auth_ch = r_ch["top_auths"][0][0] if r_ch["hits_ok"] and r_ch["top_auths"] else "N/A"
    top_bc_es = r_es["top_betweenness"][0][0] if r_es["top_betweenness"] else "N/A"
    top_bc_ch = r_ch["top_betweenness"][0][0] if r_ch["top_betweenness"] else "N/A"

    a(f"### ES (IDEE — Spain)")
    a(f"")
    a(f"- **Most influential site (PageRank):** `{top_pr_es}` — receives the most link authority in the ES catalog network.")
    a(f"- **Top hub:** `{top_hub_es}` — best aggregator pointing to authoritative ES data sources.")
    a(f"- **Top authority:** `{top_auth_es}` — most cited data/service endpoint within ES.")
    a(f"- **Key bridge (betweenness):** `{top_bc_es}` — structural gateway connecting sub-clusters.")
    a(f"- Graph density {r_es['density']:.6f} and Gini {r_es['gini_pagerank']:.4f} indicate "
      f"{'high centralization' if r_es['gini_pagerank'] > 0.5 else 'moderate distribution'} of link authority.")
    a(f"")
    a(f"### CH (geocat.ch — Switzerland)")
    a(f"")
    a(f"- **Most influential site (PageRank):** `{top_pr_ch}` — receives the most link authority in the CH catalog network.")
    a(f"- **Top hub:** `{top_hub_ch}` — best aggregator pointing to authoritative CH data sources.")
    a(f"- **Top authority:** `{top_auth_ch}` — most cited data/service endpoint within CH.")
    a(f"- **Key bridge (betweenness):** `{top_bc_ch}` — structural gateway connecting sub-clusters.")
    a(f"- Graph density {r_ch['density']:.6f} and Gini {r_ch['gini_pagerank']:.4f} indicate "
      f"{'high centralization' if r_ch['gini_pagerank'] > 0.5 else 'moderate distribution'} of link authority.")
    a(f"")
    a(f"### Cross-catalog")
    a(f"")
    a(f"- Jaccard similarity of site sets: **{cross['jaccard']:.4f}** — "
      f"{'low' if cross['jaccard'] < 0.05 else 'moderate' if cross['jaccard'] < 0.15 else 'high'} overlap between the two catalog ecosystems.")
    a(f"- **{cross['common']} shared sites** appear in both catalogs. "
      f"These likely represent pan-European standards bodies, INSPIRE infrastructure, or common data services.")
    if cross["rank_corr"] is not None:
        a(f"- PageRank rank correlation among shared sites: **{cross['rank_corr']:.4f}** — "
          f"{'sites are ranked similarly' if abs(cross['rank_corr']) > 0.5 else 'catalogs assign different importance to shared sites'}.")
    a(f"")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="WSM cross-catalog comparison: Spain IDEE vs Switzerland geocat.ch"
    )
    parser.add_argument("--es-run-id",   default=DEFAULT_ES_RUN)
    parser.add_argument("--ch-run-id",   default=DEFAULT_CH_RUN)
    parser.add_argument("--es-port",     type=int, default=DEFAULT_ES_PORT)
    parser.add_argument("--ch-port",     type=int, default=DEFAULT_CH_PORT)
    parser.add_argument("--top",         type=int, default=15)
    parser.add_argument("--no-report",   action="store_true")
    parser.add_argument("--output",      default=None)
    parser.add_argument("--exclude-es",  nargs="*", default=[], metavar="LABEL")
    parser.add_argument("--exclude-ch",  nargs="*", default=[], metavar="LABEL")
    parser.add_argument("--path-length", action="store_true",
                        help="Compute avg path length and diameter (slow on large graphs)")
    args = parser.parse_args()

    print(f"Connecting to Neo4j...")
    drv_es = connect(args.es_port, "ES")
    drv_ch = connect(args.ch_port, "CH")

    print(f"Fetching ES graph  (run_id={args.es_run_id})...")
    es_nodes, es_edges = fetch_graph(drv_es, args.es_run_id, args.exclude_es)
    drv_es.close()

    print(f"Fetching CH graph  (run_id={args.ch_run_id})...")
    ch_nodes, ch_edges = fetch_graph(drv_ch, args.ch_run_id, args.exclude_ch)
    drv_ch.close()

    G_es = build_graph(es_nodes, es_edges)
    G_ch = build_graph(ch_nodes, ch_edges)

    r_es = analyze_catalog("ES", G_es, CATALOG_DOMAINS["es"], args)
    r_ch = analyze_catalog("CH", G_ch, CATALOG_DOMAINS["ch"], args)

    cross = cross_compare(r_es, r_ch)

    report = build_report(r_es, r_ch, cross, args)

    print(report)

    if not args.no_report:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = args.output or os.path.join(SCRIPT_DIR, f"catalog_comparison_{ts}.md")
        with open(out_path, "w") as f:
            f.write(report)
        print(f"\nReport written → {out_path}")


if __name__ == "__main__":
    main()
