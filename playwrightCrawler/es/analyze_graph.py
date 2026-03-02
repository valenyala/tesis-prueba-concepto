"""
Analyze the site-level graph for a given run_id from Neo4j.
Computes basic statistics, PageRank, and HITS, then writes
a markdown report to graph_analysis_{run_id}.md in the same folder.
"""

import argparse
import os
import sys
from datetime import datetime
from neo4j import GraphDatabase
import networkx as nx
import numpy as np


def _pagerank(G, alpha=0.85, max_iter=200, tol=1e-9):
    """Power-iteration PageRank using numpy (no scipy required)."""
    nodes = list(G.nodes())
    n = len(nodes)
    if n == 0:
        return {}
    idx = {v: i for i, v in enumerate(nodes)}

    # Build weighted adjacency, column-normalised (stochastic matrix)
    W = np.zeros((n, n))
    for u, v, d in G.edges(data=True):
        W[idx[v], idx[u]] += d.get("weight", 1)

    col_sum = W.sum(axis=0)
    dangling = col_sum == 0
    col_sum[dangling] = 1          # avoid div-by-zero; dangling nodes teleport
    W = W / col_sum

    r = np.full(n, 1.0 / n)
    dangling_weight = np.full(n, 1.0 / n)

    for _ in range(max_iter):
        r_new = alpha * (W @ r + dangling_weight * dangling @ r) + (1 - alpha) / n
        if np.linalg.norm(r_new - r, 1) < tol:
            r = r_new
            break
        r = r_new

    return {nodes[i]: float(r[i]) for i in range(n)}


def _hits(G, max_iter=500, tol=1e-9):
    """Power-iteration HITS using numpy (no scipy required).
    Returns (converged, hubs_dict, authorities_dict).
    """
    nodes = list(G.nodes())
    n = len(nodes)
    if n == 0:
        return True, {}, {}
    idx = {v: i for i, v in enumerate(nodes)}

    A = np.zeros((n, n))
    for u, v, d in G.edges(data=True):
        A[idx[u], idx[v]] += d.get("weight", 1)

    h = np.ones(n)
    for _ in range(max_iter):
        a_new = A.T @ h
        h_new = A @ a_new
        # Normalise
        a_norm = np.linalg.norm(a_new)
        h_norm = np.linalg.norm(h_new)
        if a_norm:
            a_new /= a_norm
        if h_norm:
            h_new /= h_norm
        # Check convergence by change between iterations
        if np.linalg.norm(h_new - h, 1) < tol:
            h, a = h_new, a_new
            break
        h, a = h_new, a_new
    else:
        return False, {}, {}

    # Normalise to [0,1]
    h = h / h.max() if h.max() else h
    a = a / a.max() if a.max() else a
    return (
        True,
        {nodes[i]: float(h[i]) for i in range(n)},
        {nodes[i]: float(a[i]) for i in range(n)},
    )


NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "tesis_password"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def fetch_graph(driver, run_id, exclude=None):
    """Fetch all Site nodes and LINKS_TO edges for the given run_id."""
    exclude = set(exclude or [])
    with driver.session() as session:
        nodes = session.run(
            "MATCH (s:Site {run_id: $run_id}) "
            "RETURN s.url AS url, s.label AS label, s.pages_crawled AS pages_crawled",
            run_id=run_id,
        ).data()

        edges = session.run(
            "MATCH (a:Site {run_id: $run_id})-[r:LINKS_TO {run_id: $run_id}]->(b:Site {run_id: $run_id}) "
            "RETURN a.label AS src, b.label AS tgt, r.link_count AS weight",
            run_id=run_id,
        ).data()

    nodes = [n for n in nodes if n["label"] not in exclude]
    edges = [e for e in edges if e["src"] not in exclude and e["tgt"] not in exclude]
    return nodes, edges


def build_nx_graph(nodes, edges):
    G = nx.DiGraph()
    for n in nodes:
        G.add_node(n["label"], url=n["url"], pages_crawled=n["pages_crawled"] or 0)
    for e in edges:
        G.add_edge(e["src"], e["tgt"], weight=e["weight"] or 1)
    return G


def fmt_table(headers, rows, col_widths=None):
    """Return a markdown table string."""
    if not col_widths:
        col_widths = [max(len(str(r[i])) for r in ([headers] + rows)) for i in range(len(headers))]
    sep = "| " + " | ".join("-" * w for w in col_widths) + " |"
    header = "| " + " | ".join(str(headers[i]).ljust(col_widths[i]) for i in range(len(headers))) + " |"
    body = "\n".join(
        "| " + " | ".join(str(rows[r][i]).ljust(col_widths[i]) for i in range(len(headers))) + " |"
        for r in range(len(rows))
    )
    return "\n".join([header, sep, body])


def analyze(run_id, args=None):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()

    print(f"Fetching graph for run_id={run_id}...")
    nodes, edges = fetch_graph(driver, run_id, exclude=args.exclude)
    driver.close()

    G = build_nx_graph(nodes, edges)

    print(f"  Nodes: {G.number_of_nodes()}  Edges: {G.number_of_edges()}")

    # ── Basic stats ──────────────────────────────────────────────────────────

    total_pages = sum(d["pages_crawled"] for _, d in G.nodes(data=True))
    total_link_weight = sum(d["weight"] for _, _, d in G.edges(data=True))

    crawled_nodes = [(n, d["pages_crawled"]) for n, d in G.nodes(data=True) if d["pages_crawled"] > 0]
    crawled_nodes.sort(key=lambda x: x[1], reverse=True)

    out_degree = sorted(G.out_degree(weight="weight"), key=lambda x: x[1], reverse=True)
    in_degree  = sorted(G.in_degree(weight="weight"),  key=lambda x: x[1], reverse=True)

    out_degree_unweighted = sorted(G.out_degree(), key=lambda x: x[1], reverse=True)
    in_degree_unweighted  = sorted(G.in_degree(),  key=lambda x: x[1], reverse=True)

    # ── PageRank (power iteration with numpy) ────────────────────────────────
    print("  Computing PageRank...")
    pr = _pagerank(G, alpha=0.85, max_iter=200, tol=1e-9)
    pr_sorted = sorted(pr.items(), key=lambda x: x[1], reverse=True)

    # ── HITS (power iteration with numpy) ────────────────────────────────────
    print("  Computing HITS...")
    hits_ok, hubs, authorities = _hits(G, max_iter=500, tol=1e-9)
    if hits_ok:
        hubs_sorted = sorted(hubs.items(),       key=lambda x: x[1], reverse=True)
        auth_sorted = sorted(authorities.items(), key=lambda x: x[1], reverse=True)
    else:
        print("  HITS did not converge.")

    # ── Structural metrics ────────────────────────────────────────────────────
    density = nx.density(G)
    try:
        # Only meaningful for weakly connected graphs
        wcc = list(nx.weakly_connected_components(G))
        scc = list(nx.strongly_connected_components(G))
    except Exception:
        wcc, scc = [], []

    # ── Write report ─────────────────────────────────────────────────────────
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    suffix = "_no_" + "_".join(args.exclude).replace(".", "-") if args.exclude else ""
    out_path = os.path.join(SCRIPT_DIR, f"graph_analysis_{run_id}{suffix}.md")

    TOP = 15

    lines = []
    a = lines.append

    a(f"# Graph Analysis — `{run_id}`")
    a(f"")
    a(f"Generated: {now}")
    a(f"")

    # Overview
    a(f"## Overview")
    a(f"")
    a(f"| Metric | Value |")
    a(f"|---|---|")
    a(f"| Site nodes | {G.number_of_nodes()} |")
    a(f"| Site-to-site edges | {G.number_of_edges()} |")
    a(f"| Total page-level links (edge weights) | {total_link_weight:,} |")
    a(f"| Total pages crawled | {total_pages} |")
    a(f"| Graph density | {density:.6f} |")
    a(f"| Weakly connected components | {len(wcc)} |")
    a(f"| Strongly connected components | {len(scc)} |")
    a(f"")

    # Pages crawled per site
    a(f"## Pages Crawled per Site")
    a(f"")
    rows = [(s, p) for s, p in crawled_nodes[:TOP]]
    if rows:
        a(fmt_table(["Site", "Pages crawled"], rows))
    else:
        a("_No pages crawled._")
    a(f"")

    # Degree (weighted)
    a(f"## Top Sites by Weighted Out-Degree (total outgoing links)")
    a(f"")
    a(fmt_table(["Site", "Outgoing link weight"], [(s, w) for s, w in out_degree[:TOP]]))
    a(f"")

    a(f"## Top Sites by Weighted In-Degree (total incoming links)")
    a(f"")
    a(fmt_table(["Site", "Incoming link weight"], [(s, w) for s, w in in_degree[:TOP]]))
    a(f"")

    # Degree (unweighted)
    a(f"## Top Sites by Unweighted Out-Degree (distinct sites linked to)")
    a(f"")
    a(fmt_table(["Site", "Sites linked to"], [(s, d) for s, d in out_degree_unweighted[:TOP]]))
    a(f"")

    a(f"## Top Sites by Unweighted In-Degree (distinct sites linking in)")
    a(f"")
    a(fmt_table(["Site", "Sites linking in"], [(s, d) for s, d in in_degree_unweighted[:TOP]]))
    a(f"")

    # Strongest edges
    a(f"## Strongest Site-to-Site Connections")
    a(f"")
    edge_rows = sorted(
        [(u, v, d["weight"]) for u, v, d in G.edges(data=True)],
        key=lambda x: x[2], reverse=True
    )[:TOP]
    a(fmt_table(["Source", "Target", "Link count"], edge_rows))
    a(f"")

    # PageRank
    a(f"## PageRank (α = 0.85, weighted)")
    a(f"")
    a(
        "PageRank measures the global importance of a site based on how many other "
        "important sites link to it. Higher score = more central to the network."
    )
    a(f"")
    a(fmt_table(
        ["Rank", "Site", "PageRank score"],
        [(i+1, s, f"{v:.6f}") for i, (s, v) in enumerate(pr_sorted[:TOP])]
    ))
    a(f"")

    # HITS
    if hits_ok:
        a(f"## HITS — Hubs")
        a(f"")
        a(
            "A **hub** is a site that links to many authoritative sites. "
            "High hub score = good directory or aggregator."
        )
        a(f"")
        a(fmt_table(
            ["Rank", "Site", "Hub score"],
            [(i+1, s, f"{v:.6f}") for i, (s, v) in enumerate(hubs_sorted[:TOP])]
        ))
        a(f"")

        a(f"## HITS — Authorities")
        a(f"")
        a(
            "An **authority** is a site that is linked to by many good hubs. "
            "High authority score = trusted content source."
        )
        a(f"")
        a(fmt_table(
            ["Rank", "Site", "Authority score"],
            [(i+1, s, f"{v:.6f}") for i, (s, v) in enumerate(auth_sorted[:TOP])]
        ))
        a(f"")
    else:
        a(f"## HITS")
        a(f"")
        a("_HITS algorithm did not converge for this graph._")
        a(f"")

    # Connected components
    a(f"## Connected Components")
    a(f"")
    a(f"**Weakly connected components** (ignoring edge direction): {len(wcc)}")
    a(f"")
    for i, comp in enumerate(sorted(wcc, key=len, reverse=True)[:5], 1):
        a(f"- Component {i}: {len(comp)} sites — {', '.join(sorted(comp)[:8])}{'...' if len(comp) > 8 else ''}")
    a(f"")
    a(f"**Strongly connected components** (following edge direction): {len(scc)}")
    a(f"")
    non_trivial_scc = [c for c in scc if len(c) > 1]
    if non_trivial_scc:
        for i, comp in enumerate(sorted(non_trivial_scc, key=len, reverse=True)[:5], 1):
            a(f"- SCC {i}: {len(comp)} sites — {', '.join(sorted(comp)[:8])}{'...' if len(comp) > 8 else ''}")
    else:
        a("_No non-trivial strongly connected components (no cycles between sites)._")
    a(f"")

    # Interpretation
    a(f"## Interpretation")
    a(f"")
    top_pr = pr_sorted[0][0] if pr_sorted else "N/A"
    top_hub = hubs_sorted[0][0] if hits_ok and hubs_sorted else "N/A"
    top_auth = auth_sorted[0][0] if hits_ok and auth_sorted else "N/A"
    top_out = out_degree[0][0] if out_degree else "N/A"
    top_in  = in_degree[0][0]  if in_degree  else "N/A"

    a(f"- **Most central site (PageRank):** `{top_pr}` — this site receives the most "
      f"link authority from the rest of the network.")
    a(f"- **Top hub (HITS):** `{top_hub}` — best aggregator/directory pointing to authoritative content.")
    a(f"- **Top authority (HITS):** `{top_auth}` — most trusted content source as judged by hub sites.")
    a(f"- **Highest out-degree (weighted):** `{top_out}` — sends the most links to other sites.")
    a(f"- **Highest in-degree (weighted):** `{top_in}` — receives the most links from other sites.")
    if len(wcc) == 1:
        a(f"- The graph is **weakly connected** — all sites are reachable from each other ignoring direction.")
    else:
        a(f"- The graph has **{len(wcc)} weakly connected components**, meaning some sites are isolated sub-clusters.")
    if not non_trivial_scc:
        a(f"- There are **no cycles** in the directed graph — the link structure is essentially a DAG "
          f"(or close to it), meaning no two sites mutually link to each other through the crawled pages.")
    a(f"")

    report = "\n".join(lines)
    with open(out_path, "w") as f:
        f.write(report)

    print(f"\nReport written to: {out_path}")
    return out_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze site graph from Neo4j and write markdown report")
    parser.add_argument("--run-id", default="run_20260218", help="Run ID to analyze")
    parser.add_argument("--exclude", nargs="*", default=[], metavar="LABEL",
                        help="Site labels to exclude (e.g. twitter.com www.facebook.com)")
    args = parser.parse_args()
    analyze(args.run_id, args=args)
