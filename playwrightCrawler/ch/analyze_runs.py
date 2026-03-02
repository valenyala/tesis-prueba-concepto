"""
Compares CH crawler runs using PageRank and HITS (Hubs & Authorities).
Queries Neo4j for graph data, computes algorithms with networkx.
"""

import networkx as nx
from neo4j import GraphDatabase
from collections import defaultdict

NEO4J_URI = "bolt://localhost:7687"
AUTH = ("neo4j", "tesis_password")
PORT = 7688  # CH container mapped port

RUNS = [
    "run_20260223",
    "run_20260223_16:11:01",
    "run_20260223_17:06:48",
]

TOP_N = 10


def fetch_graph(driver, run_id):
    with driver.session() as session:
        edges = session.run(
            """
            MATCH (a:Site {run_id: $run_id})-[r:LINKS_TO {run_id: $run_id}]->(b:Site {run_id: $run_id})
            RETURN a.label AS src, b.label AS dst, r.link_count AS weight,
                   a.pages_crawled AS src_pages, b.pages_crawled AS dst_pages
            """,
            run_id=run_id,
        ).data()

        nodes = session.run(
            """
            MATCH (s:Site {run_id: $run_id})
            RETURN s.label AS label, s.pages_crawled AS pages_crawled
            ORDER BY s.pages_crawled DESC
            """,
            run_id=run_id,
        ).data()
    return nodes, edges


def build_digraph(edges):
    G = nx.DiGraph()
    for e in edges:
        w = e["weight"] or 1
        G.add_edge(e["src"], e["dst"], weight=w)
    return G


def top_n(d, n=TOP_N, reverse=True):
    return sorted(d.items(), key=lambda x: x[1], reverse=reverse)[:n]


def print_table(title, rows, col1="Site", col2="Score"):
    print(f"\n  {title}")
    print(f"  {'#':<3} {col1:<40} {col2}")
    print(f"  {'-'*3} {'-'*40} {'-'*12}")
    for i, (label, score) in enumerate(rows, 1):
        print(f"  {i:<3} {label:<40} {score:.6f}")


def analyze_run(driver, run_id, node_count, rel_count):
    print(f"\n{'='*70}")
    print(f"RUN: {run_id}")
    print(f"  Nodes: {node_count}  |  Relationships: {rel_count}")

    nodes, edges = fetch_graph(driver, run_id)
    G = build_digraph(edges)

    # Basic graph metrics
    isolates = list(nx.isolates(G))
    scc = list(nx.strongly_connected_components(G))
    largest_scc = max(scc, key=len) if scc else set()
    wcc = list(nx.weakly_connected_components(G))
    largest_wcc = max(wcc, key=len) if wcc else set()

    print(f"\n  Graph metrics:")
    print(f"    Nodes in graph (with edges): {G.number_of_nodes()}")
    print(f"    Edges in graph:              {G.number_of_edges()}")
    print(f"    Isolates (no edges):         {len(isolates)}")
    print(f"    Weakly connected components: {len(wcc)}  (largest: {len(largest_wcc)} nodes)")
    print(f"    Strongly connected comps:    {len(scc)}  (largest: {len(largest_scc)} nodes)")
    density = nx.density(G)
    print(f"    Density:                     {density:.6f}")

    # Top pages_crawled nodes (from Neo4j)
    print(f"\n  Top {TOP_N} sites by pages_crawled:")
    print(f"  {'#':<3} {'Site':<40} {'Pages'}")
    print(f"  {'-'*3} {'-'*40} {'-'*8}")
    for i, n in enumerate(nodes[:TOP_N], 1):
        print(f"  {i:<3} {n['label']:<40} {n['pages_crawled']}")

    # PageRank (weight-aware)
    pr = nx.pagerank(G, alpha=0.85, weight="weight", max_iter=200)
    print_table(f"PageRank (α=0.85, weighted) — Top {TOP_N}", top_n(pr))

    # In-degree centrality
    in_deg = dict(G.in_degree(weight="weight"))
    print_table(f"Weighted In-degree — Top {TOP_N}", top_n(in_deg), col2="In-weight")

    # HITS (hubs & authorities)
    try:
        hubs, authorities = nx.hits(G, max_iter=300, normalized=True)
        print_table(f"HITS Hubs — Top {TOP_N}", top_n(hubs))
        print_table(f"HITS Authorities — Top {TOP_N}", top_n(authorities))
    except nx.PowerIterationFailedConvergence:
        print("\n  HITS: did not converge")

    # Self-loops (sites linking to themselves)
    self_loops = [(u, d["weight"]) for u, v, d in G.edges(data=True) if u == v]
    if self_loops:
        print(f"\n  Self-loops ({len(self_loops)}):")
        for site, w in sorted(self_loops, key=lambda x: -x[1])[:5]:
            print(f"    {site}  (weight={w})")

    return {
        "run_id": run_id,
        "nodes": node_count,
        "rels": rel_count,
        "graph_nodes": G.number_of_nodes(),
        "graph_edges": G.number_of_edges(),
        "isolates": len(isolates),
        "wcc": len(wcc),
        "largest_wcc": len(largest_wcc),
        "scc": len(scc),
        "largest_scc": len(largest_scc),
        "density": density,
        "pagerank": pr,
        "hubs": hubs if "hubs" in dir() else {},
        "authorities": authorities if "authorities" in dir() else {},
    }


def compare_pagerank(results):
    print(f"\n\n{'='*70}")
    print("CROSS-RUN PAGERANK COMPARISON (top sites)")
    all_sites = set()
    for r in results:
        all_sites.update(r["pagerank"].keys())

    # Build a merged view of top sites by average PageRank
    avg_pr = {}
    for site in all_sites:
        scores = [r["pagerank"].get(site, 0) for r in results]
        avg_pr[site] = sum(scores) / len(scores)

    top_sites = [s for s, _ in sorted(avg_pr.items(), key=lambda x: -x[1])[:15]]

    labels = [r["run_id"].replace("run_20260223", "R1").replace("_16:11:01", "").replace("_17:06:48", "") for r in results]
    labels = ["run_20260223", "run_16:11:01", "run_17:06:48"]

    header = f"  {'Site':<42}" + "".join(f"{l:<18}" for l in labels)
    print(f"\n{header}")
    print(f"  {'-'*42}" + "-"*54)
    for site in top_sites:
        row = f"  {site:<42}"
        for r in results:
            score = r["pagerank"].get(site, 0)
            row += f"{score:<18.6f}"
        print(row)


def compare_summary(results):
    print(f"\n\n{'='*70}")
    print("SUMMARY COMPARISON")
    print(f"\n  {'Metric':<30} {'run_20260223':<20} {'run_16:11:01':<20} {'run_17:06:48':<20}")
    print(f"  {'-'*30} {'-'*20} {'-'*20} {'-'*20}")
    metrics = [
        ("nodes (Neo4j)", "nodes"),
        ("relationships (Neo4j)", "rels"),
        ("graph nodes (w/ edges)", "graph_nodes"),
        ("graph edges", "graph_edges"),
        ("isolates", "isolates"),
        ("weakly conn. components", "wcc"),
        ("largest WCC", "largest_wcc"),
        ("strongly conn. comps", "scc"),
        ("largest SCC", "largest_scc"),
        ("density", "density"),
    ]
    for label, key in metrics:
        vals = [r[key] for r in results]
        row = f"  {label:<30}"
        for v in vals:
            cell = f"{v:.6f}" if isinstance(v, float) else str(v)
            row += f"{cell:<20}"
        print(row)


if __name__ == "__main__":
    # CH container uses port 7688
    driver = GraphDatabase.driver(f"bolt://localhost:7688", auth=AUTH)

    COUNTS = {
        "run_20260223":          (166, 216),
        "run_20260223_16:11:01": (146, 179),
        "run_20260223_17:06:48": (171, 215),
    }

    results = []
    for run_id in RUNS:
        n, r = COUNTS[run_id]
        res = analyze_run(driver, run_id, n, r)
        results.append(res)

    compare_summary(results)
    compare_pagerank(results)

    driver.close()
