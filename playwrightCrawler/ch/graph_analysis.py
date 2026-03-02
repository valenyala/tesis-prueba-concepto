"""
Graph analysis for site-level crawl results stored in Neo4j.
Computes PageRank and HITS (hubs/authorities), outputs a Markdown report.
Run: python ch/graph_analysis.py --run-id <run_id>
"""
import argparse
import os
from datetime import datetime
from neo4j import GraphDatabase
import networkx as nx

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "tesis_password"

NOISE_DOMAINS = {"twitter.com", "facebook.com", "github.com"}


def fetch_graph(run_id):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    nodes = {}
    edges = []
    with driver.session() as s:
        for row in s.run(
            "MATCH (s:Site {run_id: $r}) RETURN s.url AS url, s.label AS label, s.pages_crawled AS pages",
            r=run_id,
        ):
            nodes[row["url"]] = {"label": row["label"], "pages": row["pages"] or 0}

        for row in s.run(
            "MATCH (a:Site {run_id: $r})-[r:LINKS_TO {run_id: $r}]->(b:Site {run_id: $r}) "
            "RETURN a.url AS src, b.url AS tgt, r.link_count AS count",
            r=run_id,
        ):
            edges.append((row["src"], row["tgt"], row["count"]))

    driver.close()
    return nodes, edges


def build_graph(nodes, edges, exclude=None):
    G = nx.DiGraph()
    exclude = exclude or set()
    for url, data in nodes.items():
        if data["label"] not in exclude:
            G.add_node(url, **data)
    for src, tgt, count in edges:
        if src in G and tgt in G:
            G.add_edge(src, tgt, weight=count)
    return G


def top(d, n=10):
    return sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]


def label(G, url):
    return G.nodes[url].get("label", url)


def analyse(G):
    pr = nx.pagerank(G, weight="weight")
    try:
        hubs, authorities = nx.hits(G, max_iter=500)
    except nx.PowerIterationFailedConvergence:
        hubs, authorities = {n: 0 for n in G}, {n: 0 for n in G}

    in_deg  = dict(G.in_degree(weight="weight"))
    out_deg = dict(G.out_degree(weight="weight"))
    in_deg_raw  = dict(G.in_degree())
    out_deg_raw = dict(G.out_degree())

    return {
        "pagerank":     pr,
        "hubs":         hubs,
        "authorities":  authorities,
        "in_degree":    in_deg,
        "out_degree":   out_deg,
        "in_degree_raw":  in_deg_raw,
        "out_degree_raw": out_deg_raw,
    }


def section(title, level=2):
    prefix = "#" * level
    return f"\n{prefix} {title}\n"


def table(headers, rows):
    sep = "| " + " | ".join("---" for _ in headers) + " |"
    head = "| " + " | ".join(headers) + " |"
    body = "\n".join("| " + " | ".join(str(c) for c in row) + " |" for row in rows)
    return f"{head}\n{sep}\n{body}\n"


def render_section(title, G, stats, nodes):
    lines = []
    lines.append(section(title, level=2))

    # Basic counts
    total_links = sum(d for _, _, d in G.edges.data("weight", default=0))
    pages_crawled = sum(d["pages"] for _, d in G.nodes(data=True))
    lines.append(f"| Metric | Value |\n|---|---|\n"
                 f"| Sites (nodes) | {G.number_of_nodes()} |\n"
                 f"| Site-to-site edges | {G.number_of_edges()} |\n"
                 f"| Total page-level links | {int(total_links)} |\n"
                 f"| Pages crawled | {int(pages_crawled)} |\n")

    # PageRank
    lines.append(section("PageRank — top 15", level=3))
    lines.append("> Measures overall importance/influence: a site scores higher when many other "
                 "important sites link to it.\n")
    pr_rows = [(i+1, label(G, u), f"{s:.6f}", int(stats['in_degree_raw'].get(u, 0)),
                int(stats['out_degree_raw'].get(u, 0)))
               for i, (u, s) in enumerate(top(stats["pagerank"], 15))]
    lines.append(table(["#", "Site", "PageRank", "In-degree", "Out-degree"], pr_rows))

    # HITS — Authorities
    lines.append(section("HITS — Authorities (top 15)", level=3))
    lines.append("> Authority score: sites that are linked to by many hubs — "
                 "i.e. the most-cited data/service endpoints.\n")
    auth_rows = [(i+1, label(G, u), f"{s:.6f}")
                 for i, (u, s) in enumerate(top(stats["authorities"], 15))]
    lines.append(table(["#", "Site", "Authority Score"], auth_rows))

    # HITS — Hubs
    lines.append(section("HITS — Hubs (top 15)", level=3))
    lines.append("> Hub score: sites that link out to many authorities — "
                 "i.e. catalog/portal pages that aggregate many resources.\n")
    hub_rows = [(i+1, label(G, u), f"{s:.6f}")
                for i, (u, s) in enumerate(top(stats["hubs"], 15))]
    lines.append(table(["#", "Site", "Hub Score"], hub_rows))

    # In-degree (weighted)
    lines.append(section("Most referenced sites (weighted in-degree, top 15)", level=3))
    lines.append("> Total number of page-level links pointing at each site.\n")
    in_rows = [(i+1, label(G, u), int(s))
               for i, (u, s) in enumerate(top(stats["in_degree"], 15))]
    lines.append(table(["#", "Site", "Incoming links"], in_rows))

    # Out-degree (weighted)
    lines.append(section("Most linking sites (weighted out-degree, top 15)", level=3))
    lines.append("> Total number of page-level links going out from each site.\n")
    out_rows = [(i+1, label(G, u), int(s))
                for i, (u, s) in enumerate(top(stats["out_degree"], 15))]
    lines.append(table(["#", "Site", "Outgoing links"], out_rows))

    # Strongest edges
    lines.append(section("Strongest site-to-site connections (top 15)", level=3))
    edge_data = sorted(
        [(label(G, u), label(G, v), int(d)) for u, v, d in G.edges.data("weight", default=0)],
        key=lambda x: x[2], reverse=True
    )[:15]
    lines.append(table(["Source", "Target", "Link count"], edge_data))

    # Pages crawled per site
    lines.append(section("Pages crawled per site (top 15)", level=3))
    page_rows = sorted(
        [(label(G, u), int(d["pages"])) for u, d in G.nodes(data=True)],
        key=lambda x: x[1], reverse=True
    )[:15]
    lines.append(table(["Site", "Pages crawled"], page_rows))

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    print(f"Fetching graph for run_id={args.run_id}...")
    nodes, edges = fetch_graph(args.run_id)
    print(f"  {len(nodes)} nodes, {len(edges)} edges fetched")

    G_full    = build_graph(nodes, edges)
    G_filtered = build_graph(nodes, edges, exclude=NOISE_DOMAINS)

    print("Running algorithms on full graph...")
    stats_full = analyse(G_full)
    print("Running algorithms on filtered graph...")
    stats_filtered = analyse(G_filtered)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_path = os.path.join(SCRIPT_DIR, f"graph_analysis_{args.run_id}.md")

    with open(out_path, "w") as f:
        f.write(f"# Graph Analysis — {args.run_id}\n")
        f.write(f"\n_Generated: {now}_\n")
        f.write(f"\n**Catalog:** geocat.ch · **Category:** Inland Waters\n")
        f.write(f"\n**Noise domains excluded in filtered version:** "
                f"{', '.join(sorted(NOISE_DOMAINS))}\n")

        f.write("\n---\n")
        f.write(section("1. Full graph (including social/generic sites)", level=1))
        f.write(render_section("", G_full, stats_full, nodes))

        f.write("\n---\n")
        f.write(section("2. Filtered graph (excluding twitter, facebook, github)", level=1))
        f.write(render_section("", G_filtered, stats_filtered, nodes))

    print(f"\nReport written to: {out_path}")


if __name__ == "__main__":
    main()
