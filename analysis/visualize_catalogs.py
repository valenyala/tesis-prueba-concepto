"""
Visual representation of ES and CH site-level graphs side by side.
Nodes sized by PageRank, colored by in-degree, edges weighted by link count.

Usage:
  python analysis/visualize_catalogs.py [--out path/to/output.png]
"""

import argparse
import os
import sys
from datetime import datetime

import numpy as np
import networkx as nx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from neo4j import GraphDatabase

AUTH = ("neo4j", "tesis_password")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_ES_RUN = "sites-es-2026-02-23"
DEFAULT_CH_RUN = "sites-ch-2026-02-23"


def fetch_graph(uri, run_id, exclude=None):
    exclude = set(exclude or [])
    driver = GraphDatabase.driver(uri, auth=AUTH)
    driver.verify_connectivity()
    with driver.session() as s:
        nodes = s.run(
            "MATCH (n:Site {run_id: $r}) "
            "RETURN n.label AS label, n.pages_crawled AS pages",
            r=run_id,
        ).data()
        edges = s.run(
            "MATCH (a:Site {run_id: $r})-[e:LINKS_TO {run_id: $r}]->(b:Site {run_id: $r}) "
            "RETURN a.label AS src, b.label AS tgt, e.link_count AS w",
            r=run_id,
        ).data()
    driver.close()
    labels = {n["label"] for n in nodes if n["label"] not in exclude}
    G = nx.DiGraph()
    for n in nodes:
        if n["label"] in labels:
            G.add_node(n["label"], pages=n["pages"] or 0)
    for e in edges:
        if e["src"] in labels and e["tgt"] in labels:
            G.add_edge(e["src"], e["tgt"], weight=e["w"] or 1)
    return G


def _pagerank(G):
    n = G.number_of_nodes()
    if n == 0:
        return {}
    nodes = list(G.nodes())
    idx = {v: i for i, v in enumerate(nodes)}
    W = np.zeros((n, n))
    for u, v, d in G.edges(data=True):
        W[idx[v], idx[u]] += d.get("weight", 1)
    col = W.sum(axis=0)
    dangling = col == 0
    col[dangling] = 1
    W = W / col
    r = np.full(n, 1.0 / n)
    dw = np.full(n, 1.0 / n)
    alpha = 0.85
    for _ in range(200):
        r_new = alpha * (W @ r + dw * dangling @ r) + (1 - alpha) / n
        if np.linalg.norm(r_new - r, 1) < 1e-9:
            r = r_new
            break
        r = r_new
    return {nodes[i]: float(r[i]) for i in range(n)}


def draw_graph(ax, G, title, top_n_labels=12):
    if G.number_of_nodes() == 0:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title)
        return

    pr = _pagerank(G)
    pr_vals = np.array([pr.get(n, 0) for n in G.nodes()])
    pr_min, pr_max = pr_vals.min(), pr_vals.max()

    # Node sizes: scale PageRank to [100, 3000]
    if pr_max > pr_min:
        norm_pr = (pr_vals - pr_min) / (pr_max - pr_min)
    else:
        norm_pr = np.ones(len(pr_vals))
    node_sizes = 100 + norm_pr * 2900

    # Node colors: in-degree (number of distinct sites linking in)
    in_deg = np.array([G.in_degree(n) for n in G.nodes()], dtype=float)
    in_max = in_deg.max() if in_deg.max() > 0 else 1
    norm_indeg = in_deg / in_max

    cmap = plt.cm.plasma
    node_colors = [cmap(v) for v in norm_indeg]

    # Layout: spring for CH (larger), spring for ES too but with stronger k
    k = 2.5 / (G.number_of_nodes() ** 0.5)
    pos = nx.spring_layout(G, k=k, iterations=80, seed=42, weight="weight")

    # Edge widths: log-scaled link count
    weights = np.array([d.get("weight", 1) for _, _, d in G.edges(data=True)], dtype=float)
    if len(weights):
        log_w = np.log1p(weights)
        lw = 0.3 + (log_w / log_w.max()) * 2.5
    else:
        lw = []

    nx.draw_networkx_edges(
        G, pos, ax=ax,
        width=lw,
        alpha=0.35,
        edge_color="#888888",
        arrows=True,
        arrowsize=8,
        connectionstyle="arc3,rad=0.1",
        min_source_margin=6,
        min_target_margin=6,
    )

    nx.draw_networkx_nodes(
        G, pos, ax=ax,
        node_size=node_sizes,
        node_color=node_colors,
        linewidths=0.5,
        edgecolors="white",
    )

    # Label only top-N by PageRank
    top_nodes = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:top_n_labels]
    top_labels = {n: n for n, _ in top_nodes}
    nx.draw_networkx_labels(
        G, pos, labels=top_labels, ax=ax,
        font_size=6.5,
        font_color="white",
        font_weight="bold",
        bbox=dict(boxstyle="round,pad=0.15", fc="#222222", alpha=0.55, lw=0),
    )

    ax.set_title(title, fontsize=13, fontweight="bold", pad=8)
    ax.axis("off")

    # Colorbar for in-degree
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=mcolors.Normalize(0, int(in_max)))
    sm.set_array([])
    cb = plt.colorbar(sm, ax=ax, fraction=0.03, pad=0.02, shrink=0.6)
    cb.set_label("In-degree (# sites linking in)", fontsize=7)
    cb.ax.tick_params(labelsize=6)

    # Stats annotation
    n_nodes = G.number_of_nodes()
    n_edges = G.number_of_edges()
    density = nx.density(G)
    top_pr_node = top_nodes[0][0] if top_nodes else "—"
    top_pr_val  = top_nodes[0][1] if top_nodes else 0
    info = (
        f"Sites: {n_nodes}  |  Edges: {n_edges}\n"
        f"Density: {density:.5f}\n"
        f"Top PageRank: {top_pr_node}\n"
        f"  (PR={top_pr_val:.5f})"
    )
    ax.text(
        0.01, 0.01, info,
        transform=ax.transAxes,
        fontsize=7,
        verticalalignment="bottom",
        bbox=dict(boxstyle="round,pad=0.4", fc="black", alpha=0.5, lw=0),
        color="white",
        family="monospace",
    )

    # Node size legend
    for pr_label, sz in [("low PR", 100), ("mid PR", 800), ("high PR", 3000)]:
        ax.scatter([], [], s=sz, c="gray", alpha=0.6, label=pr_label)
    ax.legend(
        title="Node size = PageRank",
        title_fontsize=6,
        fontsize=6,
        loc="upper right",
        framealpha=0.4,
        labelcolor="white",
        facecolor="black",
        edgecolor="none",
    )


def main():
    parser = argparse.ArgumentParser(description="Visualize ES and CH site graphs side by side")
    parser.add_argument("--es-run-id", default=DEFAULT_ES_RUN)
    parser.add_argument("--ch-run-id", default=DEFAULT_CH_RUN)
    parser.add_argument("--es-port", default=7687, type=int)
    parser.add_argument("--ch-port", default=7688, type=int)
    parser.add_argument("--top-labels", default=12, type=int, help="How many nodes to label")
    parser.add_argument("--out", default=None, help="Output PNG path")
    parser.add_argument("--exclude-es", nargs="*", default=[], metavar="LABEL")
    parser.add_argument("--exclude-ch", nargs="*", default=[], metavar="LABEL")
    args = parser.parse_args()

    print(f"Fetching ES graph  (run={args.es_run_id}, port={args.es_port})...")
    G_es = fetch_graph(f"bolt://localhost:{args.es_port}", args.es_run_id, args.exclude_es)
    print(f"  → {G_es.number_of_nodes()} nodes, {G_es.number_of_edges()} edges")

    print(f"Fetching CH graph  (run={args.ch_run_id}, port={args.ch_port})...")
    G_ch = fetch_graph(f"bolt://localhost:{args.ch_port}", args.ch_run_id, args.exclude_ch)
    print(f"  → {G_ch.number_of_nodes()} nodes, {G_ch.number_of_edges()} edges")

    fig, axes = plt.subplots(1, 2, figsize=(22, 12))
    fig.patch.set_facecolor("#111111")
    for ax in axes:
        ax.set_facecolor("#111111")

    draw_graph(axes[0], G_es, f"Spain IDEE — {args.es_run_id}", args.top_labels)
    draw_graph(axes[1], G_ch, f"Switzerland geocat.ch — {args.ch_run_id}", args.top_labels)

    fig.suptitle(
        "Web Structure Mining — Site-Level Link Graphs",
        fontsize=16, fontweight="bold", color="white", y=0.98,
    )
    fig.text(
        0.5, 0.01,
        "Node size ∝ PageRank  |  Node color ∝ in-degree  |  Edge width ∝ log(link count)  |  Top-labeled nodes by PageRank",
        ha="center", fontsize=8, color="#aaaaaa",
    )

    plt.tight_layout(rect=[0, 0.02, 1, 0.97])

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = args.out or os.path.join(SCRIPT_DIR, f"graph_viz_{ts}.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"\nSaved → {out_path}")


if __name__ == "__main__":
    main()
