"""
Service Layer Visualization — IDE → GeoNetwork → Services
==========================================================
Hierarchical visualization of the 3-layer graph structure.
Nodes are positioned by semantic layer (IDE at top, services at bottom)
and colored by node type. Edges are colored by whether they carry
OGC service links (service_link_count > 0).

Output: one PNG with ES (left) and CH (right) side by side,
        plus individual layer-focused close-ups.

Usage:
  python analysis/visualize_service_layers.py [options]

Options:
  --es-run-id     ES run ID   (default: run_20260223)
  --ch-run-id     CH run ID   (default: run_20260223_17:06:48)
  --es-port       ES port     (default: 7687)
  --ch-port       CH port     (default: 7688)
  --out           Output PNG  (default: analysis/service_layers_<ts>.png)
  --no-other      Exclude SOCIAL and OTHER nodes from the graph (cleaner)
  --top-labels    How many nodes to label per graph (default: 30)
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime

import numpy as np
import networkx as nx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines

try:
    from neo4j import GraphDatabase
except ImportError:
    print("ERROR: neo4j package not found.")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
AUTH = ("neo4j", "tesis_password")

DEFAULT_ES_RUN  = "run_20260223"
DEFAULT_CH_RUN  = "run_20260223_17:06:48"
DEFAULT_ES_PORT = 7687
DEFAULT_CH_PORT = 7688

# ── Node type palette & layers ─────────────────────────────────────────────────

TYPE_COLOR = {
    "IDE_SUPRANACIONAL": "#9B59B6",   # purple
    "IDE_NACIONAL":      "#1565C0",   # deep blue
    "IDE_REGIONAL":      "#42A5F5",   # light blue
    "GEONETWORK":        "#2ECC71",   # green
    "GEO_PORTAL":        "#00BCD4",   # cyan/teal
    "SERVICE_WMS":       "#E74C3C",   # red
    "SERVICE_WFS":       "#E67E22",   # orange
    "SERVICE_WMTS":      "#F1C40F",   # yellow
    "SERVICE_PLATFORM":  "#FF7043",   # deep orange
    "SERVICE_OTHER":     "#FF8A65",   # light orange
    "SOCIAL":            "#78909C",   # blue-grey
    "OTHER":             "#546E7A",   # darker blue-grey
}

# Y-level for hierarchical layout (higher = top of figure)
LAYER_Y = {
    "IDE_SUPRANACIONAL":  5,
    "IDE_NACIONAL":       4,
    "GEONETWORK":         3,
    "IDE_REGIONAL":       3,
    "GEO_PORTAL":         2,
    "SERVICE_WMS":        1,
    "SERVICE_WFS":        1,
    "SERVICE_WMTS":       1,
    "SERVICE_PLATFORM":   1,
    "SERVICE_OTHER":      1,
    "SOCIAL":             0,
    "OTHER":              0,
}

# Types to always label (regardless of top-N)
ALWAYS_LABEL = {
    "IDE_SUPRANACIONAL", "IDE_NACIONAL", "GEONETWORK", "SERVICE_PLATFORM",
}

# ── Classification (copied from service_layer_report.py for self-containment) ──

SOCIAL_DOMAINS = {
    "twitter.com", "www.facebook.com", "github.com", "www.linkedin.com",
    "www.youtube.com", "www.instagram.com", "pt-br.facebook.com",
    "fr-fr.facebook.com", "it-it.facebook.com", "de-de.facebook.com",
    "id-id.facebook.com", "ar-ar.facebook.com", "hi-in.facebook.com",
    "zh-cn.facebook.com", "l.facebook.com", "developers.facebook.com",
    "messenger.com", "www.meta.com", "about.meta.com", "www.meta.ai",
    "www.threads.com", "play.google.com", "itunes.apple.com",
}
IDE_SUPRANACIONAL_DOMAINS = {"inspire-geoportal.ec.europa.eu", "inspire.ec.europa.eu"}
IDE_NACIONAL_DOMAINS = {
    "www.idee.es", "idee.es",
    "www.geo.admin.ch", "geo.admin.ch", "www.swisstopo.admin.ch",
}
GEONETWORK_DOMAINS = {
    "www.geocat.ch", "geocat.ch", "info.geocat.ch", "www.info.geocat.ch",
    "registry.geocat.ch", "www.geocat.admin.ch",
}
IDE_REGIONAL_DOMAINS = {
    "icearagon.aragon.es", "catalogo.idecanarias.es", "idecyl.jcyl.es",
    "ide.cat", "idena.navarra.es", "idem.madrid.org", "idem.comunidad.madrid",
    "ideas.asturias.es", "www.ideandalucia.es", "www.iderioja.larioja.org",
    "metadatos.ideex.es", "ide.caceres.es", "geoserveis.ide.cat",
    "servicios.idee.es", "wmts-snczi.idee.es", "api-coverages.idee.es",
    "ideespain.github.io", "idechg.chguadalquivir.es",
}
SERVICE_WMS_DOMAINS = {
    "wms.mapama.gob.es", "geoserveis.ide.cat",
    "wms.geo.admin.ch", "wms.geo.sh.ch", "wms.geo.bs.ch",
    "services.geo.sg.ch", "services.geo.zg.ch", "geoservices.jura.ch",
}
SERVICE_WFS_DOMAINS = {"wfs.geo.sh.ch", "wfs.geo.bs.ch"}
SERVICE_WMTS_DOMAINS = {"wmts.mapama.gob.es", "wmts-snczi.idee.es", "wmts.geo.admin.ch"}
SERVICE_PLATFORM_DOMAINS = {"geodienste.ch", "www.geodienste.ch"}
SERVICE_OTHER_DOMAINS = {
    "servicios.idee.es", "api-coverages.idee.es",
    "rts.larioja.org", "vts.larioja.org", "geoapi.larioja.org",
    "ch-osm-services.geodatasolutions.ch",
}
GEO_PORTAL_DOMAINS = {
    "www.geo.euskadi.eus", "mapas-gis-inter.carm.es", "geo.araba.eus",
    "sig.mapama.gob.es", "terramapas.icv.gva.es", "sig.miteco.gob.es",
    "portaleslr.carm.es", "mapas.ideex.es", "visor.gva.es",
    "sig.pamplona.es", "www.chguadalquivir.es", "laboratoriorediam.cica.es",
    "portalrediam.cica.es", "descargasrediam.cica.es", "centrodedescargas.cnig.es",
    "icv.gva.es", "icvficherosweb.icv.gva.es", "descargas.icv.gva.es",
    "data.geo.admin.ch", "map.geo.admin.ch", "viewer.swissgeol.ch",
    "www.gis-daten.ch", "geoshop.lisag.ch", "geobasisdaten.ch",
    "raster.sitg.ge.ch", "map.koeniz.ch", "www.swissgeol.ch",
    "map.geo.sz.ch", "map.geo.tg.ch", "geo.fr.ch", "maps.fr.ch",
    "data.geo.sh.ch", "geo.jura.ch", "geo.vs.ch", "api3.geo.admin.ch",
    "docs.geo.admin.ch", "backend.geo.admin.ch", "s.geo.admin.ch",
    "models.geo.admin.ch", "map.geo.fr.ch", "models.geo.bs.ch",
    "map.geo.gl.ch", "sig.biel-bienne.ch", "be-geo.ch",
    "geoportal.koeniz.ch", "map.geo.sh.ch", "geozen.ch",
    "hydromaps.ch", "biel-bienne.mapplus.ch",
    "enterprise.arcgis.com", "www.arcgis.com", "survey123.arcgis.com",
    "opendata.swiss",
}


def classify_node(label: str) -> str:
    if label in SOCIAL_DOMAINS:             return "SOCIAL"
    if label in IDE_SUPRANACIONAL_DOMAINS:  return "IDE_SUPRANACIONAL"
    if label in IDE_NACIONAL_DOMAINS:       return "IDE_NACIONAL"
    if label in GEONETWORK_DOMAINS:         return "GEONETWORK"
    if label in IDE_REGIONAL_DOMAINS:       return "IDE_REGIONAL"
    if label in SERVICE_WMS_DOMAINS:        return "SERVICE_WMS"
    if label in SERVICE_WFS_DOMAINS:        return "SERVICE_WFS"
    if label in SERVICE_WMTS_DOMAINS:       return "SERVICE_WMTS"
    if label in SERVICE_PLATFORM_DOMAINS:   return "SERVICE_PLATFORM"
    if label in SERVICE_OTHER_DOMAINS:      return "SERVICE_OTHER"
    if label in GEO_PORTAL_DOMAINS:         return "GEO_PORTAL"
    l = label.lower()
    if l.startswith("wms.") or ".wms." in l:    return "SERVICE_WMS"
    if l.startswith("wfs.") or ".wfs." in l:    return "SERVICE_WFS"
    if l.startswith("wcs."):                    return "SERVICE_WCS"
    if l.startswith("wmts.") or l.startswith("wmts-"): return "SERVICE_WMTS"
    if "geonetwork" in l or "geocat" in l:      return "GEONETWORK"
    if any(l.startswith(p) for p in ("ide.", "ide-", "idem.", "ideas.", "idee.", "idex", "ides.")):
        return "IDE_REGIONAL"
    if any(x in l for x in ("idena.", "ideex", "idecyl", "idecat", "ideand", "iderioja")):
        return "IDE_REGIONAL"
    return "OTHER"


# ── Neo4j fetching ────────────────────────────────────────────────────────────

def fetch_graph(uri, run_id):
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
            "RETURN a.label AS src, b.label AS dst, "
            "e.link_count AS weight, e.service_link_count AS svc_weight",
            r=run_id,
        ).data()
    driver.close()

    G = nx.DiGraph()
    for n in nodes:
        ntype = classify_node(n["label"])
        G.add_node(
            n["label"],
            pages=n["pages"] or 0,
            node_type=ntype,
            layer_y=LAYER_Y.get(ntype, 0),
        )
    for e in edges:
        if e["src"] in G and e["dst"] in G:
            G.add_edge(
                e["src"], e["dst"],
                weight=e.get("weight") or 1,
                svc_weight=e.get("svc_weight") or 0,
            )
    return G


# ── Hierarchical layout ───────────────────────────────────────────────────────

def hierarchical_layout(G, exclude_types=None, x_spread=1.8, y_spread=1.4, seed=42):
    """
    Place nodes in horizontal bands by layer_y attribute.
    Within each band, nodes are spread evenly along X with a small random jitter.
    """
    exclude_types = set(exclude_types or [])
    rng = np.random.default_rng(seed)

    # Collect nodes per layer
    layers = defaultdict(list)
    for n, d in G.nodes(data=True):
        if d["node_type"] not in exclude_types:
            layers[d["layer_y"]].append(n)

    pos = {}
    for y_val, nodes_in_layer in layers.items():
        n = len(nodes_in_layer)
        # Sort nodes within layer for deterministic ordering
        nodes_sorted = sorted(nodes_in_layer)
        for i, node in enumerate(nodes_sorted):
            x = (i - (n - 1) / 2) * x_spread
            # Small jitter so labels don't perfectly overlap
            jitter = rng.uniform(-0.05 * x_spread, 0.05 * x_spread)
            pos[node] = (x + jitter, y_val * y_spread)

    # Nodes excluded from drawing still need a position (off-canvas)
    for n, d in G.nodes(data=True):
        if n not in pos:
            pos[n] = (99999, 99999)

    return pos


# ── Drawing ───────────────────────────────────────────────────────────────────

def draw_layer_graph(ax, G, title, top_labels=30, exclude_types=None, catalog_name=""):
    exclude_types = set(exclude_types or [])
    BG = "#0D1117"
    ax.set_facecolor(BG)

    # Filter to drawable nodes
    draw_nodes = [n for n, d in G.nodes(data=True) if d["node_type"] not in exclude_types]
    if not draw_nodes:
        ax.text(0.5, 0.5, "No data", ha="center", va="center",
                transform=ax.transAxes, color="white")
        ax.set_title(title, color="white")
        return

    H = G.subgraph(draw_nodes).copy()
    pos = hierarchical_layout(H, exclude_types=exclude_types)

    # ── Node sizes: log(pages+1), min=60 for non-crawled ──────────────────────
    pages_arr = np.array([H.nodes[n]["pages"] for n in H.nodes()])
    log_pages = np.log1p(pages_arr)
    if log_pages.max() > 0:
        norm_pages = log_pages / log_pages.max()
    else:
        norm_pages = np.zeros(len(pages_arr))
    node_sizes = 60 + norm_pages * 1400

    # ── Node colors by type ───────────────────────────────────────────────────
    node_colors = [TYPE_COLOR.get(H.nodes[n]["node_type"], "#AAAAAA") for n in H.nodes()]

    # ── Edge colors: service edges (svc_weight > 0) vs generic ───────────────
    edge_svc    = [(u, v) for u, v, d in H.edges(data=True) if d.get("svc_weight", 0) > 0]
    edge_gen    = [(u, v) for u, v, d in H.edges(data=True) if d.get("svc_weight", 0) == 0]
    weights_svc = [H[u][v]["weight"] for u, v in edge_svc]
    weights_gen = [H[u][v]["weight"] for u, v in edge_gen]

    def log_width(ws, lo=0.4, hi=2.5):
        if not ws:
            return []
        arr = np.log1p(np.array(ws, dtype=float))
        mx = arr.max() if arr.max() > 0 else 1
        return list(lo + (arr / mx) * (hi - lo))

    common_edge_kwargs = dict(
        ax=ax,
        arrows=True,
        arrowsize=7,
        connectionstyle="arc3,rad=0.08",
        min_source_margin=5,
        min_target_margin=5,
        node_size=node_sizes,  # needed for arrow placement
    )

    if edge_gen:
        nx.draw_networkx_edges(
            H, pos,
            edgelist=edge_gen,
            width=log_width(weights_gen, 0.3, 1.5),
            edge_color="#4A5568",
            alpha=0.45,
            **common_edge_kwargs,
        )
    if edge_svc:
        nx.draw_networkx_edges(
            H, pos,
            edgelist=edge_svc,
            width=log_width(weights_svc, 0.8, 3.0),
            edge_color="#FF6B6B",
            alpha=0.85,
            **common_edge_kwargs,
        )

    # ── Draw nodes ────────────────────────────────────────────────────────────
    nx.draw_networkx_nodes(
        H, pos, ax=ax,
        node_size=node_sizes,
        node_color=node_colors,
        linewidths=0.8,
        edgecolors="#FFFFFF",
        alpha=0.92,
    )

    # ── Labels: always-label types + top-N by in-degree ───────────────────────
    in_w = dict(H.in_degree(weight="weight"))
    always = {n for n, d in H.nodes(data=True) if d["node_type"] in ALWAYS_LABEL}
    top_by_indeg = set(
        n for n, _ in sorted(in_w.items(), key=lambda x: -x[1])[:top_labels]
    )
    label_set = always | top_by_indeg
    label_map = {n: n for n in label_set if n in pos}

    nx.draw_networkx_labels(
        H, pos,
        labels=label_map,
        ax=ax,
        font_size=5.5,
        font_color="white",
        font_weight="bold",
        bbox=dict(boxstyle="round,pad=0.12", fc="#1A202C", alpha=0.7, lw=0),
    )

    # ── Layer band annotations on Y-axis ──────────────────────────────────────
    layer_labels = {
        5 * 1.4: "SUPRANACIONAL",
        4 * 1.4: "IDE NACIONAL",
        3 * 1.4: "GEONETWORK / IDE REGIONAL",
        2 * 1.4: "GEO PORTALS",
        1 * 1.4: "SERVICES (WMS / WFS / WMTS)",
        0 * 1.4: "SOCIAL / OTHER",
    }
    for y_pos, layer_label in layer_labels.items():
        ax.axhline(y=y_pos, color="#2D3748", linewidth=0.6, linestyle="--", alpha=0.5)
        ax.text(
            ax.get_xlim()[0] if ax.get_xlim()[0] != 0 else -8,
            y_pos + 0.08,
            layer_label,
            fontsize=5.5, color="#718096", ha="left", va="bottom",
            style="italic",
        )

    # ── Stats box ─────────────────────────────────────────────────────────────
    type_counts = defaultdict(int)
    for n, d in H.nodes(data=True):
        type_counts[d["node_type"]] += 1
    svc_types = ["SERVICE_WMS", "SERVICE_WFS", "SERVICE_WMTS", "SERVICE_PLATFORM", "SERVICE_OTHER"]
    n_services  = sum(type_counts.get(t, 0) for t in svc_types)
    n_ide       = type_counts.get("IDE_NACIONAL", 0) + type_counts.get("IDE_REGIONAL", 0)
    n_geo_cat   = type_counts.get("GEONETWORK", 0)
    n_svc_edges = sum(1 for _, _, d in H.edges(data=True) if d.get("svc_weight", 0) > 0)
    total_svc_w = sum(d.get("svc_weight", 0) for _, _, d in H.edges(data=True))
    total_w     = sum(d.get("weight", 0) for _, _, d in H.edges(data=True))

    info = (
        f"Nodes: {H.number_of_nodes()}  |  Edges: {H.number_of_edges()}\n"
        f"IDEs: {n_ide}  |  Catalog: {n_geo_cat}  |  Services: {n_services}\n"
        f"Service-tagged edges: {n_svc_edges}\n"
        f"Service links: {total_svc_w:,} / {total_w:,} total"
    )
    ax.text(
        0.01, 0.01, info,
        transform=ax.transAxes, fontsize=6.5,
        verticalalignment="bottom", color="white", family="monospace",
        bbox=dict(boxstyle="round,pad=0.4", fc="#1A202C", alpha=0.7, lw=0),
    )

    ax.set_title(title, fontsize=12, fontweight="bold", color="white", pad=10)
    ax.axis("off")

    # Auto-fit axes to node positions
    all_x = [pos[n][0] for n in H.nodes()]
    all_y = [pos[n][1] for n in H.nodes()]
    pad_x = max(1.5, (max(all_x) - min(all_x)) * 0.08)
    pad_y = 0.6
    ax.set_xlim(min(all_x) - pad_x, max(all_x) + pad_x)
    ax.set_ylim(min(all_y) - pad_y, max(all_y) + pad_y)


def build_legend(fig):
    """Add a shared type-color legend to the figure."""
    legend_items = []
    for ntype, color in TYPE_COLOR.items():
        legend_items.append(mpatches.Patch(color=color, label=ntype))

    # Edge types
    legend_items.append(mlines.Line2D([], [], color="#4A5568", lw=1.5, label="General link"))
    legend_items.append(mlines.Line2D([], [], color="#FF6B6B", lw=2.5, label="Service link (svc_weight > 0)"))

    fig.legend(
        handles=legend_items,
        loc="lower center",
        ncol=7,
        fontsize=6.5,
        framealpha=0.3,
        facecolor="#1A202C",
        edgecolor="none",
        labelcolor="white",
        title="Node type  |  Edge type",
        title_fontsize=7,
        bbox_to_anchor=(0.5, 0.0),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Hierarchical service-layer visualization for ES and CH catalogs"
    )
    parser.add_argument("--es-run-id",   default=DEFAULT_ES_RUN)
    parser.add_argument("--ch-run-id",   default=DEFAULT_CH_RUN)
    parser.add_argument("--es-port",     type=int, default=DEFAULT_ES_PORT)
    parser.add_argument("--ch-port",     type=int, default=DEFAULT_CH_PORT)
    parser.add_argument("--out",         default=None)
    parser.add_argument("--no-other",    action="store_true",
                        help="Exclude SOCIAL and OTHER nodes (cleaner visualization)")
    parser.add_argument("--top-labels",  type=int, default=30)
    args = parser.parse_args()

    exclude = {"SOCIAL", "OTHER"} if args.no_other else set()

    print(f"Fetching ES graph (run={args.es_run_id}, port={args.es_port})...")
    G_es = fetch_graph(f"bolt://localhost:{args.es_port}", args.es_run_id)
    print(f"  → {G_es.number_of_nodes()} nodes, {G_es.number_of_edges()} edges")

    print(f"Fetching CH graph (run={args.ch_run_id}, port={args.ch_port})...")
    G_ch = fetch_graph(f"bolt://localhost:{args.ch_port}", args.ch_run_id)
    print(f"  → {G_ch.number_of_nodes()} nodes, {G_ch.number_of_edges()} edges")

    fig, axes = plt.subplots(1, 2, figsize=(26, 14))
    fig.patch.set_facecolor("#0D1117")
    for ax in axes:
        ax.set_facecolor("#0D1117")

    draw_layer_graph(
        axes[0], G_es,
        title=f"Spain IDEE  —  {args.es_run_id}",
        top_labels=args.top_labels,
        exclude_types=exclude,
    )
    draw_layer_graph(
        axes[1], G_ch,
        title=f"Switzerland geocat.ch  —  {args.ch_run_id}",
        top_labels=args.top_labels,
        exclude_types=exclude,
    )

    fig.suptitle(
        "Service Layer Architecture: IDE → GeoNetwork → Services",
        fontsize=15, fontweight="bold", color="white", y=0.99,
    )
    fig.text(
        0.5, 0.055,
        "Node size ∝ log(pages crawled + 1)  |  "
        "Node color = semantic layer type  |  "
        "Red edges = OGC service links (service_link_count > 0)  |  "
        "Grey edges = general hyperlinks",
        ha="center", fontsize=7.5, color="#9AA5B4",
    )

    build_legend(fig)

    plt.tight_layout(rect=[0, 0.08, 1, 0.98])

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_no_other" if args.no_other else ""
    out_path = args.out or os.path.join(SCRIPT_DIR, f"service_layers_{ts}{suffix}.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"\nSaved → {out_path}")


if __name__ == "__main__":
    main()
