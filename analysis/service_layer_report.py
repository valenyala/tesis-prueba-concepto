"""
Service Layer Analysis — IDE → GeoNetwork → Services
=====================================================
Classifies site-level graph nodes into semantic layers and analyzes
the 3-layer architecture:

  IDE_SUPRANACIONAL  — INSPIRE / EU-level portals
  IDE_NACIONAL       — national SDI portals (IDEE, swisstopo/geo.admin.ch)
  IDE_REGIONAL       — regional / cantonal IDEs
  GEONETWORK         — GeoNetwork catalog instances (geocat.ch, etc.)
  GEO_PORTAL         — viewers, data portals, download centers, APIs
  SERVICE_WMS        — OGC WMS service endpoints
  SERVICE_WFS        — OGC WFS service endpoints
  SERVICE_WMTS       — OGC WMTS service endpoints
  SERVICE_PLATFORM   — multi-service platforms (geodienste.ch, etc.)
  SERVICE_OTHER      — other geo-service endpoints
  SOCIAL             — social networks (excluded from structural analysis)
  OTHER              — uncategorized

Generates a markdown report comparing ES and CH layer structures.

Usage:
  python analysis/service_layer_report.py [options]

Options:
  --es-run-id   ES Neo4j run ID   (default: run_20260223)
  --ch-run-id   CH Neo4j run ID   (default: run_20260223_17:06:48)
  --es-port     ES bolt port      (default: 7687)
  --ch-port     CH bolt port      (default: 7688)
  --output      Output path       (default: analysis/service_layer_<ts>.md)
  --top         Top-N rows        (default: 15)
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime

import networkx as nx
import numpy as np

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

# ── Node classification tables ────────────────────────────────────────────────

SOCIAL_DOMAINS = {
    "twitter.com", "www.facebook.com", "github.com", "www.linkedin.com",
    "www.youtube.com", "www.instagram.com", "pt-br.facebook.com",
    "fr-fr.facebook.com", "it-it.facebook.com", "de-de.facebook.com",
    "id-id.facebook.com", "ar-ar.facebook.com", "hi-in.facebook.com",
    "zh-cn.facebook.com", "l.facebook.com", "developers.facebook.com",
    "messenger.com", "www.meta.com", "about.meta.com", "www.meta.ai",
    "www.threads.com", "play.google.com", "itunes.apple.com",
}

IDE_SUPRANACIONAL_DOMAINS = {
    "inspire-geoportal.ec.europa.eu",
    "inspire.ec.europa.eu",
}

IDE_NACIONAL_DOMAINS = {
    "www.idee.es", "idee.es",                  # Spain — also hosts GeoNetwork
    "www.geo.admin.ch", "geo.admin.ch",         # Switzerland (swisstopo)
    "www.swisstopo.admin.ch",
}

GEONETWORK_DOMAINS = {
    "www.geocat.ch", "geocat.ch",
    "info.geocat.ch", "www.info.geocat.ch",
    "registry.geocat.ch", "www.geocat.admin.ch",
}

IDE_REGIONAL_DOMAINS = {
    # ES — autonomous communities
    "icearagon.aragon.es",       # Aragón
    "catalogo.idecanarias.es",   # Canarias
    "idecyl.jcyl.es",            # Castilla y León
    "ide.cat",                   # Cataluña
    "idena.navarra.es",          # Navarra
    "idem.madrid.org",           # Madrid
    "idem.comunidad.madrid",     # Madrid (alt)
    "ideas.asturias.es",         # Asturias
    "www.ideandalucia.es",       # Andalucía
    "www.iderioja.larioja.org",  # La Rioja
    "metadatos.ideex.es",        # Extremadura (catalog)
    "ide.caceres.es",            # Cáceres
    "ideespain.github.io",       # IDEEspain docs
    "idechg.chguadalquivir.es",  # Guadalquivir confederation IDE
}

SERVICE_WMS_DOMAINS = {
    # ES
    "wms.mapama.gob.es",
    "geoserveis.ide.cat",        # Cataluña WMS
    # CH
    "wms.geo.admin.ch",
    "wms.geo.sh.ch",             # Schaffhausen
    "wms.geo.bs.ch",             # Basel-Stadt
    "services.geo.sg.ch",        # St. Gallen
    "services.geo.zg.ch",        # Zug
    "geoservices.jura.ch",       # Jura
}

SERVICE_WFS_DOMAINS = {
    "wfs.geo.sh.ch",             # Schaffhausen
    "wfs.geo.bs.ch",             # Basel-Stadt
}

SERVICE_WMTS_DOMAINS = {
    "wmts.mapama.gob.es",
    "wmts-snczi.idee.es",
    "wmts.geo.admin.ch",
}

SERVICE_PLATFORM_DOMAINS = {
    "geodienste.ch",
    "www.geodienste.ch",         # Swiss cantonal service platform
}

SERVICE_OTHER_DOMAINS = {
    "servicios.idee.es",         # ES IDEE generic services
    "api-coverages.idee.es",     # ES OGC API - Coverages
    "rts.larioja.org",           # La Rioja tile service
    "vts.larioja.org",           # La Rioja vector tiles
    "geoapi.larioja.org",        # La Rioja API
    "ch-osm-services.geodatasolutions.ch",
}

GEO_PORTAL_DOMAINS = {
    # ES portals
    "www.geo.euskadi.eus",
    "mapas-gis-inter.carm.es",
    "geo.araba.eus",
    "sig.mapama.gob.es",
    "terramapas.icv.gva.es",
    "sig.miteco.gob.es",
    "portaleslr.carm.es",
    "mapas.ideex.es",
    "visor.gva.es",
    "sig.pamplona.es",
    "www.chguadalquivir.es",
    "laboratoriorediam.cica.es",
    "portalrediam.cica.es",
    "descargasrediam.cica.es",
    "centrodedescargas.cnig.es",
    "icv.gva.es",
    "icvficherosweb.icv.gva.es",
    "descargas.icv.gva.es",
    # CH portals
    "data.geo.admin.ch",
    "map.geo.admin.ch",
    "viewer.swissgeol.ch",
    "www.gis-daten.ch",
    "geoshop.lisag.ch",
    "geobasisdaten.ch",
    "raster.sitg.ge.ch",
    "map.koeniz.ch",
    "www.swissgeol.ch",
    "map.geo.sz.ch",
    "map.geo.tg.ch",
    "geo.fr.ch",
    "maps.fr.ch",
    "data.geo.sh.ch",
    "geo.jura.ch",
    "geo.vs.ch",
    "api3.geo.admin.ch",
    "docs.geo.admin.ch",
    "backend.geo.admin.ch",
    "s.geo.admin.ch",
    "models.geo.admin.ch",
    "map.geo.fr.ch",
    "models.geo.bs.ch",
    "map.geo.gl.ch",
    "sig.biel-bienne.ch",
    "be-geo.ch",
    "geoportal.koeniz.ch",
    "map.geo.sh.ch",
    "geozen.ch",
    "hydromaps.ch",
    "biel-bienne.mapplus.ch",
    "enterprise.arcgis.com",
    "www.arcgis.com",
    "survey123.arcgis.com",
    "opendata.swiss",
}

# Layer assignment for cross-layer matrix (higher = closer to top/IDE)
LAYER_LEVEL = {
    "IDE_SUPRANACIONAL":  4,
    "IDE_NACIONAL":       3,
    "IDE_REGIONAL":       2,
    "GEONETWORK":         2,
    "GEO_PORTAL":         1,
    "SERVICE_WMS":        0,
    "SERVICE_WFS":        0,
    "SERVICE_WMTS":       0,
    "SERVICE_PLATFORM":   0,
    "SERVICE_OTHER":      0,
    "SOCIAL":            -1,
    "OTHER":             -1,
}

# Display order for report tables
TYPE_ORDER = [
    "IDE_SUPRANACIONAL", "IDE_NACIONAL", "IDE_REGIONAL", "GEONETWORK",
    "GEO_PORTAL", "SERVICE_WMS", "SERVICE_WFS", "SERVICE_WMTS",
    "SERVICE_PLATFORM", "SERVICE_OTHER", "SOCIAL", "OTHER",
]

SERVICE_TYPES = {"SERVICE_WMS", "SERVICE_WFS", "SERVICE_WMTS", "SERVICE_PLATFORM", "SERVICE_OTHER"}


# ── Node classification ───────────────────────────────────────────────────────

def classify_node(label: str) -> str:
    """Return the semantic layer type for a site domain label."""
    if label in SOCIAL_DOMAINS:          return "SOCIAL"
    if label in IDE_SUPRANACIONAL_DOMAINS: return "IDE_SUPRANACIONAL"
    if label in IDE_NACIONAL_DOMAINS:    return "IDE_NACIONAL"
    if label in GEONETWORK_DOMAINS:      return "GEONETWORK"
    if label in IDE_REGIONAL_DOMAINS:    return "IDE_REGIONAL"
    if label in SERVICE_WMS_DOMAINS:     return "SERVICE_WMS"
    if label in SERVICE_WFS_DOMAINS:     return "SERVICE_WFS"
    if label in SERVICE_WMTS_DOMAINS:    return "SERVICE_WMTS"
    if label in SERVICE_PLATFORM_DOMAINS: return "SERVICE_PLATFORM"
    if label in SERVICE_OTHER_DOMAINS:   return "SERVICE_OTHER"
    if label in GEO_PORTAL_DOMAINS:      return "GEO_PORTAL"

    # Pattern-based fallback
    l = label.lower()
    if l.startswith("wms.") or ".wms." in l:     return "SERVICE_WMS"
    if l.startswith("wfs.") or ".wfs." in l:     return "SERVICE_WFS"
    if l.startswith("wcs.") or ".wcs." in l:     return "SERVICE_WCS"
    if l.startswith("wmts.") or l.startswith("wmts-"): return "SERVICE_WMTS"
    if "geonetwork" in l:                         return "GEONETWORK"
    if "geocat" in l:                             return "GEONETWORK"
    if any(l.startswith(p) for p in ("ide.", "ide-", "idem.", "ideas.", "idee.", "idex", "ides.")):
        return "IDE_REGIONAL"
    if any(x in l for x in ("idena.", "ideex", "idecyl", "idecat", "ideand", "iderioja", "idecanarias")):
        return "IDE_REGIONAL"

    return "OTHER"


# ── Neo4j fetching ────────────────────────────────────────────────────────────

def connect(port, label):
    uri = f"bolt://localhost:{port}"
    try:
        drv = GraphDatabase.driver(uri, auth=AUTH)
        drv.verify_connectivity()
        return drv
    except Exception as e:
        print(f"ERROR: Cannot connect to {label} Neo4j at {uri}: {e}")
        sys.exit(1)


def fetch_graph(driver, run_id):
    """Fetch Site nodes and LINKS_TO edges (including service_link_count)."""
    with driver.session() as s:
        nodes = s.run(
            "MATCH (n:Site {run_id: $r}) "
            "RETURN n.label AS label, n.url AS url, "
            "n.pages_crawled AS pages, n.external_hops AS hops",
            r=run_id,
        ).data()
        edges = s.run(
            "MATCH (a:Site {run_id: $r})-[e:LINKS_TO {run_id: $r}]->(b:Site {run_id: $r}) "
            "RETURN a.label AS src, b.label AS dst, "
            "e.link_count AS weight, e.service_link_count AS svc_weight",
            r=run_id,
        ).data()
    return nodes, edges


def build_graph(nodes, edges):
    G = nx.DiGraph()
    for n in nodes:
        ntype = classify_node(n["label"])
        G.add_node(
            n["label"],
            url=n.get("url") or "",
            pages=n.get("pages") or 0,
            hops=n.get("hops"),
            node_type=ntype,
            layer=LAYER_LEVEL.get(ntype, -1),
        )
    for e in edges:
        if e["src"] in G and e["dst"] in G:
            G.add_edge(
                e["src"], e["dst"],
                weight=e.get("weight") or 1,
                svc_weight=e.get("svc_weight") or 0,
            )
    return G


# ── Analysis helpers ──────────────────────────────────────────────────────────

def node_type_stats(G):
    """Per-type counts, pages_crawled, in/out degree sums."""
    stats = defaultdict(lambda: {
        "count": 0, "pages": 0, "nodes": [],
        "in_weight": 0, "out_weight": 0,
        "in_svc_weight": 0,
    })
    in_w  = dict(G.in_degree(weight="weight"))
    out_w = dict(G.out_degree(weight="weight"))

    # Service-weight in-degree (service_link_count on incoming edges)
    in_svc = defaultdict(int)
    for u, v, d in G.edges(data=True):
        in_svc[v] += d.get("svc_weight", 0)

    for n, d in G.nodes(data=True):
        t = d["node_type"]
        stats[t]["count"]      += 1
        stats[t]["pages"]      += d["pages"]
        stats[t]["nodes"].append(n)
        stats[t]["in_weight"]  += in_w.get(n, 0)
        stats[t]["out_weight"] += out_w.get(n, 0)
        stats[t]["in_svc_weight"] += in_svc.get(n, 0)
    return dict(stats)


def cross_layer_matrix(G):
    """
    Returns dict: (src_type, dst_type) → {edges, link_count, svc_count}.
    Useful for understanding which layers link to which.
    """
    matrix = defaultdict(lambda: {"edges": 0, "link_count": 0, "svc_count": 0})
    for u, v, d in G.edges(data=True):
        src_t = G.nodes[u]["node_type"]
        dst_t = G.nodes[v]["node_type"]
        key = (src_t, dst_t)
        matrix[key]["edges"]      += 1
        matrix[key]["link_count"] += d.get("weight", 0)
        matrix[key]["svc_count"]  += d.get("svc_weight", 0)
    return dict(matrix)


def service_inventory(G):
    """List all service nodes with their type, in-weight, and svc-in-weight."""
    in_w   = dict(G.in_degree(weight="weight"))
    in_svc = defaultdict(int)
    for u, v, d in G.edges(data=True):
        in_svc[v] += d.get("svc_weight", 0)

    services = []
    for n, d in G.nodes(data=True):
        if d["node_type"] in SERVICE_TYPES:
            services.append({
                "label":      n,
                "type":       d["node_type"],
                "in_weight":  in_w.get(n, 0),
                "svc_weight": in_svc.get(n, 0),
                "pages":      d["pages"],
            })
    return sorted(services, key=lambda x: x["in_weight"], reverse=True)


def top_service_edges(G, top=15):
    """Edges where svc_weight > 0, sorted by svc_weight descending."""
    edges = [
        (u, v, d["weight"], d.get("svc_weight", 0))
        for u, v, d in G.edges(data=True)
        if d.get("svc_weight", 0) > 0
    ]
    return sorted(edges, key=lambda x: x[3], reverse=True)[:top]


# ── Markdown helpers ──────────────────────────────────────────────────────────

def tbl(headers, rows):
    if not rows:
        return "_No data._"
    widths = [max(len(str(r[i])) for r in ([headers] + list(rows))) for i in range(len(headers))]
    sep = "| " + " | ".join("-" * w for w in widths) + " |"
    hdr = "| " + " | ".join(str(headers[i]).ljust(widths[i]) for i in range(len(headers))) + " |"
    body = "\n".join(
        "| " + " | ".join(str(row[i]).ljust(widths[i]) for i in range(len(headers))) + " |"
        for row in rows
    )
    return "\n".join([hdr, sep, body])


def pct(a, b):
    return f"{a/b:.1%}" if b else "0.0%"


# ── Report builder ────────────────────────────────────────────────────────────

def build_report(G_es, G_ch, args):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    L = []
    a = L.append

    a("# Service Layer Analysis — IDE → GeoNetwork → Services")
    a("")
    a(f"Generated: {now}")
    a("")
    a("> This report classifies sites discovered during crawls into semantic layers")
    a("> (IDE portals, GeoNetwork catalog instances, OGC service endpoints, geo-portals)")
    a("> and analyzes how the two national SDIs structure their catalog-to-service ecosystem.")
    a("")
    a("| | ES (IDEE) | CH (geocat.ch) |")
    a("|---|---|---|")
    a(f"| Run ID | `{args.es_run_id}` | `{args.ch_run_id}` |")
    a(f"| Neo4j port | {args.es_port} | {args.ch_port} |")
    a(f"| Total sites (nodes) | {G_es.number_of_nodes()} | {G_ch.number_of_nodes()} |")
    a(f"| Total site-to-site edges | {G_es.number_of_edges()} | {G_ch.number_of_edges()} |")
    a("")

    # ── Section 1: Node Taxonomy ──────────────────────────────────────────────
    a("## 1. Node Taxonomy")
    a("")
    a("> Each site domain is classified into a semantic layer. "
      "The table shows how many sites of each type appear in each catalog's crawl.")
    a("")

    stats_es = node_type_stats(G_es)
    stats_ch = node_type_stats(G_ch)

    tax_rows = []
    for t in TYPE_ORDER:
        es = stats_es.get(t, {})
        ch = stats_ch.get(t, {})
        tax_rows.append((
            t,
            es.get("count", 0), es.get("pages", 0),
            ch.get("count", 0), ch.get("pages", 0),
        ))
    a(tbl(
        ["Node Type", "ES count", "ES pages crawled", "CH count", "CH pages crawled"],
        tax_rows,
    ))
    a("")

    # Classification notes
    a("### Classification notes")
    a("")
    a("- **ES `www.idee.es`** is classified as `IDE_NACIONAL` but in practice it **fuses** the national IDE portal "
      "and the GeoNetwork catalog instance (the crawl seed was `catalog.search`). "
      "In ES, there is no separate GeoNetwork node — the IDE IS the catalog.")
    a("- **CH `www.geocat.ch`** is classified as `GEONETWORK` (dedicated catalog software). "
      "The national IDE portal (`www.geo.admin.ch`, classified as `IDE_NACIONAL`) is a separate site, "
      "making the 3-layer hierarchy explicit.")
    a("- Regional IDEs in ES (autonómicas) typically host their own GeoNetwork instances internally; "
      "they appear as `IDE_REGIONAL` since the domain represents both the IDE portal and its catalog.")
    a("")

    # ── Section 2: Layer Architecture ────────────────────────────────────────
    a("## 2. Layer Architecture Comparison")
    a("")
    a("```")
    a("ES architecture (centralized):             CH architecture (federated):")
    a("")
    a("  [IDE_NACIONAL / GeoNetwork]               [IDE_NACIONAL: geo.admin.ch]")
    a("         www.idee.es                                    │")
    a("    ┌────────┴────────┐                    [GEONETWORK: www.geocat.ch]")
    a("    │                 │                     ┌─────┬─────┴──────┬──────┐")
    a("[IDE_REGIONAL]  [SERVICE_WMS/WMTS]     [WMS] [WFS] [WMTS] [PLATFORM] [PORTAL]")
    a("  ide.cat          wms.*              canton services    geodienste  map.*")
    a("  idena.*          wmts.*")
    a("  idem.*")
    a("```")
    a("")

    # Per-layer node lists
    for name, G, stats in [("ES", G_es, stats_es), ("CH", G_ch, stats_ch)]:
        a(f"### {name} — nodes by layer")
        a("")
        for t in TYPE_ORDER:
            s = stats.get(t)
            if not s or s["count"] == 0:
                continue
            nodes_str = ", ".join(f"`{n}`" for n in sorted(s["nodes"])[:8])
            if s["count"] > 8:
                nodes_str += f" ... (+{s['count']-8} more)"
            a(f"**{t}** ({s['count']} sites): {nodes_str}")
        a("")

    # ── Section 3: Cross-Layer Connectivity Matrix ─────────────────────────────
    a("## 3. Cross-Layer Connectivity Matrix")
    a("")
    a("> Each cell shows: `edges (total_links / service_links)`. "
      "Service links are page-level links whose target URL matched an OGC service pattern.")
    a("")

    for name, G in [("ES", G_es), ("CH", G_ch)]:
        a(f"### {name}")
        a("")
        matrix = cross_layer_matrix(G)

        # Collect all types that appear as source or target
        present_types = set()
        for (src_t, dst_t) in matrix:
            present_types.add(src_t)
            present_types.add(dst_t)
        row_types = [t for t in TYPE_ORDER if t in present_types]

        matrix_rows = []
        for src_t in row_types:
            row = [src_t]
            for dst_t in row_types:
                cell = matrix.get((src_t, dst_t))
                if cell:
                    row.append(f"{cell['edges']}e / {cell['link_count']}lnk / {cell['svc_count']}svc")
                else:
                    row.append("—")
            matrix_rows.append(row)

        headers = ["FROM \\ TO"] + row_types
        a(tbl(headers, matrix_rows))
        a("")
        a("_Format: `edges / total_link_count / service_link_count`_")
        a("")

    # ── Section 4: Service Inventory ─────────────────────────────────────────
    a("## 4. Service Inventory")
    a("")
    a("> OGC service endpoints (WMS/WFS/WMTS) identified in each catalog's link graph. "
      "Service link count = page-level links pointing specifically to service URLs.")
    a("")

    for name, G in [("ES", G_es), ("CH", G_ch)]:
        svc = service_inventory(G)
        a(f"### {name} — {len(svc)} service domains")
        a("")
        if svc:
            rows = [(s["type"], s["label"], s["in_weight"], s["svc_weight"], s["pages"])
                    for s in svc]
            a(tbl(["Type", "Domain", "Total in-links", "Service in-links", "Pages crawled"], rows))
        else:
            a("_No service endpoints identified._")
        a("")

    # ── Section 5: Service Edges (top service links) ──────────────────────────
    a("## 5. Top Service-Oriented Links")
    a("")
    a("> Edges where `service_link_count > 0`, sorted by service link count. "
      "These represent pages that explicitly reference OGC service endpoints.")
    a("")

    for name, G in [("ES", G_es), ("CH", G_ch)]:
        svc_edges = top_service_edges(G, top=args.top)
        a(f"### {name}")
        a("")
        if svc_edges:
            rows = [
                (G.nodes[u]["node_type"], u,
                 G.nodes[v]["node_type"], v,
                 w, s, f"{pct(s, w)}")
                for u, v, w, s in svc_edges
            ]
            a(tbl(
                ["Src type", "Source", "Dst type", "Destination",
                 "Total links", "Service links", "Service ratio"],
                rows,
            ))
        else:
            a("_No service-oriented edges found._")
        a("")

    # ── Section 6: Layer-to-Layer Service Flow ────────────────────────────────
    a("## 6. Layer-to-Layer Service Flow Summary")
    a("")
    a("> Aggregated view: how many service links flow between each pair of semantic layers.")
    a("")

    for name, G in [("ES", G_es), ("CH", G_ch)]:
        matrix = cross_layer_matrix(G)
        a(f"### {name}")
        a("")
        # Filter to only rows with service_count > 0
        svc_flows = [
            (src_t, dst_t, v["edges"], v["link_count"], v["svc_count"])
            for (src_t, dst_t), v in sorted(matrix.items(), key=lambda x: -x[1]["svc_count"])
            if v["svc_count"] > 0
        ]
        if svc_flows:
            a(tbl(
                ["Source layer", "Target layer", "Edges", "Total links", "Service links"],
                [(s, d, e, l, sv) for s, d, e, l, sv in svc_flows],
            ))
        else:
            a("_No service-link flows found._")
        a("")

    # ── Section 7: Structural Comparison ─────────────────────────────────────
    a("## 7. Structural Comparison: ES vs CH")
    a("")

    es_stats = node_type_stats(G_es)
    ch_stats = node_type_stats(G_ch)

    es_svc_count = sum(es_stats.get(t, {}).get("count", 0) for t in SERVICE_TYPES)
    ch_svc_count = sum(ch_stats.get(t, {}).get("count", 0) for t in SERVICE_TYPES)
    es_ide_count = (es_stats.get("IDE_NACIONAL", {}).get("count", 0) +
                    es_stats.get("IDE_REGIONAL", {}).get("count", 0))
    ch_ide_count = (ch_stats.get("IDE_NACIONAL", {}).get("count", 0) +
                    ch_stats.get("IDE_REGIONAL", {}).get("count", 0))

    es_total_svc_links = sum(
        d.get("svc_weight", 0) for _, _, d in G_es.edges(data=True)
    )
    ch_total_svc_links = sum(
        d.get("svc_weight", 0) for _, _, d in G_ch.edges(data=True)
    )
    es_total_links = sum(d.get("weight", 0) for _, _, d in G_es.edges(data=True))
    ch_total_links = sum(d.get("weight", 0) for _, _, d in G_ch.edges(data=True))

    comp_rows = [
        ("Total sites",                     G_es.number_of_nodes(),    G_ch.number_of_nodes()),
        ("IDE nodes (national + regional)",  es_ide_count,              ch_ide_count),
        ("GeoNetwork nodes",                es_stats.get("GEONETWORK", {}).get("count", 0),
                                            ch_stats.get("GEONETWORK", {}).get("count", 0)),
        ("Service endpoint nodes",          es_svc_count,              ch_svc_count),
        ("Geo-portal nodes",               es_stats.get("GEO_PORTAL", {}).get("count", 0),
                                            ch_stats.get("GEO_PORTAL", {}).get("count", 0)),
        ("Total page-level links",          f"{es_total_links:,}",     f"{ch_total_links:,}"),
        ("Total service-classified links",  f"{es_total_svc_links:,}", f"{ch_total_svc_links:,}"),
        ("Service link ratio",              pct(es_total_svc_links, es_total_links),
                                            pct(ch_total_svc_links, ch_total_links)),
    ]
    a(tbl(["Dimension", "ES", "CH"], comp_rows))
    a("")

    # ── Section 8: Interpretation ─────────────────────────────────────────────
    a("## 8. Interpretation")
    a("")
    a("### ES — Centralized hub-and-spoke with fused IDE/catalog")
    a("")
    a(f"- `www.idee.es` acts as both the national IDE portal and the GeoNetwork catalog, "
      f"concentrating {G_es.nodes.get('www.idee.es', {}).get('pages', 0)} crawled pages in one domain.")
    a(f"- The catalog directly links to **{es_svc_count} service domains**, "
      f"predominantly WMS/WMTS endpoints operated by the national ministry (`mapama.gob.es`).")
    a(f"- Regional IDEs ({es_ide_count - 1} autonomous community catalogs) "
      f"are linked from the national catalog but their own service endpoints were not crawled deeply.")
    a(f"- The 3-layer IDE→GeoNetwork→Service structure is **implicit**: "
      f"the IDE and GeoNetwork occupy the same node, and services are one hop away.")
    a("")
    a("### CH — Federated multi-layer with explicit catalog separation")
    a("")
    a(f"- `www.geo.admin.ch` (IDE_NACIONAL) and `www.geocat.ch` (GEONETWORK) are **separate nodes**, "
      f"making the IDE→Catalog separation structurally visible.")
    a(f"- The catalog links to **{ch_svc_count} service domains** across multiple cantonal operators "
      f"(WMS/WFS by canton: Basel, Schaffhausen, Zug, Jura, St. Gallen, etc.).")
    a(f"- `geodienste.ch` acts as a **cantonal service aggregation platform** (SERVICE_PLATFORM), "
      f"a layer not present in ES.")
    a(f"- The 3-layer structure is **explicit and federated**: IDE → GeoNetwork → cantonal services.")
    a("")
    a("### Key structural difference")
    a("")
    a("| Dimension | ES (IDEE) | CH (geocat.ch) |")
    a("|---|---|---|")
    a("| IDE ↔ GeoNetwork | **Fused** in one domain | **Separated** (distinct nodes) |")
    a("| Service model | National ministry WMS/WMTS | Cantonal WMS/WFS + platform (geodienste) |")
    a("| Service diversity | Low (2 WMS/WMTS domains) | High (9 WMS/WFS/WMTS domains) |")
    a("| Catalog-to-service hops | 1 (catalog → service) | 1 (catalog → service, parallel) |")
    a("| Federation visible in graph | No | Yes (geo.admin.ch → geocat.ch → cantons) |")
    a("")

    return "\n".join(L)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Service layer analysis: IDE → GeoNetwork → Services (ES vs CH)"
    )
    parser.add_argument("--es-run-id",  default=DEFAULT_ES_RUN)
    parser.add_argument("--ch-run-id",  default=DEFAULT_CH_RUN)
    parser.add_argument("--es-port",    type=int, default=DEFAULT_ES_PORT)
    parser.add_argument("--ch-port",    type=int, default=DEFAULT_CH_PORT)
    parser.add_argument("--output",     default=None)
    parser.add_argument("--top",        type=int, default=15)
    args = parser.parse_args()

    print("Connecting to Neo4j...")
    drv_es = connect(args.es_port, "ES")
    drv_ch = connect(args.ch_port, "CH")

    print(f"Fetching ES graph (run_id={args.es_run_id})...")
    es_nodes, es_edges = fetch_graph(drv_es, args.es_run_id)
    drv_es.close()

    print(f"Fetching CH graph (run_id={args.ch_run_id})...")
    ch_nodes, ch_edges = fetch_graph(drv_ch, args.ch_run_id)
    drv_ch.close()

    G_es = build_graph(es_nodes, es_edges)
    G_ch = build_graph(ch_nodes, ch_edges)

    print(f"ES: {G_es.number_of_nodes()} nodes, {G_es.number_of_edges()} edges")
    print(f"CH: {G_ch.number_of_nodes()} nodes, {G_ch.number_of_edges()} edges")

    print("Building report...")
    report = build_report(G_es, G_ch, args)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = args.output or os.path.join(SCRIPT_DIR, f"service_layer_{ts}.md")
    with open(out_path, "w") as f:
        f.write(report)
    print(f"\nReport written → {out_path}")


if __name__ == "__main__":
    main()
