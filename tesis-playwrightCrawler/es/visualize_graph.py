import json
import sys
import os
import networkx as nx
import matplotlib.pyplot as plt

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def load_graph(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    G = nx.DiGraph()

    for node in data['nodes']:
        node_id = node.pop('id')
        G.add_node(node_id, **node)

    for edge in data['edges']:
        source = edge.pop('source')
        target = edge.pop('target')
        G.add_edge(source, target, **edge)

    return G, data.get('metadata', {})


def get_node_color(node_id, attrs):
    if '#/metadata/' in node_id:
        return '#3498db'   # Blue
    elif '#/search' in node_id:
        return '#2ecc71'   # Green
    elif attrs.get('type') == 'external':
        return '#e74c3c'   # Red
    elif attrs.get('type') == 'download':
        return '#f39c12'   # Orange
    else:
        return '#9b59b6'   # Purple


def get_edge_color(edge_type):
    colors = {
        'metadata': '#3498db',
        'external': '#e74c3c',
        'download': '#f39c12',
    }
    return colors.get(edge_type, '#bdc3c7')


def visualize(G, output_file=None):
    if G.number_of_nodes() == 0:
        print("No nodes to visualize")
        return

    fig, ax = plt.subplots(figsize=(20, 16))

    node_colors = [get_node_color(n, G.nodes[n]) for n in G.nodes()]
    edge_colors = [get_edge_color(d.get('type', '')) for _, _, d in G.edges(data=True)]

    pos = nx.spring_layout(G, k=2, iterations=50, seed=42)

    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=100, alpha=0.7, ax=ax)
    nx.draw_networkx_edges(G, pos, edge_color=edge_colors, alpha=0.3, arrows=True, arrowsize=10, ax=ax)

    # Labels for highly-connected nodes only
    labels = {}
    for node in G.nodes():
        if G.degree(node) > 3 or '#/search' in node:
            labels[node] = G.nodes[node].get('label', '')[:20]
    nx.draw_networkx_labels(G, pos, labels, font_size=8, ax=ax)

    legend_elements = [
        plt.scatter([], [], c='#3498db', s=100, label='Metadata'),
        plt.scatter([], [], c='#2ecc71', s=100, label='Search'),
        plt.scatter([], [], c='#e74c3c', s=100, label='External'),
        plt.scatter([], [], c='#f39c12', s=100, label='Downloads'),
        plt.scatter([], [], c='#9b59b6', s=100, label='Other internal'),
    ]
    ax.legend(handles=legend_elements, loc='upper left')
    ax.set_title('Web Structure Graph - Spain IDEE GeoNetwork', fontsize=16)
    ax.axis('off')
    fig.tight_layout()

    if output_file:
        fig.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"Saved to {output_file}")

    plt.show()


def print_stats(G, metadata):
    print("=" * 50)
    print("WEB GRAPH STATISTICS")
    print("=" * 50)
    print(f"Nodes: {G.number_of_nodes()}")
    print(f"Edges: {G.number_of_edges()}")

    from collections import defaultdict
    type_counts = defaultdict(int)
    for node, attrs in G.nodes(data=True):
        if '#/metadata/' in node:
            type_counts['metadata'] += 1
        elif '#/search' in node:
            type_counts['search'] += 1
        elif attrs.get('type') == 'external':
            type_counts['external'] += 1
        elif attrs.get('type') == 'download':
            type_counts['download'] += 1
        else:
            type_counts['other'] += 1

    print("\nNodes by type:")
    for t, count in sorted(type_counts.items()):
        print(f"  {t}: {count}")

    print("\nTop 5 by out-degree:")
    for node, deg in sorted(G.out_degree(), key=lambda x: x[1], reverse=True)[:5]:
        label = G.nodes[node].get('label', node[:40])
        print(f"  {label}: {deg}")

    print("\nTop 5 by in-degree:")
    for node, deg in sorted(G.in_degree(), key=lambda x: x[1], reverse=True)[:5]:
        label = G.nodes[node].get('label', node[:40])
        print(f"  {label}: {deg}")


if __name__ == "__main__":
    json_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(SCRIPT_DIR, 'web_graph.json')

    if not os.path.exists(json_path):
        print(f"File not found: {json_path}")
        sys.exit(1)

    print(f"Loading graph from {json_path}...")
    G, metadata = load_graph(json_path)

    print_stats(G, metadata)
    output_png = os.path.join(SCRIPT_DIR, 'web_structure.png')
    visualize(G, output_file=output_png)
