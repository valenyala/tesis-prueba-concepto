import asyncio
import json
import os
import re
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

BASE_URL = "https://visualizador.ide.uy/geonetwork/srv/spa/catalog.search"

SKIP_URL_PATTERNS = ['catalog.signin', 'signout', '/sign-in', '/login', '#/edit']

class WebGraphCrawler:
    def __init__(self, max_depth=2, external_max_hops=2):
        self.graph = nx.DiGraph()
        self.visited = set()
        self.max_depth = max_depth
        self.external_max_hops = external_max_hops
        self.page_data = {}  # Store metadata about each page

    def is_external(self, url):
        """Check if URL is external to the base site"""
        return urlparse(url).netloc != urlparse(BASE_URL).netloc

    def should_skip_url(self, url):
        """Check if this URL should be skipped (sign-in, edit, etc.)"""
        return any(pattern in url for pattern in SKIP_URL_PATTERNS)

    def normalize_url(self, url, current_url=BASE_URL):
        """Normalize URL to absolute form"""
        if url.startswith('#'):
            # Hash fragment - append to base URL
            return BASE_URL + url
        elif url.startswith('/'):
            # Relative to root
            parsed = urlparse(current_url)
            return f"{parsed.scheme}://{parsed.netloc}{url}"
        elif url.startswith('http'):
            return url
        else:
            return urljoin(current_url, url)

    def get_page_label(self, url):
        """Extract a readable label from URL"""
        if '#/metadata/' in url:
            uuid = url.split('#/metadata/')[-1][:8]
            return f"metadata/{uuid}..."
        elif '#/search' in url:
            return "search"
        elif 'topicCat' in url:
            match = re.search(r'topicCat%2F(\w+)', url)
            if match:
                return f"category:{match.group(1)}"
        parsed = urlparse(url)
        # Use the last path segment(s) to avoid cutting words like "geonetwork"
        path = parsed.path.rstrip('/')
        if path:
            segments = path.split('/')
            # Take up to the last 3 segments for a readable label
            label = '/'.join(segments[-3:]) if len(segments) > 3 else path
            if self.is_external(url):
                label = parsed.netloc + '/' + label.lstrip('/')
            return label
        return parsed.netloc

    def categorize_link(self, url):
        """Categorize the type of link"""
        if '#/metadata/' in url:
            return 'metadata'
        elif '#/search' in url:
            return 'search'
        elif 'download' in url.lower() or url.endswith(('.zip', '.pdf', '.csv', '.json', '.xml')):
            return 'download'
        elif urlparse(url).netloc != urlparse(BASE_URL).netloc:
            return 'external'
        else:
            return 'internal'

    async def extract_links(self, page, current_url):
        """Extract all links from the current page"""
        links = []

        # Get all anchor tags
        anchors = await page.query_selector_all('a[href]')
        for anchor in anchors:
            href = await anchor.get_attribute('href')
            if href and not href.startswith('javascript:') and href != '#':
                full_url = self.normalize_url(href, current_url)
                # Skip non-HTTPS links
                if not full_url.startswith('https://'):
                    continue
                # Skip sign-in, edit, and other non-content pages
                if self.should_skip_url(href) or self.should_skip_url(full_url):
                    continue
                text = await anchor.inner_text()
                links.append({
                    'url': full_url,
                    'text': text.strip()[:50] if text else '',
                    'type': self.categorize_link(full_url)
                })

        # Get buttons with ng-click or data attributes that might navigate
        buttons = await page.query_selector_all('button[data-ng-click], button[ng-click]')
        for button in buttons:
            ng_click = await button.get_attribute('data-ng-click') or await button.get_attribute('ng-click')
            if ng_click:
                text = await button.inner_text()
                links.append({
                    'url': f"action:{ng_click}",
                    'text': text.strip()[:50] if text else '',
                    'type': 'action'
                })

        return links

    async def crawl_page(self, page, url, depth=0, external_hops=0):
        """Crawl a single page and extract its links"""
        if url in self.visited:
            return
        if depth > self.max_depth:
            return
        if self.is_external(url) and external_hops > self.external_max_hops:
            return
        if self.should_skip_url(url):
            return

        self.visited.add(url)
        print(f"[Depth {depth}] Crawling: {self.get_page_label(url)} (external_hops={external_hops})")

        try:
            await page.goto(url, wait_until='networkidle', timeout=30000)
            await asyncio.sleep(2)  # Wait for dynamic content

            # Check if the page redirected to a sign-in or edit page
            current_url_after_nav = page.url
            if self.should_skip_url(current_url_after_nav):
                print(f"  Skipping: redirected to unwanted page ({current_url_after_nav})")
                return

            # Get page title
            title = await page.title()

            # Extract links
            links = await self.extract_links(page, url)

            # Store page data
            self.page_data[url] = {
                'title': title,
                'label': self.get_page_label(url),
                'links_count': len(links),
                'depth': depth,
                'external_hops': external_hops
            }

            # Add node to graph
            self.graph.add_node(url,
                               label=self.get_page_label(url),
                               title=title,
                               depth=depth)

            # Process links and add edges
            links_to_follow = []
            for link in links:
                link_url = link['url']
                link_type = link['type']

                # Add edge to graph
                self.graph.add_edge(url, link_url,
                                   type=link_type,
                                   text=link['text'])

                # Add the target node if not exists
                if not self.graph.has_node(link_url):
                    self.graph.add_node(link_url,
                                       label=self.get_page_label(link_url),
                                       type=link_type)

                # Queue links for crawling (including external, but not downloads/actions)
                if link_type in ['metadata', 'search', 'internal', 'external'] and link_url not in self.visited:
                    links_to_follow.append((link_url, link_type))

            # Follow links up to max_depth, with external hop limiting
            for link_url, link_type in links_to_follow[:10]:  # Limit to prevent explosion
                new_external_hops = external_hops + 1 if self.is_external(link_url) else external_hops
                await self.crawl_page(page, link_url, depth + 1, new_external_hops)

        except Exception as e:
            print(f"  Error crawling {url}: {e}")
            self.page_data[url] = {'error': str(e)}

    async def crawl_from_seeds(self, seed_urls):
        """Start crawling from a list of seed URLs"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            for i, url in enumerate(seed_urls):
                print(f"\n=== Processing seed {i+1}/{len(seed_urls)} ===")
                await self.crawl_page(page, url, depth=0)

            await browser.close()

    def save_graph(self, filename='web_graph.json'):
        """Save graph data to JSON"""
        data = {
            'nodes': [],
            'edges': [],
            'metadata': self.page_data
        }

        for node in self.graph.nodes(data=True):
            data['nodes'].append({
                'id': node[0],
                **node[1]
            })

        for edge in self.graph.edges(data=True):
            data['edges'].append({
                'source': edge[0],
                'target': edge[1],
                **edge[2]
            })

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"\nGraph saved to {filename}")
        print(f"Total nodes: {self.graph.number_of_nodes()}")
        print(f"Total edges: {self.graph.number_of_edges()}")

    def visualize_graph(self, output_file='web_structure.png', show=True):
        """Create a visualization of the web graph"""
        if self.graph.number_of_nodes() == 0:
            print("No nodes to visualize")
            return

        plt.figure(figsize=(20, 16))

        # Color nodes by type
        node_colors = []
        for node in self.graph.nodes():
            if '#/metadata/' in node:
                node_colors.append('#3498db')  # Blue for metadata
            elif '#/search' in node:
                node_colors.append('#2ecc71')  # Green for search
            elif 'external' in self.graph.nodes[node].get('type', ''):
                node_colors.append('#e74c3c')  # Red for external
            elif 'download' in self.graph.nodes[node].get('type', ''):
                node_colors.append('#f39c12')  # Orange for downloads
            else:
                node_colors.append('#9b59b6')  # Purple for other internal

        # Use spring layout for better visualization
        pos = nx.spring_layout(self.graph, k=2, iterations=50, seed=42)

        # Draw nodes
        nx.draw_networkx_nodes(self.graph, pos,
                               node_color=node_colors,
                               node_size=100,
                               alpha=0.7)

        # Draw edges with different colors by type
        edge_colors = []
        for u, v, data in self.graph.edges(data=True):
            edge_type = data.get('type', 'internal')
            if edge_type == 'metadata':
                edge_colors.append('#3498db')
            elif edge_type == 'external':
                edge_colors.append('#e74c3c')
            elif edge_type == 'download':
                edge_colors.append('#f39c12')
            else:
                edge_colors.append('#bdc3c7')

        nx.draw_networkx_edges(self.graph, pos,
                               edge_color=edge_colors,
                               alpha=0.3,
                               arrows=True,
                               arrowsize=10)

        # Add labels only for key nodes (to avoid clutter)
        labels = {}
        for node in self.graph.nodes():
            if self.graph.degree(node) > 3 or '#/search' in node:
                labels[node] = self.graph.nodes[node].get('label', '')[:20]

        nx.draw_networkx_labels(self.graph, pos, labels, font_size=8)

        # Legend
        legend_elements = [
            plt.scatter([], [], c='#3498db', s=100, label='Metadata pages'),
            plt.scatter([], [], c='#2ecc71', s=100, label='Search pages'),
            plt.scatter([], [], c='#e74c3c', s=100, label='External links'),
            plt.scatter([], [], c='#f39c12', s=100, label='Downloads'),
            plt.scatter([], [], c='#9b59b6', s=100, label='Other internal'),
        ]
        plt.legend(handles=legend_elements, loc='upper left')

        plt.title('Web Structure Graph - Uruguay IDE GeoNetwork', fontsize=16)
        plt.axis('off')
        plt.tight_layout()

        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"Graph visualization saved to {output_file}")

        if show:
            plt.show()

    def print_statistics(self):
        """Print graph statistics"""
        print("\n" + "="*50)
        print("WEB GRAPH STATISTICS")
        print("="*50)

        print(f"\nTotal nodes: {self.graph.number_of_nodes()}")
        print(f"Total edges: {self.graph.number_of_edges()}")

        # Count by type
        type_counts = defaultdict(int)
        for node in self.graph.nodes(data=True):
            if '#/metadata/' in node[0]:
                type_counts['metadata'] += 1
            elif '#/search' in node[0]:
                type_counts['search'] += 1
            elif node[1].get('type') == 'external':
                type_counts['external'] += 1
            elif node[1].get('type') == 'download':
                type_counts['download'] += 1
            else:
                type_counts['other'] += 1

        print("\nNodes by type:")
        for t, count in sorted(type_counts.items()):
            print(f"  {t}: {count}")

        # Most connected nodes
        print("\nMost connected nodes (by out-degree):")
        out_degrees = sorted(self.graph.out_degree(), key=lambda x: x[1], reverse=True)[:5]
        for node, degree in out_degrees:
            print(f"  {self.get_page_label(node)}: {degree} outgoing links")

        print("\nMost referenced nodes (by in-degree):")
        in_degrees = sorted(self.graph.in_degree(), key=lambda x: x[1], reverse=True)[:5]
        for node, degree in in_degrees:
            print(f"  {self.get_page_label(node)}: {degree} incoming links")


def load_metadata_links(filepath='metadata_links.txt'):
    """Load previously crawled metadata links"""
    links = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if line and line.startswith('#/metadata/'):
                full_url = BASE_URL + line
                links.append(full_url)
    return links


async def main():
    # Load the already crawled metadata links
    print("Loading metadata links...")
    seed_links = load_metadata_links(os.path.join(SCRIPT_DIR, '..', 'metadata_links.txt'))
    print(f"Found {len(seed_links)} metadata links")

    # Create crawler with max depth 1 (crawl seeds + their direct links)
    crawler = WebGraphCrawler(max_depth=1)

    # Crawl a sample of pages (start with first 5 to test)
    sample_size = 5  # Increase this to crawl more pages
    print(f"\nCrawling {sample_size} sample pages...")

    await crawler.crawl_from_seeds(seed_links[:sample_size])

    # Print statistics
    crawler.print_statistics()

    # Save graph data
    crawler.save_graph('web_graph.json')

    # Visualize the graph

if __name__ == "__main__":
    asyncio.run(main())
