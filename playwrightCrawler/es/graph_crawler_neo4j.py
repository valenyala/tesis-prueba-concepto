import argparse
import asyncio
import os
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
from neo4j import GraphDatabase
from collections import defaultdict

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'log')
RUN_LOG = os.path.join(LOG_DIR, 'run.log')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

BASE_URL = "https://www.idee.es/csw-inspire-idee/srv/spa/catalog.search"

# Neo4j connection settings
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "tesis_password"

SKIP_URL_PATTERNS = [
    'catalog.signin', 'signout', '/sign-in', '/login',
    'catalog.edit', 'admin.console'
]


class WebGraphCrawlerNeo4j:
    def __init__(self, max_depth=2, external_max_hops=2, run_id="default"):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        self.driver.verify_connectivity()
        self.visited = set()
        self.max_depth = max_depth
        self.external_max_hops = external_max_hops
        self.run_id = run_id
        self._ensure_constraints()

    def _ensure_constraints(self):
        """Create uniqueness constraint on Page (url + run_id) for fast lookups"""
        with self.driver.session() as session:
            # Drop the old single-property constraint if it exists
            try:
                session.run("DROP CONSTRAINT page_url_unique IF EXISTS")
            except Exception:
                pass
            session.run(
                "CREATE CONSTRAINT page_url_run_unique IF NOT EXISTS "
                "FOR (p:Page) REQUIRE (p.url, p.run_id) IS UNIQUE"
            )

    def close(self):
        self.driver.close()

    def is_external(self, url):
        """Check if URL is external to the base site"""
        return urlparse(url).netloc != urlparse(BASE_URL).netloc

    def should_skip_url(self, url):
        """Check if this URL should be skipped (sign-in, edit, etc.)"""
        return any(pattern in url for pattern in SKIP_URL_PATTERNS)

    def normalize_url(self, url, current_url=BASE_URL):
        """Normalize URL to absolute form"""
        if url.startswith('#'):
            return BASE_URL + url
        elif url.startswith('/'):
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
        path = parsed.path.rstrip('/')
        if path:
            segments = path.split('/')
            label = '/'.join(segments[-3:]) if len(segments) > 3 else path
            if self.is_external(url):
                label = parsed.netloc + '/' + label.lstrip('/')
            return label
        return parsed.netloc

    _OGC_SERVICES = {'wms', 'wfs', 'wcs', 'csw', 'wmts', 'wps'}

    def _is_ogc_service(self, url):
        """Detect OGC service URLs by query params, path segments, or subdomain"""
        url_lower = url.lower()
        parsed = urlparse(url_lower)
        # Standard OGC query parameter: SERVICE=WMS|WFS|WCS|CSW|WMTS|WPS
        if any(f'service={s}' in parsed.query for s in self._OGC_SERVICES):
            return True
        # Path segment match: /wms, /wfs, /ows, /geoserver, /mapserv, etc.
        path_segments = set(parsed.path.split('/'))
        if path_segments & (self._OGC_SERVICES | {'ows', 'geoserver', 'mapserv'}):
            return True
        # Subdomain hint: wms.example.org, ows.example.org, etc.
        if any(parsed.netloc.startswith(s + '.') for s in self._OGC_SERVICES | {'ows', 'geoserver'}):
            return True
        return False

    def categorize_link(self, url):
        """Categorize the type of link"""
        if '#/metadata/' in url:
            return 'metadata'
        elif '#/search' in url:
            return 'search'
        elif self._is_ogc_service(url):
            return 'service'
        elif 'download' in url.lower() or url.endswith(('.zip', '.pdf', '.csv', '.json', '.xml')):
            return 'download'
        elif urlparse(url).netloc != urlparse(BASE_URL).netloc:
            return 'external'
        else:
            return 'internal'

    def _upsert_page(self, tx, url, **properties):
        """MERGE a Page node with the given properties, scoped by run_id"""
        tx.run(
            "MERGE (p:Page {url: $url, run_id: $run_id}) "
            "SET p += $props",
            url=url, run_id=self.run_id, props=properties
        )

    def _upsert_link(self, tx, source_url, target_url, link_type, text):
        """MERGE a LINKS_TO relationship between two pages within the same run"""
        tx.run(
            "MERGE (src:Page {url: $src, run_id: $run_id}) "
            "MERGE (tgt:Page {url: $tgt, run_id: $run_id}) "
            "MERGE (src)-[r:LINKS_TO {run_id: $run_id}]->(tgt) "
            "SET r.type = $type, r.text = $text",
            src=source_url, tgt=target_url, run_id=self.run_id,
            type=link_type, text=text
        )

    async def extract_links(self, page, current_url):
        """Extract all links from the current page"""
        links = []

        anchors = await page.query_selector_all('a[href]')
        for anchor in anchors:
            href = await anchor.get_attribute('href')
            if href and not href.startswith('javascript:') and href != '#':
                full_url = self.normalize_url(href, current_url)
                if not full_url.startswith('https://'):
                    continue
                if self.should_skip_url(href) or self.should_skip_url(full_url):
                    continue
                text = await anchor.inner_text()
                links.append({
                    'url': full_url,
                    'text': text.strip()[:50] if text else '',
                    'type': self.categorize_link(full_url)
                })

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
        """Crawl a single page and insert its data into Neo4j"""
        if url in self.visited:
            return
        if depth > self.max_depth:
            return
        if self.is_external(url) and external_hops > self.external_max_hops:
            return
        if self.should_skip_url(url):
            return

        print(f"[Depth {depth}] Crawling: {self.get_page_label(url)} (external_hops={external_hops})")

        try:
            await page.goto(url, wait_until='networkidle', timeout=30000)
            await asyncio.sleep(2)

            actual_url = page.url
            if actual_url in self.visited:
                return
            self.visited.add(actual_url)

            if self.should_skip_url(actual_url):
                print(f"  Skipping: redirected to unwanted page ({actual_url})")
                return

            title = await page.title()
            links = await self.extract_links(page, url)

            # Insert the crawled page into Neo4j
            with self.driver.session() as session:
                session.execute_write(
                    self._upsert_page,
                    url,
                    label=self.get_page_label(url),
                    title=title,
                    depth=depth,
                    links_count=len(links),
                    external_hops=external_hops,
                )

                # Insert each link as a node + relationship
                for link in links:
                    link_url = link['url']
                    link_type = link['type']

                    session.execute_write(
                        self._upsert_page,
                        link_url,
                        label=self.get_page_label(link_url),
                        type=link_type,
                    )
                    session.execute_write(
                        self._upsert_link,
                        url, link_url, link_type, link['text'],
                    )

            # Queue links for deeper crawling
            links_to_follow = []
            for link in links:
                link_url = link['url']
                link_type = link['type']
                if link_type in ['metadata', 'search', 'internal', 'external'] and link_url not in self.visited:
                    links_to_follow.append((link_url, link_type))

            for link_url, link_type in links_to_follow[:10]:
                new_external_hops = external_hops + 1 if self.is_external(link_url) else external_hops
                await self.crawl_page(page, link_url, depth + 1, new_external_hops)

        except Exception as e:
            print(f"  Error crawling {url}: {e}")
            # Record the error in Neo4j as well
            with self.driver.session() as session:
                session.execute_write(
                    self._upsert_page,
                    url,
                    error=str(e),
                )

    async def crawl_from_seeds(self, seed_urls):
        """Start crawling from a list of seed URLs"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            for i, url in enumerate(seed_urls):
                print(f"\n=== Processing seed {i+1}/{len(seed_urls)} ===")
                await self.crawl_page(page, url, depth=0)

            await browser.close()

    def log_run(self, node_count):
        """Append run summary to the shared run log"""
        os.makedirs(LOG_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(RUN_LOG, 'a') as f:
            f.write(f"{timestamp}  crawler=page  run_id={self.run_id}  nodes={node_count}\n")

    def print_statistics(self):
        """Print graph statistics for the current run_id"""
        with self.driver.session() as session:
            node_count = session.run(
                "MATCH (p:Page {run_id: $run_id}) RETURN count(p) AS c",
                run_id=self.run_id
            ).single()["c"]
            edge_count = session.run(
                "MATCH ()-[r:LINKS_TO {run_id: $run_id}]->() RETURN count(r) AS c",
                run_id=self.run_id
            ).single()["c"]

            print("\n" + "=" * 50)
            print(f"WEB GRAPH STATISTICS (Neo4j) — run: {self.run_id}")
            print("=" * 50)
            print(f"\nTotal nodes: {node_count}")
            print(f"Total edges: {edge_count}")

            # Count by type
            print("\nNodes by type:")
            type_rows = session.run(
                "MATCH (p:Page {run_id: $run_id}) "
                "RETURN coalesce(p.type, "
                "  CASE "
                "    WHEN p.url CONTAINS '#/metadata/' THEN 'metadata' "
                "    WHEN p.url CONTAINS '#/search' THEN 'search' "
                "    ELSE 'other' "
                "  END"
                ") AS type, count(*) AS c "
                "ORDER BY c DESC",
                run_id=self.run_id
            )
            for row in type_rows:
                print(f"  {row['type']}: {row['c']}")

            # Most connected nodes (out-degree)
            print("\nMost connected nodes (by out-degree):")
            out_rows = session.run(
                "MATCH (p:Page {run_id: $run_id})-[r:LINKS_TO {run_id: $run_id}]->() "
                "RETURN p.label AS label, count(r) AS degree "
                "ORDER BY degree DESC LIMIT 5",
                run_id=self.run_id
            )
            for row in out_rows:
                print(f"  {row['label']}: {row['degree']} outgoing links")

            # Most referenced nodes (in-degree)
            print("\nMost referenced nodes (by in-degree):")
            in_rows = session.run(
                "MATCH ()-[r:LINKS_TO {run_id: $run_id}]->(p:Page {run_id: $run_id}) "
                "RETURN p.label AS label, count(r) AS degree "
                "ORDER BY degree DESC LIMIT 5",
                run_id=self.run_id
            )
            for row in in_rows:
                print(f"  {row['label']}: {row['degree']} incoming links")

            self.log_run(node_count)


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
    parser = argparse.ArgumentParser(description="Crawl catalog and store graph in Neo4j")
    parser.add_argument(
        "--run-id",
        default="default",
        help="Identifier for this crawl run (e.g. 'spain-2026-02-18'). "
             "Allows multiple independent graphs in the same database.",
    )
    parser.add_argument("--max-depth", type=int, default=1, help="Maximum crawl depth")
    args = parser.parse_args()

    print("Loading metadata links...")
    seed_links = load_metadata_links(os.path.join(SCRIPT_DIR, '..', 'es_metadata_links.txt'))
    print(f"Found {len(seed_links)} metadata links")

    crawler = WebGraphCrawlerNeo4j(max_depth=args.max_depth, run_id=args.run_id)

    print(f"\nCrawling all {len(seed_links)} pages... (run_id={args.run_id})")

    try:
        await crawler.crawl_from_seeds(seed_links)
        crawler.print_statistics()
    finally:
        crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
