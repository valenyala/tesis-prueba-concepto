import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
import redis


class Tee:
    """Write to multiple streams simultaneously (e.g. stdout + log file)"""
    def __init__(self, *files):
        self.files = files

    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush()

    def flush(self):
        for f in self.files:
            f.flush()


LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'log')
RUN_LOG = os.path.join(LOG_DIR, 'run.log')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

BASE_URL = "https://www.idee.es/csw-inspire-idee/srv/spa/catalog.search"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_QUEUE = "neo4j_writes_es"

REDIS_MAX_RETRIES = 20
REDIS_RETRY_DELAY = 5  # seconds between reconnect attempts

SKIP_URL_PATTERNS = [
    'catalog.signin', 'signout', '/sign-in', '/login',
    'catalog.edit', 'admin.console'
]


def get_site_url(url):
    """Extract the site base (scheme://netloc) from a full URL"""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


class SiteGraphCrawlerNeo4j:
    def __init__(self, max_depth=10, external_max_hops=2, run_id="default", concurrency=10):
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        self.visited = set()  # actual_urls seen after navigation (shared across all seed tasks)
        self.max_depth = max_depth
        self.external_max_hops = external_max_hops
        self.run_id = run_id
        self.concurrency = concurrency

    def is_external(self, url):
        return urlparse(url).netloc != urlparse(BASE_URL).netloc

    def should_skip_url(self, url):
        return any(pattern in url for pattern in SKIP_URL_PATTERNS)

    def normalize_url(self, url, current_url=BASE_URL):
        if url.startswith('#'):
            return BASE_URL + url
        elif url.startswith('/'):
            parsed = urlparse(current_url)
            return f"{parsed.scheme}://{parsed.netloc}{url}"
        elif url.startswith('http'):
            return url
        else:
            return urljoin(current_url, url)

    _OGC_SERVICES = {'wms', 'wfs', 'wcs', 'csw', 'wmts', 'wps'}

    def _is_ogc_service(self, url):
        """Detect OGC service URLs by query params, path segments, or subdomain"""
        url_lower = url.lower()
        parsed = urlparse(url_lower)
        if any(f'service={s}' in parsed.query for s in self._OGC_SERVICES):
            return True
        path_segments = set(parsed.path.split('/'))
        if path_segments & (self._OGC_SERVICES | {'ows', 'geoserver', 'mapserv'}):
            return True
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

    def _lpush(self, msg: str):
        """Push a message to the Redis queue, reconnecting on connection errors."""
        for attempt in range(1, REDIS_MAX_RETRIES + 1):
            try:
                self.redis.lpush(REDIS_QUEUE, msg)
                return
            except redis.exceptions.ConnectionError as e:
                print(f"[crawler/es] Redis connection lost (attempt {attempt}/{REDIS_MAX_RETRIES}): {e}")
                time.sleep(REDIS_RETRY_DELAY)
                try:
                    self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
                    self.redis.ping()
                    print("[crawler/es] Redis reconnected.")
                except Exception:
                    pass
        print(f"[crawler/es] ERROR: could not push to Redis after {REDIS_MAX_RETRIES} attempts — message dropped.")

    def _push_page_crawled(self, current_site, links, external_hops):
        """Push a page_crawled write operation to the Redis queue"""
        msg = json.dumps({
            "op": "page_crawled",
            "current_site": current_site,
            "external_hops": external_hops,
            "run_id": self.run_id,
            "links": links,
        })
        self._lpush(msg)

    def _push_ensure_site(self, site_url):
        """Push an ensure_site write operation to the Redis queue"""
        msg = json.dumps({
            "op": "ensure_site",
            "site_url": site_url,
            "run_id": self.run_id,
        })
        self._lpush(msg)

    def push_print_stats(self):
        """Push a print_stats signal as the final message in the queue"""
        msg = json.dumps({"op": "print_stats", "run_id": self.run_id})
        self._lpush(msg)

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
                links.append({'url': full_url, 'type': self.categorize_link(full_url)})
        return links

    async def crawl_page(self, page, url, depth=0, external_hops=0):
        """Crawl a single page and push site-level write ops to the MQ"""
        if url in self.visited:
            return
        if depth > self.max_depth:
            return
        if self.is_external(url) and external_hops > self.external_max_hops:
            return
        if self.should_skip_url(url):
            return

        current_site = get_site_url(url)

        try:
            await page.goto(url, wait_until='networkidle', timeout=30000)
            await asyncio.sleep(2)

            actual_url = page.url
            if actual_url in self.visited:
                return
            self.visited.add(actual_url)

            if self.should_skip_url(actual_url):
                return

            print(
                f"CRAWL depth={depth} hops={external_hops} "
                f"site={urlparse(current_site).netloc} url={actual_url[:100]}"
            )

            links = await self.extract_links(page, actual_url)
            self._push_page_crawled(current_site, links, external_hops)

            links_to_follow = [l['url'] for l in links[:10] if l['url'] not in self.visited]
            for link_url in links_to_follow:
                new_external_hops = (
                    external_hops + 1 if self.is_external(link_url) else external_hops
                )
                await self.crawl_page(page, link_url, depth + 1, new_external_hops)

        except Exception as e:
            print(f"ERROR depth={depth} url={url[:80]} — {e}")
            self._push_ensure_site(current_site)

    async def crawl_from_seeds(self, seed_urls):
        """Start crawling from all seeds concurrently, up to concurrency pages at a time"""
        sem = asyncio.Semaphore(self.concurrency)

        async def crawl_seed(browser, i, url):
            async with sem:
                print(f"\n=== Seed {i+1}/{len(seed_urls)}: {url[:80]} ===")
                page = await browser.new_page()
                try:
                    await self.crawl_page(page, url, depth=0)
                finally:
                    await page.close()

        print(
            f"=== START run_id={self.run_id} seeds={len(seed_urls)} "
            f"concurrency={self.concurrency} max_depth={self.max_depth} "
            f"external_max_hops={self.external_max_hops} ==="
        )

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            tasks = [crawl_seed(browser, i, url) for i, url in enumerate(seed_urls)]
            await asyncio.gather(*tasks)
            await browser.close()

        print(f"=== DONE run_id={self.run_id} pages_crawled={len(self.visited)} ===")


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
    parser = argparse.ArgumentParser(
        description="Crawl catalog and push site-level graph writes to Redis MQ"
    )
    parser.add_argument(
        "--run-id", default="default",
        help="Identifier for this crawl run (e.g. 'sites-es-2026-02-23')",
    )
    parser.add_argument(
        "--max-depth", type=int, default=10,
        help="Maximum crawl depth in pages (default: 10)",
    )
    parser.add_argument(
        "--external-max-hops", type=int, default=2,
        help="Maximum hops outside the base catalog domain (default: 2)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=10,
        help="Number of seeds to crawl in parallel (default: 10)",
    )
    args = parser.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    run_log_path = os.path.join(LOG_DIR, f"{args.run_id}_run.log")
    log_file = open(run_log_path, 'w')
    sys.stdout = Tee(sys.__stdout__, log_file)
    print(f"Logging to {run_log_path}")

    print("Loading metadata links...")
    seed_links = load_metadata_links(
        os.path.join(SCRIPT_DIR, '..', 'es_metadata_links.txt')
    )
    print(f"Found {len(seed_links)} metadata links")

    crawler = SiteGraphCrawlerNeo4j(
        max_depth=args.max_depth,
        external_max_hops=args.external_max_hops,
        run_id=args.run_id,
        concurrency=args.concurrency,
    )

    print(f"\nCrawling all {len(seed_links)} seeds... (run_id={args.run_id})")
    print(f"Write operations will be queued to Redis → {REDIS_QUEUE}")

    try:
        await crawler.crawl_from_seeds(seed_links)
        crawler.push_print_stats()
        print("All done. print_stats signal pushed to MQ.")
    finally:
        sys.stdout = sys.__stdout__
        log_file.close()


if __name__ == "__main__":
    asyncio.run(main())
