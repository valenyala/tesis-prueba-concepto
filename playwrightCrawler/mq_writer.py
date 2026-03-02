#!/usr/bin/env python3
"""
MQ Writer: reads site-graph write operations from a Redis queue
and persists them to the corresponding Neo4j instance.

Usage:
  python mq_writer.py --catalog es   # reads neo4j_writes_es → tesis-neo4j-es (port 7687)
  python mq_writer.py --catalog ch   # reads neo4j_writes_ch → tesis-neo4j-ch (port 7688)

Resilience:
  - Blocks forever on the queue (no idle timeout).
  - On Neo4j write failure: buffers the in-flight message and retries reconnection.
    Escalation: reconnect attempts → podman restart container → podman restart socket.
  - On Redis connection failure: reconnects with retries.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from urllib.parse import urlparse

import redis
from neo4j import GraphDatabase

REDIS_HOST = "localhost"
REDIS_PORT = 6379

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log')
RUN_LOG = os.path.join(LOG_DIR, 'run.log')

CATALOGS = {
    "es": {
        "neo4j_uri": "bolt://localhost:7687",
        "queue": "neo4j_writes_es",
        "neo4j_container": "tesis-neo4j-es",
    },
    "ch": {
        "neo4j_uri": "bolt://localhost:7688",
        "queue": "neo4j_writes_ch",
        "neo4j_container": "tesis-neo4j-ch",
    },
}

NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "tesis_password"

# Reconnect escalation thresholds (attempt numbers, 1-based)
NEO4J_CONTAINER_RESTART_AT = 6   # restart the Neo4j container on this attempt
NEO4J_DOCKER_RESTART_AT    = 12  # restart Docker Desktop on this attempt
NEO4J_RETRY_DELAY          = 10  # seconds between Neo4j reconnect attempts

REDIS_MAX_RETRIES = 20
REDIS_RETRY_DELAY = 5  # seconds between Redis reconnect attempts


# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------

def _make_driver(uri):
    driver = GraphDatabase.driver(uri, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    return driver


def reconnect_neo4j(config, catalog):
    """
    Try to reconnect to Neo4j indefinitely, escalating:
      attempt 6  → docker restart <neo4j-container>
      attempt 12 → systemctl --user restart docker-desktop, then wait
    Returns a live driver.
    """
    uri = config["neo4j_uri"]
    container = config["neo4j_container"]
    attempt = 0

    while True:
        attempt += 1
        print(f"[mq_writer/{catalog}] Neo4j reconnect attempt {attempt}…")

        if attempt == NEO4J_CONTAINER_RESTART_AT:
            print(f"[mq_writer/{catalog}] Restarting Neo4j container '{container}'…")
            subprocess.run(["podman", "restart", container], check=False)
            print(f"[mq_writer/{catalog}] Container restart issued — waiting 30 s for Neo4j to come up…")
            time.sleep(30)

        elif attempt == NEO4J_DOCKER_RESTART_AT:
            print(f"[mq_writer/{catalog}] Restarting Podman socket…")
            subprocess.run(["systemctl", "--user", "restart", "podman.socket"], check=False)
            print(f"[mq_writer/{catalog}] Podman socket restart issued — waiting 60 s…")
            time.sleep(60)
            # Also restart the container after Podman comes back up
            print(f"[mq_writer/{catalog}] Starting Neo4j container '{container}' after Podman restart…")
            subprocess.run(["podman", "start", container], check=False)
            print(f"[mq_writer/{catalog}] Waiting 30 s for Neo4j to initialise…")
            time.sleep(30)

        try:
            driver = _make_driver(uri)
            print(f"[mq_writer/{catalog}] Neo4j reconnected on attempt {attempt}.")
            return driver
        except Exception as e:
            print(f"[mq_writer/{catalog}] Reconnect failed: {e}")
            time.sleep(NEO4J_RETRY_DELAY)


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def reconnect_redis(catalog):
    """Reconnect to Redis with retries. Returns a live Redis client."""
    for attempt in range(1, REDIS_MAX_RETRIES + 1):
        print(f"[mq_writer/{catalog}] Redis reconnect attempt {attempt}/{REDIS_MAX_RETRIES}…")
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
            r.ping()
            print(f"[mq_writer/{catalog}] Redis reconnected.")
            return r
        except Exception as e:
            print(f"[mq_writer/{catalog}] Redis reconnect failed: {e}")
            time.sleep(REDIS_RETRY_DELAY)
    raise RuntimeError(f"[mq_writer/{catalog}] Could not reconnect to Redis after {REDIS_MAX_RETRIES} attempts")


# ---------------------------------------------------------------------------
# Neo4j write transactions (unchanged logic)
# ---------------------------------------------------------------------------

def get_site_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def ensure_constraints(session):
    session.run(
        "CREATE CONSTRAINT site_url_run_unique IF NOT EXISTS "
        "FOR (s:Site) REQUIRE (s.url, s.run_id) IS UNIQUE"
    )


def _write_page_crawled(tx, msg):
    current_site = msg["current_site"]
    run_id = msg["run_id"]
    external_hops = msg["external_hops"]
    links = msg["links"]

    tx.run(
        "MERGE (s:Site {url: $url, run_id: $run_id}) "
        "ON CREATE SET s.label = $label, s.pages_crawled = 0, s.external_hops = $hops "
        "WITH s SET s.pages_crawled = s.pages_crawled + 1, "
        "           s.external_hops = CASE WHEN s.external_hops IS NULL OR s.external_hops > $hops "
        "                             THEN $hops ELSE s.external_hops END",
        url=current_site, run_id=run_id,
        label=urlparse(current_site).netloc,
        hops=external_hops,
    )

    target_sites_seen = set()
    for link in links:
        link_url = link["url"]
        is_service = 1 if link["type"] == "service" else 0
        target_site = get_site_url(link_url)
        if target_site == current_site:
            continue
        if target_site not in target_sites_seen:
            tx.run(
                "MERGE (s:Site {url: $url, run_id: $run_id}) "
                "ON CREATE SET s.label = $label, s.pages_crawled = 0",
                url=target_site, run_id=run_id,
                label=urlparse(target_site).netloc,
            )
            target_sites_seen.add(target_site)
        tx.run(
            "MERGE (src:Site {url: $src, run_id: $run_id}) "
            "MERGE (tgt:Site {url: $tgt, run_id: $run_id}) "
            "MERGE (src)-[r:LINKS_TO {run_id: $run_id}]->(tgt) "
            "ON CREATE SET r.link_count = 1, r.service_link_count = $is_service "
            "ON MATCH SET r.link_count = r.link_count + 1, "
            "            r.service_link_count = r.service_link_count + $is_service",
            src=current_site, tgt=target_site, run_id=run_id,
            is_service=is_service,
        )


def _write_ensure_site(tx, msg):
    site_url = msg["site_url"]
    run_id = msg["run_id"]
    tx.run(
        "MERGE (s:Site {url: $url, run_id: $run_id}) "
        "ON CREATE SET s.label = $label, s.pages_crawled = 0",
        url=site_url, run_id=run_id,
        label=urlparse(site_url).netloc,
    )


def print_statistics(driver, run_id):
    with driver.session() as session:
        node_count = session.run(
            "MATCH (s:Site {run_id: $run_id}) RETURN count(s) AS c",
            run_id=run_id,
        ).single()["c"]
        edge_count = session.run(
            "MATCH ()-[rel:LINKS_TO {run_id: $run_id}]->() RETURN count(rel) AS c",
            run_id=run_id,
        ).single()["c"]
        total_links = session.run(
            "MATCH ()-[rel:LINKS_TO {run_id: $run_id}]->() RETURN sum(rel.link_count) AS c",
            run_id=run_id,
        ).single()["c"]
        pages_crawled = session.run(
            "MATCH (s:Site {run_id: $run_id}) RETURN sum(s.pages_crawled) AS c",
            run_id=run_id,
        ).single()["c"]

        print(f"\n{'='*50}")
        print(f"SITE GRAPH STATISTICS — run: {run_id}")
        print(f"{'='*50}")
        print(f"Total sites (nodes):          {node_count}")
        print(f"Total site-to-site edges:     {edge_count}")
        print(f"Total page-level links:       {total_links}")
        print(f"Total pages crawled:          {pages_crawled}")

        print("\nSites by pages crawled:")
        rows = session.run(
            "MATCH (s:Site {run_id: $run_id}) "
            "RETURN s.label AS label, s.pages_crawled AS pages "
            "ORDER BY pages DESC LIMIT 10",
            run_id=run_id,
        )
        for row in rows:
            print(f"  {row['label']}: {row['pages']} pages")

        print("\nStrongest site-to-site connections:")
        rows = session.run(
            "MATCH (a:Site {run_id: $run_id})"
            "-[rel:LINKS_TO {run_id: $run_id}]->"
            "(b:Site {run_id: $run_id}) "
            "RETURN a.label AS src, b.label AS tgt, rel.link_count AS count "
            "ORDER BY count DESC LIMIT 10",
            run_id=run_id,
        )
        for row in rows:
            print(f"  {row['src']} -> {row['tgt']}: {row['count']} links")

        print("\nMost connected sites (by out-degree):")
        rows = session.run(
            "MATCH (s:Site {run_id: $run_id})"
            "-[rel:LINKS_TO {run_id: $run_id}]->() "
            "RETURN s.label AS label, count(rel) AS degree "
            "ORDER BY degree DESC LIMIT 5",
            run_id=run_id,
        )
        for row in rows:
            print(f"  {row['label']}: links to {row['degree']} other sites")

        print("\nMost referenced sites (by in-degree):")
        rows = session.run(
            "MATCH ()-[rel:LINKS_TO {run_id: $run_id}]->"
            "(s:Site {run_id: $run_id}) "
            "RETURN s.label AS label, count(rel) AS degree "
            "ORDER BY degree DESC LIMIT 5",
            run_id=run_id,
        )
        for row in rows:
            print(f"  {row['label']}: referenced by {row['degree']} other sites")

    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(RUN_LOG, 'a') as f:
        f.write(f"{timestamp}  crawler=site  run_id={run_id}  nodes={node_count}\n")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="MQ Writer: Redis queue → Neo4j")
    parser.add_argument(
        "--catalog", required=True, choices=["es", "ch"],
        help="Which catalog to write for (es or ch)",
    )
    args = parser.parse_args()
    catalog = args.catalog

    config = CATALOGS[catalog]
    queue = config["queue"]

    r = reconnect_redis(catalog)
    driver = _make_driver(config["neo4j_uri"])

    print(f"[mq_writer/{catalog}] queue={queue} neo4j={config['neo4j_uri']}")

    with driver.session() as session:
        ensure_constraints(session)

    print(f"[mq_writer/{catalog}] Constraints ensured. Waiting for messages (no timeout)…")

    processed = 0
    pending_msg = None  # holds a message that failed to write, to be retried

    try:
        while True:
            # If we have a buffered message from a failed write, retry it
            # before pulling a new one from the queue.
            if pending_msg is not None:
                msg = pending_msg
                pending_msg = None
            else:
                # Block indefinitely (timeout=0) until a message arrives.
                result = None
                while result is None:
                    try:
                        result = r.brpop(queue, timeout=0)
                    except redis.exceptions.ConnectionError as e:
                        print(f"[mq_writer/{catalog}] Redis connection lost: {e}")
                        r = reconnect_redis(catalog)

                _, raw = result
                msg = json.loads(raw)

            op = msg.get("op")

            try:
                if op == "page_crawled":
                    with driver.session() as session:
                        session.execute_write(_write_page_crawled, msg)
                    processed += 1
                    if processed % 50 == 0:
                        print(f"[mq_writer/{catalog}] {processed} pages written to Neo4j")

                elif op == "ensure_site":
                    with driver.session() as session:
                        session.execute_write(_write_ensure_site, msg)
                    processed += 1

                elif op == "print_stats":
                    print(
                        f"[mq_writer/{catalog}] Crawl complete. "
                        f"{processed} messages processed. Printing stats…"
                    )
                    print_statistics(driver, msg["run_id"])
                    break

                else:
                    print(f"[mq_writer/{catalog}] Unknown op: {op}")

            except Exception as e:
                print(f"[mq_writer/{catalog}] Neo4j write failed: {e}")
                print(f"[mq_writer/{catalog}] Buffering message and attempting to reconnect…")
                pending_msg = msg
                try:
                    driver.close()
                except Exception:
                    pass
                driver = reconnect_neo4j(config, catalog)
                with driver.session() as session:
                    ensure_constraints(session)

    finally:
        driver.close()
        print(f"[mq_writer/{catalog}] Exited. Total processed: {processed}")


if __name__ == "__main__":
    main()
