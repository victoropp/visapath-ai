#!/usr/bin/env python3
import os
import json
import sys
import logging
import socket
from neo4j import GraphDatabase

# ─── Config from ENV ────────────────────────────────────────────────────────────
URI  = os.getenv('NEO4J_URI')   # e.g., bolt+s://<your-db>.databases.neo4j.io:7687
USER = os.getenv('NEO4J_USER')  # e.g., neo4j
PASSWORD  = os.getenv('NEO4J_PASSWORD')   

if not all([URI, USER, PWD]):
    raise RuntimeError("Please set NEO4J_URI, NEO4J_USER, and NEO4J_PWD in your environment")

# ─── Custom DNS Resolver ────────────────────────────────────────────────────────
def my_resolver(address):
    """
    Neo4j passes an Address-like object with .host and .port attributes.
    We extract host, DNS-resolve it, and return a list of (ip, port) tuples.
    """
    host = getattr(address, "host", None) or getattr(address, "address", None)
    port = address.port
    ip = socket.gethostbyname(host)
    return [(ip, port)]

# ─── MATCH_CRITERIA ─────────────────────────────────────────────────────────────
MATCH_CRITERIA = """
CALL apoc.cypher.runFile(
  'scripts/neo4j/match_criteria.groovy',
  {lucene: lucene, clean: clean}
) YIELD value
RETURN value.org       AS org,
       value.matchScore AS matchScore
"""

# ─── ENRICH_CYPHER ──────────────────────────────────────────────────────────────
ENRICH_CYPHER = """
UNWIND $rows AS job
WITH job,
     apoc.text.clean(job.company)  AS clean,
     job.company + '~'            AS lucene

""" + MATCH_CRITERIA + """

WHERE org IS NOT NULL

MERGE (j:JobAd {id: job.id})
SET j += job,
    j.company_clean    = clean,
    j.sponsor_possible = true,
    j.match_score      = matchScore,
    j.last_matched_ts  = datetime()

MERGE (org:Organisation {name: org.name})
  ON CREATE SET org.name_clean = org.name

MERGE (j)-[:POSTED_BY]->(org)

WITH j, org
MATCH (org)-[:OFFERS_ROUTE]->(rt:Route)
WITH j, collect(DISTINCT rt.name) AS routesList

SET j.route = routesList
"""

def run_file(json_path, batch_size=500):
    # Load JSON ads
    with open(json_path, encoding='utf-8') as f:
        ads = json.load(f)
    logging.info("Matching %s (%d ads)…", os.path.basename(json_path), len(ads))

    # Create driver with custom resolver
    driver = GraphDatabase.driver(
        URI,
        auth=(USER, PWD),
        resolver=my_resolver
    )

    # Fast-fail connectivity check
    driver.verify_connectivity()
    logging.info(" Connected to Neo4j")

    # Execute enrichment in batches
    with driver.session() as sess:
        for i in range(0, len(ads), batch_size):
            batch = ads[i:i+batch_size]
            sess.execute_write(lambda tx: tx.run(
                ENRICH_CYPHER,
                rows=batch
            ))

    driver.close()
    logging.info("Enrichment complete.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    if len(sys.argv) != 2:
        print("Usage: python enrich_jobads.py <path_to_json>")
        sys.exit(1)
    run_file(sys.argv[1])
