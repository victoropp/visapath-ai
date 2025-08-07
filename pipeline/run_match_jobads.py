"""
Run after every Adzuna fetch.
Usage: python pipeline/run_match_jobads.py data/adzuna_data_*.json
"""
import json, sys
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv; load_dotenv()

URI  = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER", "neo4j")
PWD  = os.getenv("NEO4J_PASSWORD")
cypher = Path("scripts/match_jobads_to_sponsors_v2.cypher").read_text()

driver = GraphDatabase.driver(URI, auth=(USER, PWD))
for jfile in sys.argv[1:]:
    batch = json.loads(Path(jfile).read_text())
    with driver.session() as s:
        summary = s.execute_write(lambda tx: tx.run(cypher, jobBatch=batch).consume())
        print(f"{jfile} → {summary.counters.properties_set} properties set")
driver.close()
