"""
etl_neo4j_load.py – v1.13-inc
• Incremental loader: keeps existing data, updates/creates new orgs.
• Auto-adds `date_added` (first sight) and `last_updated` (every load).
• Reads EVERY CSV column as str; numeric-leading names preserved.
• Stores all extra columns on Organisation via `SET o += row.props`.
"""

from pathlib import Path
import os, itertools, pandas as pd, time, re
from datetime import datetime, timezone
from neo4j import GraphDatabase, exceptions
from dotenv import load_dotenv

# ── 0. Paths & env ────────────────────────────────────────────────────
BASE = Path(r"C:\Users\victo\Documents\Data_Science_Projects\visapath-ai")
CSV  = BASE / "data" / "sponsor_register_clean.csv"
load_dotenv(BASE / ".env")

URI  = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD  = os.getenv("NEO4J_PASSWORD")
if not (URI and PWD):
    raise RuntimeError("Set NEO4J_URI & NEO4J_PASSWORD")

# Current load timestamp (UTC ISO-8601)
LOAD_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ── 1. Read CSV (all cells as strings) ───────────────────────────────
df = pd.read_csv(CSV, skiprows=1, encoding="utf-8-sig",
                 dtype=str, na_filter=False)
df.columns = df.columns.str.strip()

# ── 2. Cypher helpers ────────────────────────────────────────────────
CONSTRAINTS = [
    "CREATE CONSTRAINT org_name IF NOT EXISTS "
    "FOR (o:Organisation) REQUIRE o.name IS UNIQUE",
    "CREATE CONSTRAINT loc_key IF NOT EXISTS "
    "FOR (l:Location) REQUIRE (l.town, l.county) IS UNIQUE",
    "CREATE CONSTRAINT route_key IF NOT EXISTS "
    "FOR (r:Route) REQUIRE r.name IS UNIQUE"
]

UNWIND_CYPHER = """
UNWIND $rows AS row
MERGE (o:Organisation {name: row.name})
  ON CREATE SET o.date_added  = row.load_ts,
                o.type_rating = row.rating,
                o.town        = row.town,
                o.county      = row.county
  ON MATCH  SET o.last_updated = row.load_ts,
                o.type_rating  = row.rating
SET  o += row.props                 // store all extra cols incl. load_ts
MERGE (l:Location {town: row.town, county: row.county})
MERGE (r:Route {name: row.route})
MERGE (o)-[:LOCATED_IN]->(l)
MERGE (o)-[:OFFERS_ROUTE]->(r)
"""

DELETE_TMPL = (
    "{match} WITH n LIMIT $limit "
    "DETACH DELETE n RETURN count(n) AS deleted"
)

def delete_in_batches(sess, match, limit=10_000):
    while True:
        deleted = sess.run(
            DELETE_TMPL.format(match=match), limit=limit
        ).single()["deleted"]
        if deleted == 0:
            break

def wipe_previous(sess):
    delete_in_batches(sess, "MATCH (n) WHERE size(labels(n)) = 0")
    delete_in_batches(sess, "MATCH (n:Organisation)")
    delete_in_batches(sess, "MATCH (n:Location)")
    delete_in_batches(sess, "MATCH (n:Route)")

def create_constraints(sess):
    for cy in CONSTRAINTS:
        sess.run(cy)

def safe(v) -> str:
    return str(v).strip() if v is not None else ""

def camel_to_snake(col: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", col)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

# ── 3. Row generator (all columns) ────────────────────────────────────
def row_iter():
    for _, r in df.iterrows():
        name = safe(r["organisation_name"])
        if not name:
            continue
        town   = safe(r["town_city"]) or "<UnknownTown>"
        county = safe(r["county"])    or "<UnknownCounty>"
        rating = safe(r["type_rating"])
        route  = safe(r["route"])     or "<UnknownRoute>"

        props = {camel_to_snake(k): safe(v) for k, v in r.items()}
        props.update(dict(town=town, county=county,
                          type_rating=rating,
                          load_ts=LOAD_TS))

        yield dict(name=name, town=town, county=county,
                   rating=rating, route=route,
                   load_ts=LOAD_TS, props=props)

# ── 4. Run ────────────────────────────────────────────────────────────
cfg = dict(max_connection_lifetime=3600,
           max_connection_pool_size=50,
           connection_timeout=30)

print("Connecting to Aura …")
with GraphDatabase.driver(URI, auth=(USER, PWD), **cfg) as drv:
    drv.verify_connectivity()

    with drv.session(database="neo4j") as s:
        # print("Wiping previous data …")            # ← COMMENTED for incremental mode
        # wipe_previous(s)                           # ← COMMENTED for incremental mode
        create_constraints(s)

    print(f"Ingesting rows (timestamp {LOAD_TS}) …")
    BATCH, loaded = 1_000, 0
    with drv.session(database="neo4j") as s:
        iterator = row_iter()
        for chunk in iter(lambda: list(itertools.islice(iterator, BATCH)), []):
            try:
                s.execute_write(lambda tx: tx.run(UNWIND_CYPHER, rows=chunk))
            except exceptions.TransientError:
                s.execute_write(lambda tx: tx.run(UNWIND_CYPHER, rows=chunk))
            loaded += len(chunk)
            if loaded % 10_000 == 0:
                print(f"{loaded:,} rows processed")

print(f"Finished. {loaded:,} rows ingested.")
