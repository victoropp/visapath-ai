# pipeline/load_jobads.py – v0.2
import sys, pathlib, os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PWD = os.getenv("NEO4J_PASSWORD")
drv = GraphDatabase.driver(URI, auth=(USER, PWD))

csv_files = [pathlib.Path(p) for p in sys.argv[1:]]

def load_one(path: pathlib.Path):
    filename = path.name  # just the base filename
    query = f"""
    LOAD CSV WITH HEADERS FROM 'file:///{filename}' AS row
    WITH row WHERE row.id IS NOT NULL
    MERGE (j:JobAd {{id: row.id}})
    SET j += row
    """
    with drv.session() as session:
        session.run(query)
    print(f"✓ imported {filename}")

for f in csv_files:
    load_one(f)

drv.close()
