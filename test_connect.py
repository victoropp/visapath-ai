import os, socket
from neo4j import GraphDatabase

# Read environment
URI  = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PWD  = os.getenv("NEO4J_PWD")

def my_resolver(address):
    """
    Neo4j calls this with an Address-like object (with attributes `address` and `port`).
    We extract host/port, DNS-resolve the host, and return a list of (ip, port) tuples.
    """
    # `address` here is neo4j._sync.io._bolt_address.Address
    host = address.host if hasattr(address, "host") else address.address
    port = address.port
    # Perform a normal DNS lookup on the hostname
    ip = socket.gethostbyname(host)
    return [(ip, port)]

# Build driver with our custom resolver
driver = GraphDatabase.driver(
    URI,
    auth=(USER, PWD),
    resolver=my_resolver
)

try:
    driver.verify_connectivity()
    print(" Connected to Aura with custom resolver!")
finally:
    driver.close()
