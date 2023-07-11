from neo4j import GraphDatabase

# connect to a local Neo4j Community Edition instance
# this instance should be configured with default settings but:
#   #server.directories.import=import
#   dbms.security.auth_enabled=false
#   #dbms.security.allow_csv_import_from_file_urls=true
# hence no credentials are needed and we can LOAD CSV from files 
# located anywhere in the filesystem

# start the server by typing: `sudo /opt/neo4j/bin/neo4j start`
# do this instead of `sudo -u neo4j /opt/neo4j/bin/neo4j start` 
# in order to avoid permission errors

# Software specs:
#   java-openJDK-17
#   Neo4j Community Edition 5.9.0
#   Neo4j Python Driver 5.9.0
# Hardware specs:
#   Intel(R) Core(TM) i7-6600U CPU @ 2.60GHz
#   32GiB system memory (2133 MHz)

driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=None)

cypher_query = '''
MATCH (n)
RETURN COUNT(n) AS count
LIMIT $limit
'''

with driver.session(database="neo4j") as session:
    results = session.read_transaction(
            lambda tx: tx.run(cypher_query,
                              limit=10).data())
    for record in results:
        print(record['count'])

driver.close()
