# create a sandbox and copy the code from the Python panel

from neo4j import GraphDatabase, basic_auth

# connect to a local neo4j community edition instance
# this instance should be configured with default settings except:
#    #server.directories.import=import
#    dbms.security.auth_enabled=false
#    #dbms.security.allow_csv_import_from_file_urls=true
# hence no credentials are needed and we can LOAD from CSV files 
# located anywhere in the filesystem

# start the server by typing: `sudo /opt/neo4j/bin/neo4j start`
# do this instead of `sudo -u neo4j /opt/neo4j/bin/neo4j start` 
# to avoid permission errors
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
