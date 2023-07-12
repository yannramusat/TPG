#!/bin/env python3

from neo4j import GraphDatabase

class App(object):
    def __init__(self, uri, database=None):
        self.driver = GraphDatabase.driver(
                        uri,
                        auth=None)
        self.database = database

    def close(self):
        self.driver.close

    def flush(self):
        flush_query = '''MATCH (n) DETACH DELETE(n)'''
        with self.driver.session() as session:
            result = self.driver.execute_query(flush_query,database=self.database)
            print(result)

cypher_query = '''
MATCH (n)
RETURN COUNT(n) as count
LIMIT $limit
'''

if __name__ == "__main__":
    scheme = "bolt"
    hostname = "localhost"
    port = "7687"
    uri = f"{scheme}://{hostname}:{port}"
    app = App(uri, "neo4j")
    with app.driver.session(database="neo4j") as session:
        results = session.read_transaction(
                    lambda tx: tx.run(cypher_query,
                                        limit=10).data())
        for record in results:
            print(record['count'])
    app.flush()

    app.close()
