#!/bin/env python3

from neo4j import GraphDatabase

class App(object):
    def __init__(self, uri, database, verbose=False):
        self.driver = GraphDatabase.driver(
                uri, 
                auth=None)
        self.database = database
        self.verbose = verbose

    def close(self):
        self.driver.close

    def print_query_stats(self, records, summary, keys):
        print("The query `{query}` returned {records_count} records in {time} ms.".format(
            query=summary.query, 
            records_count=len(records),
            time=summary.result_available_after,
            ))

    def flush_database(self):
        flush_query = """
        MATCH (n) DETACH DELETE(n)
        """
        records, summary, keys = self.driver.execute_query(
                flush_query,
                database=self.database)
        if(self.verbose):
            self.print_query_stats(records, summary, keys)
        print(f"Del:    Deleted {summary.counters.nodes_deleted} nodes, deleted {summary.counters.relationships_deleted} relationships, completed after {summary.result_available_after} ms.")

    def populate_with_csv(self, path_to_csv_file, fieldterminator='|', label="test"):
        populate_query = """
        LOAD CSV FROM 'file:///home/yann/research/ibench/build/ibench/try/address.csv' as row FIELDTERMINATOR '|'
        MERGE (n:Address {zip: row[1], city: row[2]})
        """
        records, summary, keys = self.driver.execute_query(
                populate_query,
                )
        if(self.verbose):
            self.print_query_stats(records, summary, keys)
        print(f"CSV:    Added {summary.counters.labels_added} labels, created {summary.counters.nodes_created} nodes, " 
              f"set {summary.counters.properties_set} properties, created {summary.counters.relationships_created} relationships, completed after {summary.result_available_after} ms.")

    def output_all_nodes(self):
        count_all_query = """
        MATCH (n)
        RETURN COUNT(n) as count
        """
        records, summary, keys = self.driver.execute_query(
                count_all_query,
                )
        if(self.verbose):
            self.print_query_stats(records, summary, keys)
        print(f"All:    There is currently {records[0]['count']} node(s) in the database.")

if __name__ == "__main__":
    scheme = "bolt"
    hostname = "localhost"
    port = "7687"
    uri = f"{scheme}://{hostname}:{port}"
    app = App(uri, "neo4j", verbose=False)

    prefix = "file:///home/yann/ibench/build/ibench/"
    app.populate_with_csv(prefix+"try/measure_le_0_nl0_ce0_address.csv")
    app.output_all_nodes()
    app.flush_database()
    app.output_all_nodes()
    app.close()
