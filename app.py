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

    def populate_with_csv(self, path_to_csv_file, mergeCMD, fieldterminator="|"):
        populate_query = f"LOAD CSV FROM '{path_to_csv_file}' as row FIELDTERMINATOR '{fieldterminator}' " + mergeCMD
        records, summary, keys = self.driver.execute_query(
                populate_query,
                database=self.database,
                )
        if(self.verbose):
            self.print_query_stats(records, summary, keys)
            print(f"CSV:    Added {summary.counters.labels_added} labels, created {summary.counters.nodes_created} nodes, " 
                  f"set {summary.counters.properties_set} properties, created {summary.counters.relationships_created} relationships, completed after {summary.result_available_after} ms.")

    def output_all_nodes(self, stats=False):
        count_all_query = """
        MATCH (n)
        RETURN COUNT(n) as count
        """
        records, summary, keys = self.driver.execute_query(
                count_all_query,
                database=self.database,
                )
        if(self.verbose):
            self.print_query_stats(records, summary, keys)
        if(self.verbose or stats):
            print(f"Out:    There is currently {records[0]['count']} node(s) in the database.")

    def query(self, query):
        records, summary, keys = self.driver.execute_query(
                query,
                database=self.database,
                )
        if(self.verbose):
            self.print_query_stats(records, summary, keys)
            print(f"Query:  Added {summary.counters.labels_added} labels, created {summary.counters.nodes_created} nodes, " 
                  f"set {summary.counters.properties_set} properties, created {summary.counters.relationships_created} relationships, completed after {summary.result_available_after} ms.")
        return summary.result_available_after

    def addIndex(self, query, stats=False):
        records, summary, keys = self.driver.execute_query(
                query,
                database=self.database,
                )
        if(self.verbose):
            self.print_query_stats(records, summary, keys)
        if(self.verbose or stats):
            print(f"Idx:    Added {summary.counters.indexes_added} index, completed after {summary.result_available_after} ms.")

    def dropIndex(self, query, stats=False):
        records, summary, keys = self.driver.execute_query(
                query,
                database=self.database,
                )
        if(self.verbose):
            self.print_query_stats(records, summary, keys)
        if(self.verbose or stats):
            print(f"Idx:    Removed {summary.counters.indexes_removed} index, completed after {summary.result_available_after} ms.") 
