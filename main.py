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

    def populate_with_csv(self, path_to_csv_file, mergeCMD, fieldterminator="|"):
        populate_query = f"LOAD CSV FROM '{path_to_csv_file}' as row FIELDTERMINATOR '{fieldterminator}' " + mergeCMD
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
        print(f"Out:    There is currently {records[0]['count']} node(s) in the database.")

    def query(self, query):
        records, summary, keys = self.driver.execute_query(
                query,
                )
        if(self.verbose):
            self.print_query_stats(records, summary, keys)
        print(f"Query:  Added {summary.counters.labels_added} labels, created {summary.counters.nodes_created} nodes, " 
              f"set {summary.counters.properties_set} properties, created {summary.counters.relationships_created} relationships, completed after {summary.result_available_after} ms.")
        return summary.result_available_after

class InputRelation(object):
    def __init__(self, path_to_csv_file, mergeCMD):
        self.file = path_to_csv_file
        self.mergeCMD = mergeCMD

    def populate(self):
        app.populate_with_csv(self.file, self.mergeCMD)

class InputSchema(object):
    def __init__(self, input_relations):
        self.relations = input_relations

    def instanciate(self):
        for rel in self.relations:
            rel.populate()

class TransformationRule(object):
    def __init__(self, query_str):
        self.query_str = query_str

    def apply(self):
        app.query(self.query_str) 

class Scenario(object):
    def __init__(self, schema, rules):
        self.schema = schema
        self.rules = rules

    def prepare(self):
        app.flush_database()
        self.schema.instanciate()
        app.output_all_nodes()

    def transform(self):
        for rule in self.rules:
            rule.apply()
        app.output_all_nodes()

if __name__ == "__main__":
    # app setup
    scheme = "bolt"
    hostname = "localhost"
    port = "7687"
    uri = f"{scheme}://{hostname}:{port}"
    app = App(uri, "neo4j", verbose=False)

    # common prefix for CSVs; the data has been generated by ibench
    prefix = "file:///home/yann/research/ibench/build/ibench/"

    # csv#1
    rel_address_cmd = "MERGE (n:Address {zip: row[1], city: row[2]})"
    rel_address = InputRelation(prefix+"try/address.csv", rel_address_cmd)
    # csv#2
    rel_person_cmd = "MERGE (n:Person {name: row[1], address: row[2]})"
    rel_person = InputRelation(prefix+"try/person.csv", rel_person_cmd)
    # source schema
    source_schema = InputSchema([rel_address, rel_person])
    # rule#1
    rule1 = TransformationRule("""
    MATCH (a:Address)
    MERGE (x:Person2 { _id: "(Person2:" + a.zip + "," + a.city + ")", address: a.zip })
    MERGE (y:Address2 { _id: "(Address2:" + elementId(a) + ")", zip: a.zip, city: a.city})
    MERGE (x)-[v:livesAt {
        _id: "(livesAt:" + elementId(x) + "," + elementId(y) + ")"
    }]->(y)
    """)
    # rule#2
    rule2 = TransformationRule("""
    MATCH (p:Person)
    MATCH (a:Address)
    WHERE p.address = a.zip
    MERGE (x:Person2 { _id: "(Person2:" + elementId(p) + ")", name: p.name, address: p.address })
    MERGE (y:Address2 { _id: "(Address2:" + elementId(a) + ")" , zip: a.zip, city: a.city})
    MERGE (x)-[v:livesAt {
        _id: "(livesAt:" + elementId(x) + "," + elementId(y) + ")"
    }]->(y)
    """)
    # run scenario
    scenario_personaddress = Scenario(source_schema, [rule1, rule2])
    scenario_personaddress.prepare()
    scenario_personaddress.transform()

    # close connection
    app.close()
