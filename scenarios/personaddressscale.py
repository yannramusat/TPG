import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class PersonAddressScenarioScale(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        relations = []
        # csv#1
        for i in range(scale):
            rel_address_cmd = f"MERGE (n:Address{i} {{zip: row[1], city: row[2]}})"
            param_string = "personaddress/address"+str(size)+"-"+str(lstring)+".csv"
            rel_address = InputRelation(os.path.join(prefix, param_string), rel_address_cmd)
            relations.append(rel_address)
        # csv#2
        for i in range(scale):
            rel_person_cmd = f"MERGE (n:Person{i} {{name: row[1], address: row[2]}})"
            param_string = "personaddress/person"+str(size)+"-"+str(lstring)+".csv"
            rel_person = InputRelation(os.path.join(prefix, param_string), rel_person_cmd)
            relations.append(rel_person)
        # source schema
        self.schema = InputSchema(relations)
 
class PersonAddressScenarioPlainScale(PersonAddressScenarioScale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (a:Address{i})
            MERGE (x:_dummy {{
                _id: "(" + a.zip + "," + a.city + ",{i})"
            }})
            SET x:Person2s{i},
                x.address = a.zip
            MERGE (y:_dummy {{
                _id: "(" + elementId(a) + ",{i})"
            }})
            SET y:Address2s{i}, 
                y.zip = a.zip, 
                y.city = a.city
            MERGE (x)-[v:LIVES_AT{i} {{
                _id: "(LIVES_AT{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]->(y)
            """)
            rules.append(rule1)
        # rule#2 using our framework
        for i in range(scale):
            rule2 = TransformationRule(f"""
            MATCH (p:Person{i})
            MATCH (a:Address{i})
            WHERE p.address = a.zip
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(p) + ",{i})"
            }})
            SET x:Person2s{i},
                x.name = p.name,
                x.address = p.address
            MERGE (y:_dummy {{ 
                _id: "(" + elementId(a) + ",{i})"
            }})
            SET y:Address2s{i},
                y.zip = a.zip,
                y.city = a.city
            MERGE (x)-[v:LIVES_AT{i} {{
                _id: "(LIVES_AT{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]->(y)
            """)
            rules.append(rule2)
        # transformation rules
        self.rules = rules

    def addNodeIndexes(self, app, stats=False):
        # index on _dummy
        indexDummy = """
        CREATE INDEX idx_dummy IF NOT EXISTS
        FOR (n:_dummy)
        ON (n._id)
        """
        app.addIndex(indexDummy, stats)
    
    def delNodeIndexes(self, app, stats=False):
        # drop index on _dummy
        dropDummy = """
        DROP INDEX idx_dummy IF EXISTS
        """
        app.dropIndex(dropDummy, stats)

class PersonAddressScenarioCDoverPlainScale(PersonAddressScenarioPlainScale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (a:Address{i})
            MERGE (x:_dummy {{
                _id: "(" + a.zip + "," + a.city + ",{i})"
            }})
            ON CREATE
                SET x:Person2s{i},
                    x.address = a.zip
            ON MATCH
                SET x:Person2s{i},
                    x.address =
                    CASE
                        WHEN x.address <> a.zip
                            THEN "Conflict detected!"
                        ELSE a.zip
                    END
            MERGE (y:_dummy {{
                _id: "(" + elementId(a) + ",{i})"
            }})
            ON CREATE
                SET y:Address2s{i},
                    y.zip = a.zip,
                    y.city = a.city
            ON MATCH
                SET y:Address2s{i},
                    y.zip =
                    CASE
                        WHEN y.zip <> a.zip
                            THEN "Conflict detected!"
                        ELSE a.zip
                    END,
                    y.city =
                    CASE
                        WHEN y.city <> a.city
                            THEN "Conflict detected!"
                        ELSE a.city
                    END
            MERGE (x)-[v:LIVES_AT{i} {{
                _id: "(LIVES_AT{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]->(y)
            """)
            rules.append(rule1)
        # rule#2 using our framework
        for i in range(scale):
            rule2 = TransformationRule(f"""
            MATCH (p:Person{i})
            MATCH (a:Address{i})
            WHERE p.address = a.zip
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(p) + ",{i})"
            }})
            ON CREATE
                SET x:Person2s{i},
                    x.name = p.name,
                    x.address = p.address
            ON MATCH
                SET x:Person2s{i},
                    x.name =
                    CASE
                        WHEN x.name <> p.name
                            THEN "Conflict detected!"
                        ELSE p.name
                    END,
                    x.address =
                    CASE
                        WHEN x.address <> p.address
                            THEN "Conflict detected!"
                        ELSE p.address
                    END
            MERGE (y:_dummy {{ 
                _id: "(" + elementId(a) + ",{i})"
            }})
            ON CREATE
                SET y:Address2s{i},
                    y.zip = a.zip,
                    y.city = a.city
            ON MATCH
                SET y:Address2s{i},
                    y.zip =
                    CASE
                        WHEN y.zip <> a.zip
                            THEN "Conflict detected!"
                        ELSE a.zip
                    END,
                    y.city =
                    CASE
                        WHEN y.city <> a.city
                            THEN "Conflict detected!"
                        ELSE a.city
                    END 
            MERGE (x)-[v:LIVES_AT{i} {{
                _id: "(LIVES_AT{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]->(y)
            """)
            rules.append(rule2)
        # transformation rules
        self.rules = rules