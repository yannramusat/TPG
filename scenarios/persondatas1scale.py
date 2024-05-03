import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class PersonDataScenarioS1Scale(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        relations = []
        # csv#1
        for i in range(scale):
            rel_person_cmd = f"MERGE (n:Person{i} {{name: row[1], address: row[2]}})"
            param_string = "persondata/person"+str(size)+"-"+str(lstring)+".csv"
            rel_person = InputRelation(os.path.join(prefix, param_string), rel_person_cmd)
            relations.append(rel_person)
        # csv#2
        for i in range(scale):
            rel_address_cmd = f"MERGE (n:Address{i} {{occ: row[1], city: row[2]}})"
            param_string = "persondata/address"+str(size)+"-"+str(lstring)+".csv"
            rel_address = InputRelation(os.path.join(prefix, param_string), rel_address_cmd)
            relations.append(rel_address)
        # csv#3
        for i in range(scale):
            rel_place_cmd = f"MERGE (n:Place{i} {{occ: row[1], zip: row[2]}})"
            param_string = "persondata/place"+str(size)+"-"+str(lstring)+".csv"
            rel_place = InputRelation(os.path.join(prefix, param_string), rel_place_cmd)
            relations.append(rel_place)
        # source schema
        self.schema = InputSchema(relations)

class PersonDataScenarioS1PlainScale(PersonDataScenarioS1Scale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (p:Person{i})
            MATCH (a:Address{i}) WHERE a.occ = p.name
            MATCH (pl:Place{i}) WHERE pl.occ = p.name
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(p) + "),{i}" 
            }})
            SET x:Person2{i},
                x.address = p.address
            MERGE (y:_dummy {{ 
                _id: "(" + a.city + "),{i}" 
            }})
            SET y:City{i},
                y.city = a.city
            MERGE (z:_dummy {{
                _id: "(" + pl.zip + "),{i}"
            }})
            SET z:Zip{i},
                z.zip = pl.zip
            MERGE (x)-[ha:HAS_ADDRESS{i} {{
                _id: "(HAS_ADDRESS{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]->(y)
            MERGE (x)-[hp:HAS_PLACE{i} {{
                _id: "(HAS_PLACE{i}:" + elementId(x) + "," + elementId(z) + ",{i})"
            }}]->(z)
            """)
            rules.append(rule1)
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

class PersonDataScenarioS1CDoverPlainScale(PersonDataScenarioS1PlainScale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (p:Person{i})
            MATCH (a:Address{i}) WHERE a.occ = p.name
            MATCH (pl:Place{i}) WHERE pl.occ = p.name
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(p) + ",{i})" 
            }})
            ON CREATE
                SET x:Person2{i},
                    x.address = p.address
            ON MATCH
                SET x:Person2{i},
                    x.address =
                    CASE
                        WHEN x.address <> p.address
                            THEN "Conflict detected!"
                        ELSE
                            p.address
                    END
            MERGE (y:_dummy {{ 
                _id: "(" + a.city + ",{i})" 
            }})
            ON CREATE
                SET y:City{i},
                    y.city = a.city
            ON MATCH
                SET y:City{i},
                    y.city =
                    CASE
                        WHEN y.city <> a.city
                            THEN "Conflict detected!"
                        ELSE
                            a.city
                    END
            
            MERGE (z:_dummy {{
                _id: "(" + pl.zip + ",{i})"
            }})
            ON CREATE
                SET z:Zip{i},
                    z.zip = pl.zip
            ON MATCH
                SET z:Zip{i},
                    z.zip =
                    CASE
                        WHEN z.zip <> pl.zip
                            THEN "Conflict detected!"
                        ELSE
                            pl.zip
                    END
            MERGE (x)-[ha:HAS_ADDRESS{i} {{
                _id: "(HAS_ADDRESS{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]->(y)
            MERGE (x)-[hp:HAS_PLACE{i} {{
                _id: "(HAS_PLACE{i}:" + elementId(x) + "," + elementId(z) + ",{i})"
            }}]->(z)
            """)
            rules.append(rule1)
        # transformation rules
        self.rules = rules