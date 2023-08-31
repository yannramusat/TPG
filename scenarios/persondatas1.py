import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class PersonDataScenario(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # csv#1
        rel_person_cmd = "MERGE (n:Person {name: row[1], address: row[2]})"
        param_string = "persondata/person"+str(size)+"-"+str(lstring)+".csv"
        rel_person = InputRelation(os.path.join(prefix, param_string), rel_person_cmd)
        # csv#2
        rel_address_cmd = "MERGE (n:Address {occ: row[1], city: row[2]})"
        param_string = "persondata/address"+str(size)+"-"+str(lstring)+".csv"
        rel_address = InputRelation(os.path.join(prefix, param_string), rel_address_cmd)
        # csv#3
        rel_place_cmd = "MERGE (n:Place {occ: row[1], zip: row[2]})"
        param_string = "persondata/place"+str(size)+"-"+str(lstring)+".csv"
        rel_place = InputRelation(os.path.join(prefix, param_string), rel_place_cmd)

        # source schema
        self.schema = InputSchema([rel_person, rel_address, rel_place])
    
    def addRelIndexes(self, app, stats=False):
        # index on hasAddress
        indexHasAddress = """
        CREATE INDEX idx_hasAddress IF NOT EXISTS
        FOR ()-[r:HAS_ADDRESS]-()
        ON (r._id)
        """
        app.addIndex(indexHasAddress, stats)
        # index on hasPlace
        indexHasPlace = """
        CREATE INDEX idx_hasPlace IF NOT EXISTS
        FOR ()-[r:HAS_PLACE]-()
        ON (r._id)
        """
        app.addIndex(indexHasPlace, stats)
    
    def delRelIndexes(self, app, stats=False):
        # drop index on hasAddress
        dropHasAddress = """
        DROP INDEX idx_hasAddress IF EXISTS
        """
        app.dropIndex(dropHasAddress, stats)
        # drop index on hasPlace
        dropHasPlace = """
        DROP INDEX idx_hasPlace IF EXISTS
        """
        app.dropIndex(dropHasPlace, stats)

class PersonDataScenarioPlain(PersonDataScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address)
        MATCH (pl:Place)
        MERGE (x:_dummy { 
            _id: "(" + p.name + ")" 
        })
        SET x:Person2,
            x.name = p.address
        MERGE (y:_dummy { 
            _id: "(" + a.city + ")" 
        })
        SET y:City,
            y.city = a.city
        MERGE (z:_dummy {
            _id: "(" + pl.zip + ")"
        })
        SET z:Zip,
            z.zip = pl.zip
        MERGE (x)-[ha:HAS_ADDRESS {
            _id: "(HAS_ADDRESS:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        MERGE (x)-[hp:HAS_PLACE {
            _id: "(HAS_PLACE:" + elementId(x) + "," + elementId(z) + ")"
        }]->(z)
        """)
        # transformation rules
        self.rules = [rule1]

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

class PersonDataScenarioCDoverPlain(PersonDataScenarioPlain):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address)
        MATCH (pl:Place)
        MERGE (x:_dummy { 
            _id: "(" + p.name + ")" 
        })
        ON CREATE
            SET x:Person2,
                x.name = p.address
        ON MATCH
            SET x:Person2,
                x.name =
                CASE
                    WHEN x.name <> p.address
                        THEN "Conflict detected!"
                    ELSE
                        p.address
                END
        MERGE (y:_dummy { 
            _id: "(" + a.city + ")" 
        })
        ON CREATE
            SET y:City,
                y.city = a.city
        ON MATCH
            SET y:City,
                y.city =
                CASE
                    WHEN y.city <> a.city
                        THEN "Conflict detected!"
                    ELSE
                        a.city
                END
        
        MERGE (z:_dummy {
            _id: "(" + pl.zip + ")"
        })
        ON CREATE
            SET z:Zip,
                z.zip = pl.zip
        ON MATCH
            SET z:Zip,
                z.zip =
                CASE
                    WHEN z.zip <> pl.zip
                        THEN "Conflict detected!"
                    ELSE
                        pl.zip
                END
        MERGE (x)-[ha:HAS_ADDRESS {
            _id: "(HAS_ADDRESS:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        MERGE (x)-[hp:HAS_PLACE {
            _id: "(HAS_PLACE:" + elementId(x) + "," + elementId(z) + ")"
        }]->(z)
        """)
        # transformation rules
        self.rules = [rule1]
