import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class PersonDataScenarioS1(Scenario):
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

class PersonDataScenarioS1Baseline(PersonDataScenarioS1):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # Cypher script; we store it as a rule, but it is not!
        baseline_script = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address) WHERE a.occ = p.name
        MATCH (pl:Place) WHERE pl.occ = p.name 
        WITH p, collect(a) as Addresses, collect(pl) as Places
        CREATE (p2:Person2 {address: p.address})
        WITH p2, Addresses, Places
        UNWIND Addresses as a
        WITH p2, a, Places
        MERGE (c:City {city: a.city})
        MERGE (p2)-[:HAS_ADDRESS]->(c)
        WITH p2, Places
        UNWIND Places as p
        WITH p2, p
        MERGE (z:Zip {zip: p.zip})
        MERGE (p2)-[:HAS_PLACE]->(z)
        """)
        # easier to run the script as if it was a (single) rule
        self.rules = [baseline_script]

    def addNodeIndexes(self, app, stats=False):
        # index on City/city
        indexCityCity = """
        CREATE INDEX idx_CityCity IF NOT EXISTS
        FOR (n:City)
        ON (n.city)
        """
        app.addIndex(indexCityCity, stats)
        
        # index on Zip/zip
        indexZipZip = """
        CREATE INDEX idx_ZipZip IF NOT EXISTS
        FOR (n:Zip)
        ON (n.zip)
        """
        app.addIndex(indexZipZip, stats)
   
    def delNodeIndexes(self, app, stats=False):
        # drop index on City/City
        dropCityCity = """
        DROP INDEX idx_CityCity IF EXISTS
        """
        app.dropIndex(dropCityCity, stats)

        # drop index on Zip/Zip
        dropZipZip = """
        DROP INDEX idx_ZipZip IF EXISTS
        """
        app.dropIndex(dropZipZip, stats)

class PersonDataScenarioS1Plain(PersonDataScenarioS1):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address) WHERE a.occ = p.name
        MATCH (pl:Place) WHERE pl.occ = p.name
        MERGE (x:_dummy { 
            _id: "(" + elementId(p) + ")" 
        })
        SET x:Person2,
            x.address = p.address
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

class PersonDataScenarioS1CDoverPlain(PersonDataScenarioS1Plain):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address) WHERE a.occ = p.name
        MATCH (pl:Place) WHERE pl.occ = p.name
        MERGE (x:_dummy { 
            _id: "(" + elementId(p) + ")" 
        })
        ON CREATE
            SET x:Person2,
                x.address = p.address
        ON MATCH
            SET x:Person2,
                x.address =
                CASE
                    WHEN x.address <> p.address
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

class PersonDataScenarioS1Sep(PersonDataScenarioS1):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address) WHERE a.occ = p.name
        MATCH (pl:Place) WHERE pl.occ = p.name
        MERGE (x:Person2 { 
            _id: "(" + elementId(p) + ")" 
        })
        SET x.address = p.address
        MERGE (y:City { 
            _id: "(" + a.city + ")" 
        })
        SET y.city = a.city
        MERGE (z:Zip {
            _id: "(" + pl.zip + ")"
        })
        SET z.zip = pl.zip
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
        # index on Person2
        indexPerson2 = """
        CREATE INDEX idx_Person2 IF NOT EXISTS
        FOR (n:Person2)
        ON (n._id)
        """
        app.addIndex(indexPerson2, stats)

        # index on City
        indexCity = """
        CREATE INDEX idx_City IF NOT EXISTS
        FOR (n:City)
        ON (n._id)
        """
        app.addIndex(indexCity, stats)
    
        # index on Zip
        indexZip = """
        CREATE INDEX idx_Zip IF NOT EXISTS
        FOR (n:Zip)
        ON (n._id)
        """
        app.addIndex(indexZip, stats)
     
    def delNodeIndexes(self, app, stats=False):
        # drop index on Person2
        dropPerson2 = """
        DROP INDEX idx_dummy IF EXISTS
        """
        app.dropIndex(dropPerson2, stats)

        # drop index on City
        dropCity = """
        DROP INDEX idx_City IF EXISTS
        """
        app.dropIndex(dropCity, stats)

        # drop index on Zip
        dropZip = """
        DROP INDEX idx_Zip IF EXISTS
        """
        app.dropIndex(dropZip, stats)

class PersonDataScenarioS1CDoverSep(PersonDataScenarioS1Sep):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address) WHERE a.occ = p.name
        MATCH (pl:Place) WHERE pl.occ = p.name
        MERGE (x:Person2 { 
            _id: "(" + elementId(p) + ")" 
        })
        ON CREATE
            SET x.address = p.address
        ON MATCH
            SET x.address =
                CASE
                    WHEN x.address <> p.address
                        THEN "Conflict detected!"
                    ELSE
                        p.address
                END
        MERGE (y:City { 
            _id: "(" + a.city + ")" 
        })
        ON CREATE
            SET y.city = a.city
        ON MATCH
            SET y.city =
                CASE
                    WHEN y.city <> a.city
                        THEN "Conflict detected!"
                    ELSE
                        a.city
                END
        
        MERGE (z:Zip {
            _id: "(" + pl.zip + ")"
        })
        ON CREATE
            SET z.zip = pl.zip
        ON MATCH
            SET z.zip =
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
