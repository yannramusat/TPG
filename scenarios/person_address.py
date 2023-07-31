import os
from app import App
from structures import InputRelation, InputSchema, TransformationRule, Scenario

class PersonAddressScenario(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # csv#1
        rel_address_cmd = "MERGE (n:Address {zip: row[1], city: row[2]})"
        param_string = "personaddress/address"+str(size)+"-"+str(lstring)+".csv"
        rel_address = InputRelation(os.path.join(prefix, param_string), rel_address_cmd)
        # csv#2
        rel_person_cmd = "MERGE (n:Person {name: row[1], address: row[2]})"
        param_string = "personaddress/person"+str(size)+"-"+str(lstring)+".csv"
        rel_person = InputRelation(os.path.join(prefix, param_string), rel_person_cmd)
        # source schema
        self.schema = InputSchema([rel_address, rel_person])

    def addIndexes(self, app, stats=False):
        pass
    
    def destroyIndexes(self, app, stats=False):
        pass

class PersonAddressScenarioNaive(PersonAddressScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (a:Address)
        MERGE (x {
            _id: "(Person2:" + a.zip + "," + a.city + ")"
        })
        SET x:Person2, 
            x.address = a.zip
        MERGE (y {
            _id: "(Address2:" + elementId(a) + ")"
        })
        SET y:Address2, 
            y.zip = a.zip, 
            y.city = a.city
        MERGE (x)-[v:livesAt {
            _id: "(livesAt:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address)
        WHERE p.address = a.zip
        MERGE (x { 
            _id: "(Person2:" + elementId(p) + ")"
        })
        SET x:Person2,
            x.name = p.name,
            x.address = p.address
        MERGE (y { 
            _id: "(Address2:" + elementId(a) + ")"
        })
        SET y:Address2,
            y.zip = a.zip,
            y.city = a.city
        MERGE (x)-[v:livesAt {
            _id: "(livesAt:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # transformation rules
        self.rules = [rule1, rule2]

class PersonAddressScenarioWithIndexes(PersonAddressScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (a:Address)
        MERGE (x:Person2 {
            _id: "(Person2:" + a.zip + "," + a.city + ")" 
        })
        SET x.address = a.zip
        MERGE (y:Address2 {
            _id: "(Address2:" + elementId(a) + ")" 
        })
        SET y.zip = a.zip,
            y.city = a.city
        MERGE (x)-[v:livesAt {
            _id: "(livesAt:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address)
        WHERE p.address = a.zip
        MERGE (x:Person2 { 
            _id: "(Person2:" + elementId(p) + ")" 
        })
        SET x.name = p.name,
            x.address = p.address
        MERGE (y:Address2 { 
            _id: "(Address2:" + elementId(a) + ")" 
        })
        SET y.zip = a.zip,
            y.city = a.city
        MERGE (x)-[v:livesAt {
            _id: "(livesAt:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # transformation rules
        self.rules = [rule1, rule2]

    def addIndexes(self, app, stats=False):
        # index on address2
        indexAddress2 = """
        CREATE INDEX idx_address2 IF NOT EXISTS
        FOR (n:Address2)
        ON (n._id)
        """
        app.addIndex(indexAddress2, stats)
        # index on person2 
        indexPerson2 = """
        CREATE INDEX idx_person2 IF NOT EXISTS
        FOR (n:Person2)
        ON (n._id)
        """
        app.addIndex(indexPerson2, stats)

    def destroyIndexes(self, app, stats=False):
        # drop index on address2
        dropAddress2 = """
        DROP INDEX idx_address2 IF EXISTS
        """
        app.dropIndex(dropAddress2, stats)
        # drop index on person2
        dropPerson2 = """
        DROP INDEX idx_person2 IF EXISTS
        """
        app.dropIndex(dropPerson2, stats)

class PersonAddressScenarioWithConflictDetection(PersonAddressScenarioWithIndexes):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (a:Address)
        MERGE (x:Person2 {
            _id: "(Person2:" + a.zip + "," + a.city + ")"
        })
        ON CREATE
            SET x.address = a.zip
        ON MATCH
            SET x.address =
                CASE
                    WHEN x.address <> a.zip
                        THEN "Conflict detected!"
                    ELSE a.zip
                END
        MERGE (y:Address2 {
            _id: "(Address2:" + elementId(a) + ")"
        })
        ON CREATE
            SET y.zip = a.zip,
                y.city = a.city
        ON MATCH
            SET y.zip =
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
        MERGE (x)-[v:livesAt {
            _id: "(livesAt:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address)
        WHERE p.address = a.zip
        MERGE (x:Person2 { 
            _id: "(Person2:" + elementId(p) + ")"
        })
        ON CREATE
            SET x.name = p.name,
                x.address = p.address
        ON MATCH
            SET x.name =
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
        MERGE (y:Address2 { 
            _id: "(Address2:" + elementId(a) + ")"
        })
        ON CREATE
            SET y.zip = a.zip,
                y.city = a.city
        ON MATCH
            SET y.zip =
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
        MERGE (x)-[v:livesAt {
            _id: "(livesAt:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # transformation rules
        self.rules = [rule1, rule2]
