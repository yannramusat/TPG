import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

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
    
    def addRelIndexes(self, app, stats=False):
        # index on livesAt
        indexLivesAt = """
        CREATE INDEX idx_livesAt IF NOT EXISTS
        FOR ()-[r:LIVES_AT]-()
        ON (r._id)
        """
        app.addIndex(indexLivesAt, stats)
    
    def delRelIndexes(self, app, stats=False):
        # drop index on livesAt
        dropLivesAt = """
        DROP INDEX idx_livesAt IF EXISTS
        """
        app.dropIndex(dropLivesAt, stats)

class PersonAddressScenarioBaseline(PersonAddressScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # Cypher script; we store it as a rule, but it is not!
        baseline_script = TransformationRule("""
        MATCH (a:Address)
        CREATE (x:Person2 {address: a.zip})
        MERGE (y:Address2 {zip: a.zip, city: a.city})
        CREATE (x)-[:LIVES_AT]->(y)
        WITH y, a.zip as AddressZip
        MATCH (p:Person)
        WHERE p.address = AddressZip
        MERGE (x:Person2 {name: p.name})
        SET x.address = p.address
        CREATE (x)-[:LIVES_AT]->(y)
        """)
        # easier to run the script as if it was a (single) rule
        self.rules = [baseline_script]

    def addNodeIndexes(self, app, stats=False):
        # composite index on Address2/zip/city
        indexAddress2ZipCity = """
        CREATE INDEX idx_Address2ZipCity IF NOT EXISTS
        FOR (n:Address2)
        ON (n.zip, n.city)
        """
        app.addIndex(indexAddress2ZipCity, stats)
 
        # index on Person2/name
        indexPerson2Name = """
        CREATE INDEX idx_Person2Name IF NOT EXISTS
        FOR (n:Person2)
        ON (n.name)
        """
        app.addIndex(indexPerson2Name, stats)

    def delNodeIndexes(self, app, stats=False):
        # drop composite index on Address2/zip/city
        dropAddress2ZipCity = """
        DROP INDEX idx_Address2ZipCity IF EXISTS
        """
        app.dropIndex(dropAddress2ZipCity, stats)
 
        # drop index on Person2/name
        dropPerson2Name = """
        DROP INDEX idx_Person2Name IF EXISTS
        """
        app.dropIndex(dropPerson2Name, stats)
 
class PersonAddressScenarioPlain(PersonAddressScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (a:Address)
        MERGE (x:_dummy {
            _id: "(" + a.zip + "," + a.city + ")"
        })
        SET x:Person2, 
            x.address = a.zip
        MERGE (y:_dummy {
            _id: "(" + elementId(a) + ")"
        })
        SET y:Address2, 
            y.zip = a.zip, 
            y.city = a.city
        MERGE (x)-[v:LIVES_AT {
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address)
        WHERE p.address = a.zip
        MERGE (x:_dummy { 
            _id: "(" + elementId(p) + ")"
        })
        SET x:Person2,
            x.name = p.name,
            x.address = p.address
        MERGE (y:_dummy { 
            _id: "(" + elementId(a) + ")"
        })
        SET y:Address2,
            y.zip = a.zip,
            y.city = a.city
        MERGE (x)-[v:LIVES_AT {
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # transformation rules
        self.rules = [rule1, rule2]

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

class PersonAddressScenarioSeparateIndexes(PersonAddressScenario):
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
        MERGE (x)-[v:LIVES_AT {
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
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
        MERGE (x)-[v:LIVES_AT {
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # transformation rules
        self.rules = [rule1, rule2]

    def addNodeIndexes(self, app, stats=False):
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
    
    def delNodeIndexes(self, app, stats=False):
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
    
class PersonAddressScenarioCDoverSI(PersonAddressScenarioSeparateIndexes):
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
        MERGE (x)-[v:LIVES_AT {
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
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
        MERGE (x)-[v:LIVES_AT {
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # transformation rules
        self.rules = [rule1, rule2]

class PersonAddressScenarioCDoverPlain(PersonAddressScenarioPlain):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (a:Address)
        MERGE (x:_dummy {
            _id: "(" + a.zip + "," + a.city + ")"
        })
        ON CREATE
            SET x:Person2,
                x.address = a.zip
        ON MATCH
            SET x:Person2,
                x.address =
                CASE
                    WHEN x.address <> a.zip
                        THEN "Conflict detected!"
                    ELSE a.zip
                END
        MERGE (y:_dummy {
            _id: "(" + elementId(a) + ")"
        })
        ON CREATE
            SET y:Address2,
                y.zip = a.zip,
                y.city = a.city
        ON MATCH
            SET y:Address2,
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
        MERGE (x)-[v:LIVES_AT {
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule("""
        MATCH (p:Person)
        MATCH (a:Address)
        WHERE p.address = a.zip
        MERGE (x:_dummy { 
            _id: "(" + elementId(p) + ")"
        })
        ON CREATE
            SET x:Person2,
                x.name = p.name,
                x.address = p.address
        ON MATCH
            SET x:Person2,
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
        MERGE (y:_dummy { 
            _id: "(" + elementId(a) + ")"
        })
        ON CREATE
            SET y:Address2,
                y.zip = a.zip,
                y.city = a.city
        ON MATCH
            SET y:Address2,
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
        MERGE (x)-[v:LIVES_AT {
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # transformation rules
        self.rules = [rule1, rule2]

class PersonAddressScenarioRandomConflicts(PersonAddressScenarioCDoverPlain):
    def __init__(self, prefix, size = 100, lstring = 5, prob_conflict = 50):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule(f"""
        MATCH (a:Address)
        MERGE (x:_dummy {{
            _id: "(" + a.zip + "," + a.city + ")"
        }})
        ON CREATE
            SET x:Person2,
                x.address = a.zip + "1"
        ON MATCH
            SET x:Person2,
                x.address =
                CASE
                    WHEN x.address <> a.zip + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE a.zip + "1"
                END
        MERGE (y:_dummy {{
            _id: "(" + elementId(a) + ")"
        }})
        ON CREATE
            SET y:Address2,
                y.zip = a.zip + "1",
                y.city = a.city + "1"
        ON MATCH
            SET y:Address2,
                y.zip =
                CASE
                    WHEN y.zip <> a.zip + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE a.zip + "1"
                END,
                y.city =
                CASE
                    WHEN y.city <> a.city + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE a.city + "1"
                END
        MERGE (x)-[v:LIVES_AT {{
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
        }}]->(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule(f"""
        MATCH (p:Person)
        MATCH (a:Address)
        WHERE p.address = a.zip
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(p) + ")"
        }})
        ON CREATE
            SET x:Person2,
                x.name = p.name + "1",
                x.address = p.address + "1"
        ON MATCH
            SET x:Person2,
                x.name =
                CASE
                    WHEN x.name <> p.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE p.name + "1"
                END,
                x.address =
                CASE
                    WHEN x.address <> p.address + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE p.address + "1"
                END
        MERGE (y:_dummy {{ 
            _id: "(" + elementId(a) + ")"
        }})
        ON CREATE
            SET y:Address2,
                y.zip = a.zip + "1",
                y.city = a.city + "1"
        ON MATCH
            SET y:Address2,
                y.zip =
                CASE
                    WHEN y.zip <> a.zip + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE a.zip + "1"
                END,
                y.city =
                CASE
                    WHEN y.city <> a.city + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE a.city + "1"
                END 
        MERGE (x)-[v:LIVES_AT {{
            _id: "(LIVES_AT:" + elementId(x) + "," + elementId(y) + ")"
        }}]->(y)
        """)
        # transformation rules
        self.rules = [rule1, rule2]

    def run(self, app, launches=5, stats=False, nodeIndex=True, relIndex=False, shuffle=True, minmax=True):
        ttime = 0.0
        min_rtime = float("inf")
        max_rtime = 0
        for i in range(launches):
            self.prepare(app, stats=stats)
            # shuffle rules in place; if requested
            if shuffle:
                import random
                random.shuffle(self.rules)
                print(f"The rules have been shuffled.")
            # resume to the classic run procedure
            if(nodeIndex):
                self.addNodeIndexes(app, stats=stats)
            if(relIndex):
                self.addRelIndexes(app, stats=stats)
            # statistics about runtime
            rtime = self.transform(app, stats=stats)
            ttime += rtime
            if(rtime < min_rtime):
                min_rtime = rtime
            if(rtime > max_rtime):
                max_rtime = rtime
            # resume to classic run procedure
            if(nodeIndex):
                self.delNodeIndexes(app, stats=stats)
            if(relIndex):
                self.delRelIndexes(app, stats=stats)
        avg_time = ttime / launches
        if(stats):
            print(f"The transformation: {self}  averaged {avg_time} ms over {launches} run(s).")
        if(minmax):
            return (min_rtime, avg_time, max_rtime)
        else:
            return avg_time
