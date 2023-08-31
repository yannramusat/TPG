import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class PersonDataScenarioS2(Scenario):
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
    
    def run(self, app, launches = 5, stats=False, nodeIndex=True, relIndex=True):
        ttime = 0.0
        for i in range(launches):
            self.prepare(app, stats=stats)
            # apply PersonDataS1Plain.transform; do not use any index
            from persondatas1 import PersonDataScenarioS1Plain
            ptime = PersonDataScenarioS1Plain.transform(app, stats=False)
            print(f"Preliminary step in {ptime} ms.")
            # resume to the classic run procedure
            if(nodeIndex):
                self.addNodeIndexes(app, stats=stats)
            if(relIndex):
                self.addRelIndexes(app, stats=stats)
            ttime += self.transform(app, stats=stats)
            if(nodeIndex):
                self.delNodeIndexes(app, stats=stats)
            if(relIndex):
                self.delRelIndexes(app, stats=stats)
        avg_time = ttime / launches
        if(stats):
            print(f"The transformation: {self}  averaged {avg_time} ms over {launches} run(s).")
        return avg_time 

class PersonDataScenarioS2Plain(PersonDataScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
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

class PersonDataScenarioS2CDoverPlain(PersonDataScenarioPlain):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        """)
        # transformation rules
        self.rules = [rule1]
