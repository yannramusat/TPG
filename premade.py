import os
from app import App
from structures import InputRelation, InputSchema, TransformationRule, Scenario

class PersonAddressScenario(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        ## building scenario
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
        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (a:Address)
        MERGE (x:Person2 { _id: "(Person2:" + a.zip + "," + a.city + ")", address: a.zip })
        MERGE (y:Address2 { _id: "(Address2:" + elementId(a) + ")", zip: a.zip, city: a.city})
        MERGE (x)-[v:livesAt {
            _id: "(livesAt:" + elementId(x) + "," + elementId(y) + ")"
        }]->(y)
        """)
        # rule#2 using our framework
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
        # transformation rules
        self.rules = [rule1, rule2] 
