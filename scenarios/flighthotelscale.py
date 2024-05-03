import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class FlightHotelScenarioScale(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        relations = []
        # csv#1
        for i in range(scale):
            rel_flight_cmd = f"""MERGE (n:Flight{i} {{fid: row[1], src: row[2], dest: row[3]}})"""
            param_string = "flighthotel/flight"+str(size)+"-"+str(lstring)+".csv"
            rel_flight = InputRelation(os.path.join(prefix, param_string), rel_flight_cmd)
            relations.append(rel_flight)
        # csv#2
        for i in range(scale):
            rel_hotel_cmd = f"""MERGE (n:Hotel{i} {{flid: row[1], hid: row[2]}})"""
            param_string = "flighthotel/hotel"+str(size)+"-"+str(lstring)+".csv"
            rel_hotel = InputRelation(os.path.join(prefix, param_string), rel_hotel_cmd)
            relations.append(rel_hotel)
        # source schema
        self.schema = InputSchema(relations)

class FlightHotelScenarioPlainScale(FlightHotelScenarioScale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (f:Flight{i})
            MATCH (h:Hotel{i})
            WHERE f.fid = h.flid
            MERGE (l:_dummy {{ 
                _id: "(" + f.src + ",{i})" 
            }})
            SET l:Location{i},
                l.name = f.src
            MERGE (j:_dummy {{ 
                _id: "(" + f.dest + ",{i})" 
            }})
            SET j:Location{i},
                j.name = f.dest
            MERGE (t:_dummy {{
                _id: "(" + f.src + "," + f.dest + ",{i})"
            }})
            SET t:Travel{i},
                t.from = f.src,
                t.to = f.dest
            MERGE (m:_dummy {{
                _id: "(h(" + h.hid + "),{i})"
            }})
            SET m:Hotel2{i},
                m.name = h.hid
            MERGE (l)-[ft:FLIGHTS_TO{i} {{
                _id: "(FLIGHTS_TO{i}:" + elementId(l) + "," + elementId(t) + ",{i})"
            }}]->(t)
            MERGE (t)-[ft2:FLIGHTS_TO{i} {{
                _id: "(FLIGHTS_TO{i}:" + elementId(t) + "," + elementId(j) + ",{i})"
            }}]->(j)
            MERGE (t)-[hh:HAS_HOTEL{i} {{
                _id: "(HAS_HOTEL{i}:" + elementId(t) + "," + elementId(m) + ",{i})"
            }}]->(m)
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

class FlightHotelScenarioCDoverPlainScale(FlightHotelScenarioPlainScale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (f:Flight{i})
            MATCH (h:Hotel{i})
            WHERE f.fid = h.flid
            MERGE (l:_dummy {{ 
                _id: "(" + f.src + ",{i})" 
            }})
            ON CREATE
                SET l:Location{i},
                    l.name = f.src
            ON MATCH
                SET l:Location{i},
                    l.name =
                    CASE
                        WHEN l.name <> f.src
                            THEN "Conflict detected!"
                        ELSE
                            f.src
                    END
            MERGE (j:_dummy {{ 
                _id: "(" + f.dest + ",{i})" 
            }})
            ON CREATE
                SET j:Location{i},
                    j.name = f.dest
            ON MATCH
                SET j:Location{i},
                    j.name =
                    CASE
                        WHEN j.name <> f.dest
                            THEN "Conflict detected!"
                        ELSE
                            f.dest
                    END
            MERGE (t:_dummy {{
                _id: "(" + f.src + "," + f.dest + ",{i})"
            }})
            ON CREATE
                SET t:Travel{i},
                    t.from = f.src,
                    t.to = f.dest
            ON MATCH
                SET t:Travel{i},
                    t.from =
                    CASE
                        WHEN t.from <> f.src
                            THEN "Conflict detected!"
                        ELSE
                            f.src
                    END,
                    t.to =
                    CASE
                        WHEN t.to <> f.dest
                            THEN "Conflict detected!"
                        ELSE
                            f.dest
                    END
            MERGE (m:_dummy {{
                _id: "(h(" + h.hid + "),{i})"
            }})
            ON CREATE
                SET m:Hotel2{i},
                    m.name = h.hid
            ON MATCH
                SET m:Hotel2{i},
                    m.name =
                    CASE
                        WHEN m.name <> h.hid
                            THEN "Conflict detected!"
                        ELSE
                            h.hid
                    END
            MERGE (l)-[ft:FLIGHTS_TO{i} {{
                _id: "(FLIGHTS_TO{i}:" + elementId(l) + "," + elementId(t) + ",{i})"
            }}]->(t)
            MERGE (t)-[ft2:FLIGHTS_TO{i} {{
                _id: "(FLIGHTS_TO{i}:" + elementId(t) + "," + elementId(j) + ",{i})"
            }}]->(j)
            MERGE (t)-[hh:HAS_HOTEL{i} {{
                _id: "(HAS_HOTEL{i}:" + elementId(t) + "," + elementId(m) + ",{i})"
            }}]->(m)
            """)
            rules.append(rule1)
        # transformation rules
        self.rules = rules