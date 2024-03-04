import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class FlightHotelScenario(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # csv#1
        rel_flight_cmd = "MERGE (n:Flight {fid: row[1], src: row[2], dest: row[3]})"
        param_string = "flighthotel/flight"+str(size)+"-"+str(lstring)+".csv"
        rel_flight = InputRelation(os.path.join(prefix, param_string), rel_flight_cmd)
        # csv#2
        rel_hotel_cmd = "MERGE (n:Hotel {flid: row[1], hid: row[2]})"
        param_string = "flighthotel/hotel"+str(size)+"-"+str(lstring)+".csv"
        rel_hotel = InputRelation(os.path.join(prefix, param_string), rel_hotel_cmd)
        # source schema
        self.schema = InputSchema([rel_flight, rel_hotel])
    
    def addRelIndexes(self, app, stats=False):
        # index on flightsTo
        indexFlightsTo = """
        CREATE INDEX idx_flightsTo IF NOT EXISTS
        FOR ()-[r:FLIGHTS_TO]-()
        ON (r._id)
        """
        app.addIndex(indexFlightsTo, stats)
        # index on hasHotel
        indexHasHotel = """
        CREATE INDEX idx_hasHotel IF NOT EXISTS
        FOR ()-[r:HAS_HOTEL]-()
        ON (r._id)
        """
        app.addIndex(indexHasHotel, stats)
    
    def delRelIndexes(self, app, stats=False):
        # drop index on flightsTo
        dropFlightsTo = """
        DROP INDEX idx_flightsTo IF EXISTS
        """
        app.dropIndex(dropFlightsTo, stats)
        # drop index on hasHotel
        dropHasHotel = """
        DROP INDEX idx_hasHotel IF EXISTS
        """
        app.dropIndex(dropHasHotel, stats)

class FlightHotelScenarioBaseline(FlightHotelScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # Cypher script; we store it as a rule, but it is not!
        baseline_script = TransformationRule("""
        MATCH (f:Flight)
        MATCH (h:Hotel)
        WHERE f.fid = h.flid
        WITH h, collect(f) as Flights
        MERGE (m:Hotel2 {name: h.hid})
        WITH m, Flights
        UNWIND Flights as f
        WITH m, f
        MERGE (l:Location {name: f.src})
        MERGE (j:Location {name: f.dest})
        MERGE (t:Travel {from: f.src, to: f.dest})
        MERGE (l)-[:FLIGHTS_TO]->(t)
        MERGE (t)-[:FLIGHTS_TO]->(j)
        MERGE (t)-[:HAS_HOTEL]->(m)
        """)
        # easier to run the script as if it was a (single) rule
        self.rules = [baseline_script]

    def addNodeIndexes(self, app, stats=False):
        # index on Hotel2/name
        indexHotel2Name = """
        CREATE INDEX idx_Hotel2Name IF NOT EXISTS
        FOR (n:Hotel2)
        ON (n.name)
        """
        app.addIndex(indexHotel2Name, stats)
        
        # index on Location/src
        indexLocationSrc = """
        CREATE INDEX idx_LocationSrc IF NOT EXISTS
        FOR (n:Location)
        ON (n.src)
        """
        app.addIndex(indexLocationSrc, stats)
 
        # index on Location/dest
        indexLocationDest = """
        CREATE INDEX idx_LocationDest IF NOT EXISTS
        FOR (n:Location)
        ON (n.dest)
        """
        app.addIndex(indexLocationDest, stats)

        # composite index on Travel/from/to
        indexTravelFromTo = """
        CREATE INDEX idx_TravelFromTo IF NOT EXISTS
        FOR (n:Travel)
        ON (n.from, n.to)
        """
        app.addIndex(indexTravelFromTo, stats)
   
    def delNodeIndexes(self, app, stats=False):
        # drop index on Hotel2/name
        dropHotel2Name = """
        DROP INDEX idx_Hotel2Name IF EXISTS
        """
        app.dropIndex(dropHotel2Name, stats)

        # drop index on Location/src
        dropLocationSrc = """
        DROP INDEX idx_LocationSrc IF EXISTS
        """
        app.dropIndex(dropLocationSrc, stats)

        # drop index on Location/dest
        dropLocationDest = """
        DROP INDEX idx_LocationDest IF EXISTS
        """
        app.dropIndex(dropLocationDest, stats)

        # drop composite index on Travel/from/to
        dropTravelFromTo = """
        DROP INDEX idx_TravelFromTo IF EXISTS
        """
        app.dropIndex(dropTravelFromTo, stats)
        
class FlightHotelScenarioPlain(FlightHotelScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (f:Flight)
        MATCH (h:Hotel)
        WHERE f.fid = h.flid
        MERGE (l:_dummy { 
            _id: "(" + f.src + ")" 
        })
        SET l:Location,
            l.name = f.src
        MERGE (j:_dummy { 
            _id: "(" + f.dest + ")" 
        })
        SET j:Location,
            j.name = f.dest
        MERGE (t:_dummy {
            _id: "(" + f.src + "," + f.dest + ")"
        })
        SET t:Travel,
            t.from = f.src,
            t.to = f.dest
        MERGE (m:_dummy {
            _id: "(h(" + h.hid + "))"
        })
        SET m:Hotel2,
            m.name = h.hid
        MERGE (l)-[ft:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(l) + "," + elementId(t) + ")"
        }]->(t)
        MERGE (t)-[ft2:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(t) + "," + elementId(j) + ")"
        }]->(j)
        MERGE (t)-[hh:HAS_HOTEL {
            _id: "(HAS_HOTEL:" + elementId(t) + "," + elementId(m) + ")"
        }]->(m)
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

class FlightHotelScenarioConflicting(FlightHotelScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (f:Flight)
        MATCH (h:Hotel)
        WHERE f.fid = h.flid
        MERGE (l:_dummy { 
            _id: "(" + f.src + ")" 
        })
        SET l:Location,
            l.name = f.src
        MERGE (j:_dummy { 
            _id: "(" + f.dest + ")" 
        })
        SET j:Location,
            j.name = f.dest
        MERGE (t:_dummy {
            _id: "(" + f.src + "," + f.dest + ")"
        })
        SET t:Travel,
            t.from = f.src,
            t.to = f.dest
        MERGE (m:_dummy {
            _id: "(" + h.hid + ")"
        })
        SET m:Hotel2,
            m.name = "h(" + h.hid + ")"
        MERGE (l)-[ft:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(l) + "," + elementId(t) + ")"
        }]->(t)
        MERGE (t)-[ft2:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(t) + "," + elementId(j) + ")"
        }]->(j)
        MERGE (t)-[hh:HAS_HOTEL {
            _id: "(HAS_HOTEL:" + elementId(t) + "," + elementId(m) + ")"
        }]->(m)
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

class FlightHotelScenarioSeparateIndexes(FlightHotelScenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (f:Flight)
        MATCH (h:Hotel)
        WHERE f.fid = h.flid
        MERGE (l:Location { 
            _id: "(Location:" + f.src + ")" 
        })
        SET l.name = f.src
        MERGE (j:Location { 
            _id: "(Location:" + f.dest + ")" 
        })
        SET j.name = f.dest
        MERGE (t:Travel {
            _id: "(Travel:" + f.src + "," + f.dest + ")"
        })
        SET t.from = f.src,
            t.to = f.dest
        MERGE (m:Hotel2 {
            _id: "(Hotel2:" + h.hid + ")"
        })
        SET m.name = h.hid
        MERGE (l)-[ft:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(l) + "," + elementId(t) + ")"
        }]->(t)
        MERGE (t)-[ft2:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(t) + "," + elementId(j) + ")"
        }]->(j)
        MERGE (t)-[hh:HAS_HOTEL {
            _id: "(HAS_HOTEL:" + elementId(t) + "," + elementId(m) + ")"
        }]->(m)
        """)
        # transformation rules
        self.rules = [rule1]

    def addNodeIndexes(self, app, stats=False):
        # index on location
        indexLocation = """
        CREATE INDEX idx_location IF NOT EXISTS
        FOR (n:Location)
        ON (n._id)
        """
        app.addIndex(indexLocation, stats)
        # index on travel
        indexTravel = """
        CREATE INDEX idx_travel IF NOT EXISTS
        FOR (n:Travel)
        ON (n._id)
        """
        app.addIndex(indexTravel, stats)
        # index on hotel2
        indexHotel2 = """
        CREATE INDEX idx_hotel2 IF NOT EXISTS
        FOR (n:Hotel2)
        ON (n._id)
        """
        app.addIndex(indexHotel2, stats)
    
    def delNodeIndexes(self, app, stats=False):
        # drop index on location
        dropLocation = """
        DROP INDEX idx_location IF EXISTS
        """
        app.dropIndex(dropLocation, stats)
        # drop index on travel
        dropTravel = """
        DROP INDEX idx_travel IF EXISTS
        """
        app.dropIndex(dropTravel, stats)
        # drop index on hotel2
        dropHotel2 = """
        DROP INDEX idx_hotel2 IF EXISTS
        """
        app.dropIndex(dropHotel2, stats)

class FlightHotelScenarioCDoverSI(FlightHotelScenarioSeparateIndexes):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (f:Flight)
        MATCH (h:Hotel)
        WHERE f.fid = h.flid
        MERGE (l:Location { 
            _id: "(Location:" + f.src + ")" 
        })
        ON CREATE
            SET l.name = f.src
        ON MATCH
            SET l.name =
                CASE
                    WHEN l.name <> f.src
                        THEN "Conflict detected!"
                    ELSE
                        f.src
                END
        MERGE (j:Location { 
            _id: "(Location:" + f.dest + ")" 
        })
        ON CREATE
            SET j.name = f.dest
        ON MATCH
            SET j.name =
                CASE
                    WHEN j.name <> f.dest
                        THEN "Conflict detected!"
                    ELSE
                        f.dest
                END
        MERGE (t:Travel {
            _id: "(Travel:" + f.src + "," + f.dest + ")"
        })
        ON CREATE
            SET t.from = f.src,
                t.to = f.dest
        ON MATCH
            SET t.from =
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
        MERGE (m:Hotel2 {
            _id: "(Hotel2:" + h.hid + ")"
        })
        ON CREATE
            SET m.name = h.hid
        ON MATCH
            SET m.name =
                CASE
                    WHEN m.name <> h.hid
                        THEN "Conflict detected!"
                    ELSE
                        h.hid
                END
        MERGE (l)-[ft:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(l) + "," + elementId(t) + ")"
        }]->(t)
        MERGE (t)-[ft2:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(t) + "," + elementId(j) + ")"
        }]->(j)
        MERGE (t)-[hh:HAS_HOTEL {
            _id: "(HAS_HOTEL:" + elementId(t) + "," + elementId(m) + ")"
        }]->(m)
        """)
        # transformation rules
        self.rules = [rule1]

class FlightHotelScenarioCDoverPlain(FlightHotelScenarioPlain):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (f:Flight)
        MATCH (h:Hotel)
        WHERE f.fid = h.flid
        MERGE (l:_dummy { 
            _id: "(" + f.src + ")" 
        })
        ON CREATE
            SET l:Location,
                l.name = f.src
        ON MATCH
            SET l:Location,
                l.name =
                CASE
                    WHEN l.name <> f.src
                        THEN "Conflict detected!"
                    ELSE
                        f.src
                END
        MERGE (j:_dummy { 
            _id: "(" + f.dest + ")" 
        })
        ON CREATE
            SET j:Location,
                j.name = f.dest
        ON MATCH
            SET j:Location,
                j.name =
                CASE
                    WHEN j.name <> f.dest
                        THEN "Conflict detected!"
                    ELSE
                        f.dest
                END
        MERGE (t:_dummy {
            _id: "(" + f.src + "," + f.dest + ")"
        })
        ON CREATE
            SET t:Travel,
                t.from = f.src,
                t.to = f.dest
        ON MATCH
            SET t:Travel,
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
        MERGE (m:_dummy {
            _id: "(h(" + h.hid + "))"
        })
        ON CREATE
            SET m:Hotel2,
                m.name = h.hid
        ON MATCH
            SET m:Hotel2,
                m.name =
                CASE
                    WHEN m.name <> h.hid
                        THEN "Conflict detected!"
                    ELSE
                        h.hid
                END
        MERGE (l)-[ft:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(l) + "," + elementId(t) + ")"
        }]->(t)
        MERGE (t)-[ft2:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(t) + "," + elementId(j) + ")"
        }]->(j)
        MERGE (t)-[hh:HAS_HOTEL {
            _id: "(HAS_HOTEL:" + elementId(t) + "," + elementId(m) + ")"
        }]->(m)
        """)
        # transformation rules
        self.rules = [rule1]

class FlightHotelScenarioCDoverConflicting(FlightHotelScenarioConflicting):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (f:Flight)
        MATCH (h:Hotel)
        WHERE f.fid = h.flid
        MERGE (l:_dummy { 
            _id: "(" + f.src + ")" 
        })
        ON CREATE
            SET l:Location,
                l.name = f.src
        ON MATCH
            SET l:Location,
                l.name =
                CASE
                    WHEN l.name <> f.src
                        THEN "Conflict detected!"
                    ELSE
                        f.src
                END
        MERGE (j:_dummy { 
            _id: "(" + f.dest + ")" 
        })
        ON CREATE
            SET j:Location,
                j.name = f.dest
        ON MATCH
            SET j:Location,
                j.name =
                CASE
                    WHEN j.name <> f.dest
                        THEN "Conflict detected!"
                    ELSE
                        f.dest
                END
        MERGE (t:_dummy {
            _id: "(" + f.src + "," + f.dest + ")"
        })
        ON CREATE
            SET t:Travel,
                t.from = f.src,
                t.to = f.dest
        ON MATCH
            SET t:Travel,
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
        MERGE (m:_dummy {
            _id: "(" + h.hid + ")"
        })
        ON CREATE
            SET m:Hotel2,
                m.name = "h(" + h.hid + ")"
        ON MATCH
            SET m:Hotel2,
                m.name =
                CASE
                    WHEN m.name <> h.hid
                        THEN "Conflict detected!"
                    ELSE
                        "h(" + h.hid + ")"
                END
        MERGE (l)-[ft:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(l) + "," + elementId(t) + ")"
        }]->(t)
        MERGE (t)-[ft2:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(t) + "," + elementId(j) + ")"
        }]->(j)
        MERGE (t)-[hh:HAS_HOTEL {
            _id: "(HAS_HOTEL:" + elementId(t) + "," + elementId(m) + ")"
        }]->(m)
        """)
        # transformation rules
        self.rules = [rule1]

class FlightHotelScenarioRandom(FlightHotelScenarioPlain):
    def __init__(self, prefix, size = 100, lstring = 5, prob_conflict = 50):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule(f"""
        MATCH (f:Flight)
        MATCH (h:Hotel)
        WHERE f.fid = h.flid
        MERGE (l:_dummy { 
            _id: "(" + f.src + ")" 
        })
        ON CREATE
            SET l:Location,
                l.name = f.src + "1"
        ON MATCH
            SET l:Location,
                l.name =
                CASE
                    WHEN l.name <> f.src + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        f.src + "1"
                END
        MERGE (j:_dummy { 
            _id: "(" + f.dest + ")" 
        })
        ON CREATE
            SET j:Location,
                j.name = f.dest + "1"
        ON MATCH
            SET j:Location,
                j.name =
                CASE
                    WHEN j.name <> f.dest + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        f.dest + "1"
                END
        MERGE (t:_dummy {
            _id: "(" + f.src + "," + f.dest + ")"
        })
        ON CREATE
            SET t:Travel,
                t.from = f.src + "1",
                t.to = f.dest + "1"
        ON MATCH
            SET t:Travel,
                t.from =
                CASE
                    WHEN t.from <> f.src + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        f.src + "1"
                END,
                t.to =
                CASE
                    WHEN t.to <> f.dest + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        f.dest + "1"
                END
        MERGE (m:_dummy {
            _id: "(h(" + h.hid + "))"
        })
        ON CREATE
            SET m:Hotel2,
                m.name = h.hid + "1"
        ON MATCH
            SET m:Hotel2,
                m.name =
                CASE
                    WHEN m.name <> h.hid + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        h.hid + "1"
                END
        MERGE (l)-[ft:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(l) + "," + elementId(t) + ")"
        }]->(t)
        MERGE (t)-[ft2:FLIGHTS_TO {
            _id: "(FLIGHTS_TO:" + elementId(t) + "," + elementId(j) + ")"
        }]->(j)
        MERGE (t)-[hh:HAS_HOTEL {
            _id: "(HAS_HOTEL:" + elementId(t) + "," + elementId(m) + ")"
        }]->(m)
        """)
        # transformation rules
        self.rules = [rule1]
