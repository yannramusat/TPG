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
        MERGE (j:Location { 
            _id: "(Location:" + f.dest + ")" 
        })
        MERGE (t:Travel {
            _id: "(Travel:" + f.src + "," + f.dest + ")"
        })
        MERGE (m:Hotel2 {
            _id: "(Hotel2:" + h.hid + ")"
        })
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
