from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

class FigureComparisonFlightHotelSplit(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        # results
        self.results_Sep_long = []
        self.results_Plain_long = []
        self.results_Conflicting_long = []
        self.results_Sep_long_split = []
        self.results_Plain_long_split = []
        self.results_Conflicting_long_split = []

    def compute(self):
        # execute the alternative implementation of the scenario FlightHotel with Separate indexes
        from scenarios.flighthotel import FlightHotelScenarioSeparateIndexes
        for i in self.x:
            scenario = FlightHotelScenarioSeparateIndexes(self.prefix, size=i)
            self.results_Sep_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the plain implementation of the scenario FlightHotel
        from scenarios.flighthotel import FlightHotelScenarioPlain
        for i in self.x:
            scenario = FlightHotelScenarioPlain(self.prefix, size=i)
            self.results_Plain_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the conflicting variant of the transformation
        from scenarios.flighthotel import FlightHotelScenarioConflicting
        for i in self.x:
            scenario = FlightHotelScenarioConflicting(self.prefix, size=i)
            self.results_Conflicting_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the alternative implementation of the scenario FlightHotel (Split) with Separate indexes
        from scenarios.flighthotelsplit import FlightHotelScenarioSeparateIndexesSplit
        for i in self.x:
            scenario = FlightHotelScenarioSeparateIndexesSplit(self.prefix, size=i)
            self.results_Sep_long_split.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the plain implementation of the scenario FlightHotel (Split)
        from scenarios.flighthotelsplit import FlightHotelScenarioPlainSplit
        for i in self.x:
            scenario = FlightHotelScenarioPlainSplit(self.prefix, size=i)
            self.results_Plain_long_split.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the conflicting variant of the transformation (Split)
        from scenarios.flighthotelsplit import FlightHotelScenarioConflictingSplit
        for i in self.x:
            scenario = FlightHotelScenarioConflictingSplit(self.prefix, size=i)
            self.results_Conflicting_long_split.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
 
    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np
        # Figure for comparing with Split variant | FlightHotel scenario
        fig2, ax = plt.subplots(layout="constrained")
        ax.plot(self.x, self.results_Sep_long, label="Separate indexes alternative")
        ax.plot(self.x, self.results_Plain_long, label="Plain implementation")
        ax.plot(self.x, self.results_Conflicting_long, label="Variant with conflicts")
        ax.plot(self.x, self.results_Sep_long_split, label="Conflict Detection over Separate indexes")
        ax.plot(self.x, self.results_Plain_long_split, label="Conflict Detection over Plain implementation")
        ax.plot(self.x, self.results_Conflicting_long_split, label="Conflict Detection over variant with conflicts")
        ax.set_title("Comparison with Split variant | FlightHotel scenario")
        ax.set_xlabel("number of rows per input relation")
        ax.set_ylabel("time (ms)")
        ax.legend()

    def print_cmd(self):
        print("## Figure for comparing with Split variant | FlightHotel scenario")
        print("# Comparison with Split variant")
        print(f"{self.results_Sep_long=}")
        print(f"{self.results_Plain_long=}")
        print(f"{self.results_Conflicting_long=}")
        print(f"{self.results_Sep_long_split=}")
        print(f"{self.results_Plain_long_split=}")
        print(f"{self.results_Conflicting_long_split=}")
