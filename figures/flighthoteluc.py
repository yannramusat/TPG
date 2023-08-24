from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

#class FigureComparisonIndexesFlightHotel(Figure):
#    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
#        super().__init__(app, prefix, values, nbLaunches, showStats)
#        # results 
#        self.results_Sep_NI_RI = []
#        self.results_Sep_NI = []
#        self.results_Sep_RI = []
#        self.results_Sep = []
#        self.results_Plain_NI_RI = []
#        self.results_Plain_NI = []
#        self.results_Plain_RI = []
#        self.results_Plain = []
#        self.results_CDoverSI_NI_RI = []
#        self.results_CDoverSI_NI = []
#        self.results_CDoverSI_RI = []
#        self.results_CDoverSI = []
#        self.results_CDoverPlain_NI_RI = []
#        self.results_CDoverPlain_NI = []
#        self.results_CDoverPlain_RI = []
#        self.results_CDoverPlain = []
#
#    def compute(self):
#        # execute the alternative implementation of the scenario FlightHotel with Separate indexes
#        from scenarios.flighthotel import FlightHotelScenarioSeparateIndexes
#        for i in self.x:
#            scenario = FlightHotelScenarioSeparateIndexes(self.prefix, size=i)
#            self.results_Sep_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
#        for i in self.x:
#            scenario = FlightHotelScenarioSeparateIndexes(self.prefix, size=i)
#            self.results_Sep_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
#        for i in self.x:
#            scenario = FlightHotelScenarioSeparateIndexes(self.prefix, size=i)
#            self.results_Sep_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
#        for i in self.x:
#            scenario = FlightHotelScenarioSeparateIndexes(self.prefix, size=i)
#            self.results_Sep.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
#        # execute the plain implementation of the scenario FlightHotel
#        from scenarios.flighthotel import FlightHotelScenarioPlain
#        for i in self.x:
#            scenario = FlightHotelScenarioPlain(self.prefix, size=i)
#            self.results_Plain_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
#        for i in self.x:
#            scenario = FlightHotelScenarioPlain(self.prefix, size=i)
#            self.results_Plain_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
#        for i in self.x:
#            scenario = FlightHotelScenarioPlain(self.prefix, size=i)
#            self.results_Plain_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
#        for i in self.x:
#            scenario = FlightHotelScenarioPlain(self.prefix, size=i)
#            self.results_Plain.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
#        # execute the alternative implementation with Conflict Detection of the scenario FlightHotel based on Separate index
#        from scenarios.flighthotel import FlightHotelScenarioCDoverSI
#        for i in self.x:
#            scenario = FlightHotelScenarioCDoverSI(self.prefix, size=i)
#            self.results_CDoverSI_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
#        for i in self.x:
#            scenario = FlightHotelScenarioCDoverSI(self.prefix, size=i)
#            self.results_CDoverSI_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
#        for i in self.x:
#            scenario = FlightHotelScenarioCDoverSI(self.prefix, size=i)
#            self.results_CDoverSI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True)) 
#        for i in self.x:
#            scenario = FlightHotelScenarioCDoverSI(self.prefix, size=i)
#            self.results_CDoverSI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
#        # execute the alternative implementation with Conflict Detection of the scenario FlightHotel based on Plain implementation
#        from scenarios.flighthotel import FlightHotelScenarioCDoverPlain
#        for i in self.x:
#            scenario = FlightHotelScenarioCDoverPlain(self.prefix, size=i)
#            self.results_CDoverPlain_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
#        for i in self.x:
#            scenario = FlightHotelScenarioCDoverPlain(self.prefix, size=i)
#            self.results_CDoverPlain_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
#        for i in self.x:
#            scenario = FlightHotelScenarioCDoverPlain(self.prefix, size=i)
#            self.results_CDoverPlain_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
#        for i in self.x:
#            scenario = FlightHotelScenarioCDoverPlain(self.prefix, size=i)
#            self.results_CDoverPlain.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
#    
#    def plot(self):
#        # Figure for exhaustive comparison of indexes | FlightHotel scenario
#        fig1, axs = plt.subplots(2, 2, layout="constrained") 
#        fig1.suptitle("FlightHotel scenario", fontsize=16)
#        # Axes Separate indexes
#        axs[0, 0].plot(self.x, self.results_Sep_NI_RI, label="Indexes on Nodes and Relations")
#        axs[0, 0].plot(self.x, self.results_Sep_NI, label="Indexes on Nodes only")
#        axs[0, 0].plot(self.x, self.results_Sep_RI, label="Indexes on Relations only")
#        axs[0, 0].plot(self.x, self.results_Sep, label="Without indexes")
#        axs[0, 0].set_title("Separate indexes alternative implementation")
#        axs[0, 0].set_xlabel("number of rows per input relation")
#        axs[0, 0].set_ylabel("time (ms)")
#        axs[0, 0].set_yscale("log")
#        axs[0, 0].legend()
#        # Axes Plain implementation
#        axs[0, 1].plot(self.x, self.results_Plain_NI_RI, label="Indexes on Nodes and Relations")
#        axs[0, 1].plot(self.x, self.results_Plain_NI, label="Indexes on Nodes only")
#        axs[0, 1].plot(self.x, self.results_Plain_RI, label="Indexes on Relations only") 
#        axs[0, 1].plot(self.x, self.results_Plain, label="Without indexes")
#        axs[0, 1].set_title("Plain implementation")
#        axs[0, 1].set_xlabel("number of rows per input relation")
#        axs[0, 1].set_ylabel("time (ms)")
#        axs[0, 1].set_yscale("log")
#        axs[0, 1].legend()
#        # Axes Conflict Detection over Separate indexes
#        axs[1, 0].plot(self.x, self.results_CDoverSI_NI_RI, label="Indexes on Nodes and Relations")
#        axs[1, 0].plot(self.x, self.results_CDoverSI_NI, label="Indexes on Nodes only")
#        axs[1, 0].plot(self.x, self.results_CDoverSI_RI, label="Indexes on Relations only") 
#        axs[1, 0].plot(self.x, self.results_CDoverSI, label="Without indexes")
#        axs[1, 0].set_title("Conflict Detection over Separate indexes")
#        axs[1, 0].set_xlabel("number of rows per input relation")
#        axs[1, 0].set_ylabel("time (ms)")
#        axs[1, 0].set_yscale("log")
#        axs[1, 0].legend()
#        # Axes Conflict Detection over Plain
#        axs[1, 1].plot(self.x, self.results_CDoverPlain_NI_RI, label="Indexes on Nodes and Relations")
#        axs[1, 1].plot(self.x, self.results_CDoverPlain_NI, label="Indexes on Nodes only")
#        axs[1, 1].plot(self.x, self.results_CDoverPlain_RI, label="Indexes on Relations only") 
#        axs[1, 1].plot(self.x, self.results_CDoverPlain, label="Without indexes")
#        axs[1, 1].set_title("Conflict Detection over Plain implementation")
#        axs[1, 1].set_xlabel("number of rows per input relation")
#        axs[1, 1].set_ylabel("time (ms)")
#        axs[1, 1].set_yscale("log")
#        axs[1, 1].legend()
#
#    def print_cmd(self):
#        print("## Figure for exhaustive comparison of indexes | FlightHotel scenario")
#        print("# Separate indexes alternative")
#        print(f"{self.results_Sep_NI_RI=}")
#        print(f"{self.results_Sep_NI=}")
#        print(f"{self.results_Sep_RI=}")
#        print(f"{self.results_Sep=}")
#        print("# Plain implementation")
#        print(f"{self.results_Plain_NI_RI=}")
#        print(f"{self.results_Plain_NI=}")
#        print(f"{self.results_Plain_RI=}")
#        print(f"{self.results_Plain=}")
#        print("# Conflict Detection over Separate indexes")
#        print(f"{self.results_CDoverSI_NI_RI=}")
#        print(f"{self.results_CDoverSI_NI=}")
#        print(f"{self.results_CDoverSI_RI=}")
#        print(f"{self.results_CDoverSI=}")
#        print("# Conflict Detection over Plain")
#        print(f"{self.results_CDoverPlain_NI_RI=}")
#        print(f"{self.results_CDoverPlain_NI=}")
#        print(f"{self.results_CDoverPlain_RI=}")
#        print(f"{self.results_CDoverPlain=}")

class FigureComparisonAlternativeApproachesFlightHotelUC(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        # results
        self.results_Sep_long = []
        self.results_Plain_long = []
        self.results_Conflicting_long = []
        self.results_Sep_long_UC = []
        self.results_Plain_long_UC = []
        self.results_Conflicting_long_UC = []

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
        # execute the alternative implementation of the scenario FlightHotel (UC) with Separate indexes
        from scenarios.flighthoteluc import FlightHotelScenarioUCSeparateIndexes
        for i in self.x:
            scenario = FlightHotelScenarioUCSeparateIndexes(self.prefix, size=i)
            self.results_Sep_long_UC.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the plain implementation of the scenario FlightHotel (UC)
        from scenarios.flighthoteluc import FlightHotelScenarioUCPlain
        for i in self.x:
            scenario = FlightHotelScenarioUCPlain(self.prefix, size=i)
            self.results_Plain_long_UC.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the conflicting variant of the transformation (UC)
        from scenarios.flighthoteluc import FlightHotelScenarioUCConflicting
        for i in self.x:
            scenario = FlightHotelScenarioUCConflicting(self.prefix, size=i)
            self.results_Conflicting_long_UC.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
 
    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np
        # Figure for comparing alternative implementations | FlightHotel scenario
        fig2, ax = plt.subplots(layout="constrained")
        ax.plot(self.x, self.results_Sep_long, label="Separate indexes alternative")
        ax.plot(self.x, self.results_Plain_long, label="Plain implementation")
        ax.plot(self.x, self.results_Conflicting_long, label="Variant with conflicts")
        ax.plot(self.x, self.results_Sep_long_UC, label="Separate indexes (UC)")
        ax.plot(self.x, self.results_Plain_long_UC, label="Plain implementation (UC)")
        ax.plot(self.x, self.results_Conflicting_long_UC, label="Variant with conflicts (UC)")
        ax.set_title("Comparison of Uniqueness Constraints vs Indexes | FlightHotel scenario")
        ax.set_xlabel("number of rows per input relation")
        ax.set_ylabel("time (ms)")
        ax.legend()

    def print_cmd(self):
        print("## Figure for comparing Uniqueness Constraints vs Indexes | FlightHotel scenario")
        print("# Comparison of alternative implementations")
        print(f"{self.results_Sep_long=}")
        print(f"{self.results_Plain_long=}")
        print(f"{self.results_Conflicting_long=}")
        print(f"{self.results_Sep_long_UC=}")
        print(f"{self.results_Plain_long_UC=}")
        print(f"{self.results_Conflicting_long_UC=}")
