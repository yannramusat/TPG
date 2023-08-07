#!/bin/env python3
from app import App

class FigureComparisonIndexesPersonAddress(object):
    def __init__(self, prefix, values=[], nbLaunches=1, showStats=True):
        self.prefix = prefix
        self.x = values
        self.nbLaunches = nbLaunches
        self.showStats = showStats
        # results 
        self.results_Sep_NI_RI = []
        self.results_Sep_NI = []
        self.results_Sep_RI = []
        self.results_Sep = []
        self.results_Plain_NI_RI = []
        self.results_Plain_NI = []
        self.results_Plain_RI = []
        self.results_Plain = []
        self.results_CDoverSI_NI_RI = []
        self.results_CDoverSI_NI = []
        self.results_CDoverSI_RI = []
        self.results_CDoverSI = []
        self.results_CDoverPlain_NI_RI = []
        self.results_CDoverPlain_NI = []
        self.results_CDoverPlain_RI = []
        self.results_CDoverPlain = []

    def compute(self):
        # execute the alternative implementation of the scenario PersonAddress with Separate indexes
        from scenarios.personaddress import PersonAddressScenarioSeparateIndexes
        for i in self.x:
            scenario = PersonAddressScenarioSeparateIndexes(self.prefix, size=i)
            self.results_Sep_NI_RI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = PersonAddressScenarioSeparateIndexes(self.prefix, size=i)
            self.results_Sep_NI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = PersonAddressScenarioSeparateIndexes(self.prefix, size=i)
            self.results_Sep_RI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = PersonAddressScenarioSeparateIndexes(self.prefix, size=i)
            self.results_Sep.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the plain implementation of the scenario PersonAddress
        from scenarios.personaddress import PersonAddressScenarioPlain
        for i in self.x:
            scenario = PersonAddressScenarioPlain(self.prefix, size=i)
            self.results_Plain_NI_RI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = PersonAddressScenarioPlain(self.prefix, size=i)
            self.results_Plain_NI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = PersonAddressScenarioPlain(self.prefix, size=i)
            self.results_Plain_RI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = PersonAddressScenarioPlain(prefix, size=i)
            self.results_Plain.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the alternative implementation with Conflict Detection of the scenario PersonAddress based on Separate index
        from scenarios.personaddress import PersonAddressScenarioCDoverSI
        for i in self.x:
            scenario = PersonAddressScenarioCDoverSI(self.prefix, size=i)
            self.results_CDoverSI_NI_RI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = PersonAddressScenarioCDoverSI(self.prefix, size=i)
            self.results_CDoverSI_NI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = PersonAddressScenarioCDoverSI(self.prefix, size=i)
            self.results_CDoverSI_RI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True)) 
        for i in self.x:
            scenario = PersonAddressScenarioCDoverSI(self.prefix, size=i)
            self.results_CDoverSI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the alternative implementation with Conflict Detection of the scenario PersonAddress based on Plain implementation
        from scenarios.personaddress import PersonAddressScenarioCDoverPlain
        for i in self.x:
            scenario = PersonAddressScenarioCDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_NI_RI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = PersonAddressScenarioCDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_NI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = PersonAddressScenarioCDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_RI.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = PersonAddressScenarioCDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
    
    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np
        # Figure for exhaustive comparison of indexes | PersonAddress scenario
        fig1, axs = plt.subplots(2, 2, layout="constrained") 
        fig1.suptitle("PersonAddress scenario", fontsize=16)
        # Axes Separate indexes
        axs[0, 0].plot(x, self.results_Sep_NI_RI, label="Indexes on Nodes and Relations")
        axs[0, 0].plot(x, self.results_Sep_NI, label="Indexes on Nodes only")
        axs[0, 0].plot(x, self.results_Sep_RI, label="Indexes on Relations only")
        axs[0, 0].plot(x, self.results_Sep, label="Without indexes")
        axs[0, 0].set_title("Separate indexes alternative implementation")
        axs[0, 0].set_xlabel("number of rows in each input relation")
        axs[0, 0].set_ylabel("time (ms)")
        axs[0, 0].set_yscale("log")
        axs[0, 0].legend()
        # Axes Plain implementation
        axs[0, 1].plot(x, self.results_Plain_NI_RI, label="Indexes on Nodes and Relations")
        axs[0, 1].plot(x, self.results_Plain_NI, label="Indexes on Nodes only")
        axs[0, 1].plot(x, self.results_Plain_RI, label="Indexes on Relations only") 
        axs[0, 1].plot(x, self.results_Plain, label="Without indexes")
        axs[0, 1].set_title("Plain implementation")
        axs[0, 1].set_xlabel("number of rows in each input relation")
        axs[0, 1].set_ylabel("time (ms)")
        axs[0, 1].set_yscale("log")
        axs[0, 1].legend()
        # Axes Conflict Detection over Separate indexes
        axs[1, 0].plot(x, self.results_CDoverSI_NI_RI, label="Indexes on Nodes and Relations")
        axs[1, 0].plot(x, self.results_CDoverSI_NI, label="Indexes on Nodes only")
        axs[1, 0].plot(x, self.results_CDoverSI_RI, label="Indexes on Relations only") 
        axs[1, 0].plot(x, self.results_CDoverSI, label="Without indexes")
        axs[1, 0].set_title("Conflict Detection over Separate indexes")
        axs[1, 0].set_xlabel("number of rows in each input relation")
        axs[1, 0].set_ylabel("time (ms)")
        axs[1, 0].set_yscale("log")
        axs[1, 0].legend()
        # Axes Conflict Detection over Plain
        axs[1, 1].plot(x, self.results_CDoverPlain_NI_RI, label="Indexes on Nodes and Relations")
        axs[1, 1].plot(x, self.results_CDoverPlain_NI, label="Indexes on Nodes only")
        axs[1, 1].plot(x, self.results_CDoverPlain_RI, label="Indexes on Relations only") 
        axs[1, 1].plot(x, self.results_CDoverPlain, label="Without indexes")
        axs[1, 1].set_title("Conflict Detection over Plain implementation")
        axs[1, 1].set_xlabel("number of rows in each input relation")
        axs[1, 1].set_ylabel("time (ms)")
        axs[1, 1].set_yscale("log")
        axs[1, 1].legend()

    def print_cmd(self):
        print("## Figure for exhaustive comparison of indexes | PersonAddress scenario")
        print("# Separate indexes alternative")
        print(f"{self.results_Sep_NI_RI=}")
        print(f"{self.results_Sep_NI=}")
        print(f"{self.results_Sep_RI=}")
        print(f"{self.results_Sep=}")
        print("# Plain implementation")
        print(f"{self.results_Plain_NI_RI=}")
        print(f"{self.results_Plain_NI=}")
        print(f"{self.results_Plain_RI=}")
        print(f"{self.results_Plain=}")
        print("# Conflict Detection over Separate indexes")
        print(f"{self.results_CDoverSI_NI_RI=}")
        print(f"{self.results_CDoverSI_NI=}")
        print(f"{self.results_CDoverSI_RI=}")
        print(f"{self.results_CDoverSI=}")
        print("# Conflict Detection over Plain")
        print(f"{self.results_CDoverPlain_NI_RI=}")
        print(f"{self.results_CDoverPlain_NI=}")
        print(f"{self.results_CDoverPlain_RI=}")
        print(f"{self.results_CDoverPlain=}")

class FigureComparisonAlternativeApproachesPersonAddress(object):
    def __init__(self, prefix, values=[], nbLaunches=1, showStats=True):
        self.prefix = prefix
        self.x = values
        self.nbLaunches = nbLaunches
        self.showStats = showStats
        # results
        self.results_Sep_long = []
        self.results_Plain_long = []
        self.results_CDoverSI_long = []
        self.results_CDoverPlain_long = []

    def compute(self):
        # execute the alternative implementation of the scenario PersonAddress with Separate indexes
        from scenarios.personaddress import PersonAddressScenarioSeparateIndexes
        for i in self.x:
            scenario = PersonAddressScenarioSeparateIndexes(self.prefix, size=i)
            self.results_Sep_long.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the plain implementation of the scenario PersonAddress
        from scenarios.personaddress import PersonAddressScenarioPlain
        for i in self.x:
            scenario = PersonAddressScenarioPlain(self.prefix, size=i)
            self.results_Plain_long.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the alternative implementation with Conflict Detection of the scenario PersonAddress based on Separate index
        from scenarios.personaddress import PersonAddressScenarioCDoverSI
        for i in self.x:
            scenario = PersonAddressScenarioCDoverSI(self.prefix, size=i)
            self.results_CDoverSI_long.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the alternative implementation with Conflict Detection of the scenario PersonAddress based on Plain implementation
        from scenarios.personaddress import PersonAddressScenarioCDoverPlain
        for i in self.x:
            scenario = PersonAddressScenarioCDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_long.append(scenario.run(app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))

    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np
        # Figure for comparing alternative implementations | PersonAddress scenario
        fig2, ax = plt.subplots(layout="constrained")
        ax.plot(self.x, self.results_Sep_long, label="Separate indexes alternative")
        ax.plot(self.x, self.results_Plain_long, label="Plain implementation")
        ax.plot(self.x, self.results_CDoverSI_long, label="Conflict Detection over Separate indexes")
        ax.plot(self.x, self.results_CDoverPlain_long, label="Conflict Detection over Plain implementation")
        ax.set_title("Comparison of alternative implementations | PersonAddress scenario")
        ax.set_xlabel("number of rows in each input relation")
        ax.set_ylabel("time (ms)")
        ax.legend()

    def print_cmd(self):
        print("## Figure for comparing alternative implementations | PersonAddress scenario")
        print("# Comparison of alternative implementations")
        print(f"{self.results_Sep_long=}")
        print(f"{self.results_Plain_long=}")
        print(f"{self.results_CDoverSI_long=}")
        print(f"{self.results_CDoverPlain_long=}")

if __name__ == "__main__":
    # app setup
    scheme = "bolt"
    hostname = "localhost"
    port = "7687"
    uri = f"{scheme}://{hostname}:{port}"
    app = App(uri, "neo4j", verbose=False)

    # common prefix for CSVs; the data has been generated by iBench
    prefix = "file:///home/yann/research/ibench/build/ibench/"
    nbLaunches = 3
    showStats = True
    nodeIndexes = True
    relIndexes = True
    x = [100, 200]#, 500, 1_000] #, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000]
    y = [100, 200, 500]#, 1_000, 2_000, 5_000, 10_000] #, 20_000, 50_000, 100_000] 
    # choose which figures to use and their parameters
    figures = [
        FigureComparisonIndexesPersonAddress(prefix=prefix, values=x, nbLaunches=nbLaunches, showStats=showStats),
        FigureComparisonAlternativeApproachesPersonAddress(prefix=prefix, values=y, nbLaunches=nbLaunches, showStats=showStats)
    ]
    # launch experiments    
    for fig in figures:
        fig.compute()
    # print results into cmdline
    if showStats:
        for fig in figures:
            fig.print_cmd()
    # generate figures
    for fig in figures:
        fig.plot()
    # show all figures using matplotlib
    import matplotlib.pyplot as plt
    import numpy as np
    plt.show()
 
    # TEMPORARY BREAK POINT
    exit()

    # execute the Optimized alternative implementation of the scenario FlightHotel
    from scenarios.flighthotel import FlightHotelScenarioSeparateIndexes
    resultsOptiFH = []
    for i in x:
        scenario = FlightHotelScenarioSeparateIndexes(prefix, size=i)
        resultsOptiFH.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=True))

    # execute the Optimized alternative implementation of the scenario FlightHotel (without Indexes!)
    resultsNoIndexFH = []
    for i in x:
        scenario = FlightHotelScenarioSeparateIndexes(prefix, size=i)
        resultsNoIndexFH.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=False))

    # optional printing of the results in the console
    if showStats:
        print(resultsOptiFH)
        print(resultsNoIndexFH)
   
    #####
    # TEMPORARY PLOTTING
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(layout="constrained")
    ax.plot(x, resultsOptiFH, label="Optimized")
    ax.plot(x, resultsNoIndexFH, label="Without indexes")
    ax.set_title("$\mathtt{FlightHotel}$ scenario")
    ax.set_xlabel("cardinality of input relations")
    ax.set_ylabel("time [ms]")
    ax.set_yscale("log")
    ax.legend()
    plt.show()
    #####

    # close connection
    app.close()
