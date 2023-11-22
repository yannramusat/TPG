from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

class FigureComparisonIndexesPersonData(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        super().__init__(app, prefix, values, nbLaunches, showStats)
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
        # execute the alternative implementation of the scenario PersonData with Separate indexes
        from scenarios.persondatas1 import PersonDataScenarioS1Sep
        for i in self.x:
            scenario = PersonDataScenarioS1Sep(self.prefix, size=i)
            self.results_Sep_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = PersonDataScenarioS1Sep(self.prefix, size=i)
            self.results_Sep_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = PersonDataScenarioS1Sep(self.prefix, size=i)
            self.results_Sep_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = PersonDataScenarioS1Sep(self.prefix, size=i)
            self.results_Sep.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the plain implementation of the scenario PersonData
        from scenarios.persondatas1 import PersonDataScenarioS1Plain
        for i in self.x:
            scenario = PersonDataScenarioS1Plain(self.prefix, size=i)
            self.results_Plain_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = PersonDataScenarioS1Plain(self.prefix, size=i)
            self.results_Plain_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = PersonDataScenarioS1Plain(self.prefix, size=i)
            self.results_Plain_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = PersonDataScenarioS1Plain(self.prefix, size=i)
            self.results_Plain.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the alternative implementation with Conflict Detection of the scenario PersonData based on Separate index
        from scenarios.persondatas1 import PersonDataScenarioS1CDoverSep
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverSep(self.prefix, size=i)
            self.results_CDoverSI_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverSep(self.prefix, size=i)
            self.results_CDoverSI_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverSep(self.prefix, size=i)
            self.results_CDoverSI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True)) 
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverSep(self.prefix, size=i)
            self.results_CDoverSI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the alternative implementation with Conflict Detection of the scenario PersonData based on Plain implementation
        from scenarios.persondatas1 import PersonDataScenarioS1CDoverPlain
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
    
    def plot(self):
        # Figure for exhaustive comparison of indexes | PersonData scenario
        fig1, axs = plt.subplots(2, 2, layout="constrained", figsize=(6,5)) 
        fig1.suptitle("PersonData", fontsize=14)
        # Axes Separate indexes
        axs[0, 0].plot(self.x, self.results_Sep_NI_RI, label="NI_RI", marker="D")
        axs[0, 0].plot(self.x, self.results_Sep_NI, label="NI", marker="s")
        axs[0, 0].plot(self.x, self.results_Sep_RI, label="RI", marker="o")
        axs[0, 0].plot(self.x, self.results_Sep, label="WI", marker="x")
        axs[0, 0].set_title("SI")
        axs[0, 0].set_xlabel("number of nodes of each type")
        axs[0, 0].set_ylabel("time (ms)")
        axs[0, 0].set_yscale("log")
        axs[0, 0].legend()
        # Axes Plain implementation
        axs[0, 1].plot(self.x, self.results_Plain_NI_RI, label="NI_RI", marker="D")
        axs[0, 1].plot(self.x, self.results_Plain_NI, label="NI", marker="s")
        axs[0, 1].plot(self.x, self.results_Plain_RI, label="RI", marker="o") 
        axs[0, 1].plot(self.x, self.results_Plain, label="WI", marker="x")
        axs[0, 1].set_title("PI")
        axs[0, 1].set_xlabel("number of nodes of each type")
        axs[0, 1].set_ylabel("time (ms)")
        axs[0, 1].set_yscale("log")
        #axs[0, 1].legend()
        # Axes Conflict Detection over Separate indexes
        axs[1, 0].plot(self.x, self.results_CDoverSI_NI_RI, label="NI_RI", marker="D")
        axs[1, 0].plot(self.x, self.results_CDoverSI_NI, label="NI", marker="s")
        axs[1, 0].plot(self.x, self.results_CDoverSI_RI, label="RI", marker="o") 
        axs[1, 0].plot(self.x, self.results_CDoverSI, label="WI", marker="x")
        axs[1, 0].set_title("CD/SI")
        axs[1, 0].set_xlabel("number of nodes of each type")
        axs[1, 0].set_ylabel("time (ms)")
        axs[1, 0].set_yscale("log")
        #axs[1, 0].legend()
        # Axes Conflict Detection over Plain
        axs[1, 1].plot(self.x, self.results_CDoverPlain_NI_RI, label="NI_RI", marker="D")
        axs[1, 1].plot(self.x, self.results_CDoverPlain_NI, label="NI", marker="s")
        axs[1, 1].plot(self.x, self.results_CDoverPlain_RI, label="RI", marker="o") 
        axs[1, 1].plot(self.x, self.results_CDoverPlain, label="WI", marker="x")
        axs[1, 1].set_title("CD/PI")
        axs[1, 1].set_xlabel("number of nodes of each type")
        axs[1, 1].set_ylabel("time (ms)")
        axs[1, 1].set_yscale("log")
        #axs[1, 1].legend()

        axs[0, 0].set_ylim([20, 10_000])
        axs[0, 1].set_ylim([20, 10_000])
        axs[1, 0].set_ylim([20, 10_000])
        axs[1, 1].set_ylim([20, 10_000])
        plt.savefig("outfigs/FigureExhaustiveIndexesPersonData.png")

    def print_cmd(self):
        print("## Figure for exhaustive comparison of indexes | PersonData scenario")
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

class FigureLongRunPersonData(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        # results
        self.results_Plain_S1_long = []
        self.results_CD_S1_long = []
        self.results_Plain_S2_long = []
        self.results_CD_S2_long = []

    def compute(self):
        # Step 1 - Plain
        from scenarios.persondatas1 import PersonDataScenarioS1Plain
        for i in self.x:
            scenario = PersonDataScenarioS1Plain(self.prefix, size=i)
            self.results_Plain_S1_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # Step 1 - CD
        from scenarios.persondatas1 import PersonDataScenarioS1CDoverPlain
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverPlain(self.prefix, size=i)
            self.results_CD_S1_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # Step 2 - Plain
        from scenarios.persondatas2 import PersonDataScenarioS2Plain
        for i in self.x:
            scenario = PersonDataScenarioS2Plain(self.prefix, size=i)
            self.results_Plain_S2_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # Step 2 - CD
        from scenarios.persondatas2 import PersonDataScenarioS2CDoverPlain
        for i in self.x:
            scenario = PersonDataScenarioS2CDoverPlain(self.prefix, size=i)
            self.results_CD_S2_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))

    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np
        # Figure for long run evaluation | PersonData scenario
        fig2, ax = plt.subplots(layout="constrained")
        ax.plot(self.x, self.results_Plain_S1_long, label="Plain implementation; First step")
        ax.plot(self.x, self.results_CD_S1_long, label="Conflict Detection; First step")
        ax.plot(self.x, self.results_Plain_S2_long, label="Plain implementation; Second step")
        ax.plot(self.x, self.results_CD_S2_long, label="Conflict Detection; Second step")
        ax.set_title("Long run evaluation | PersonData scenario")
        ax.set_xlabel("number of rows per input relation")
        ax.set_ylabel("time (ms)")
        ax.legend()

    def print_cmd(self):
        print("## Figure for long run evaluation | PersonData scenario")
        print("# Long run evaluation")
        print(f"{self.results_Plain_S1_long=}")
        print(f"{self.results_CD_S1_long=}")
        print(f"{self.results_Plain_S2_long=}")
        print(f"{self.results_CD_S2_long=}")

class FigureComparisonBaselinePersonData(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        # results
        self.results_baseline_long = []
        self.results_baseline_NI_long = []
        self.results_PI_NI_long = []

    def compute(self):
        # execute the baseline implementation of the scenario PersonData
        from scenarios.persondatas1 import PersonDataScenarioS1Baseline
        for i in self.x:
            scenario = PersonDataScenarioS1Baseline(self.prefix, size=i)
            self.results_baseline_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the baseline implementation of the scenario PersonData using Node indexes
        from scenarios.persondatas1 import PersonDataScenarioS1Baseline
        for i in self.x:
            scenario = PersonDataScenarioS1Baseline(self.prefix, size=i)
            self.results_baseline_NI_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the plain implementation of the scenario PersonData using Node indexes
        from scenarios.persondatas1 import PersonDataScenarioS1Plain
        for i in self.x:
            scenario = PersonDataScenarioS1Plain(self.prefix, size=i)
            self.results_PI_NI_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))

    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np
        # Figure for comparing with the baseline approach | PersonData scenario
        fig2, ax = plt.subplots(layout="constrained", figsize=(4, 2.5))
        ax.plot(self.x, self.results_baseline_long, label="B", marker="D") 
        ax.plot(self.x, self.results_baseline_NI_long, label="B_NI", marker="s")
        ax.plot(self.x, self.results_PI_NI_long, label="PI_NI", marker= "o")
        ax.set_title("PersonData")
        ax.set_xlabel("number of nodes of each type")
        ax.set_ylabel("time (ms)")
        ax.legend()
        plt.savefig("outfigs/FigureComparisonBaselinePersonData.png")

    def print_cmd(self):
        print("## Figure for comparing with the baseline approach | PersonData scenario")
        print(f"{self.results_baseline_long=}")
        print(f"{self.results_baseline_NI_long=}")
        print(f"{self.results_PI_NI_long=}")
