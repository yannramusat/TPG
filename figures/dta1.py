from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

class FigureComparisonIndexesDTA1(Figure):
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
        self.results_Sep_Shuffled_NI_RI = []
        self.results_Sep_Shuffled_NI = []
        self.results_Sep_Shuffled_RI = []
        self.results_Sep_Shuffled = []
        self.results_Plain_Shuffled_NI_RI = []
        self.results_Plain_Shuffled_NI = []
        self.results_Plain_Shuffled_RI = []
        self.results_Plain_Shuffled = []

    def compute(self):
        # execute the separate indexes implementation of the scenario DTA1
        from scenarios.dta1 import DBLPToAmalgam1SeparateIndexes
        for i in self.x:
            scenario = DBLPToAmalgam1SeparateIndexes(self.prefix, size=i)
            self.results_Sep_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = DBLPToAmalgam1SeparateIndexes(self.prefix, size=i)
            self.results_Sep_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = DBLPToAmalgam1SeparateIndexes(self.prefix, size=i)
            self.results_Sep_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = DBLPToAmalgam1SeparateIndexes(self.prefix, size=i)
            self.results_Sep.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False)) 
        # execute the plain implementation of the scenario DTA1
        from scenarios.dta1 import DBLPToAmalgam1Plain
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Plain_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Plain_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Plain_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Plain.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the separate indexes implementation (shuffled) of the scenario DTA1
        for i in self.x:
            scenario = DBLPToAmalgam1SeparateIndexes(self.prefix, size=i)
            self.results_Sep_Shuffled_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True, shuffle=True))
        for i in self.x:
            scenario = DBLPToAmalgam1SeparateIndexes(self.prefix, size=i)
            self.results_Sep_Shuffled_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False, shuffle=True))
        for i in self.x:
            scenario = DBLPToAmalgam1SeparateIndexes(self.prefix, size=i)
            self.results_Sep_Shuffled_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True, shuffle=True)) 
        for i in self.x:
            scenario = DBLPToAmalgam1SeparateIndexes(self.prefix, size=i)
            self.results_Sep_Shuffled.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False, shuffle=True))
        # execute the plain implementation (shuffled) of the scenario DTA1
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Plain_Shuffled_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True, shuffle=True))
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Plain_Shuffled_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False, shuffle=True))
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Plain_Shuffled_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True, shuffle=True)) 
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Plain_Shuffled.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False, shuffle=True))
    
    def plot(self):
        # Figure for exhaustive comparison of indexes | DTA1 scenario
        fig1, axs = plt.subplots(2, 2, layout="constrained", figsize=(6,5)) 
        fig1.suptitle("DBLPToAmalgam1", fontsize=14)
        # Axes Sep implementation
        axs[0, 0].plot(self.x, self.results_Sep_NI_RI, label="NI_RI", marker="D")
        axs[0, 0].plot(self.x, self.results_Sep_NI, label="NI", marker="s")
        axs[0, 0].plot(self.x, self.results_Sep_RI, label="RI", marker="o") 
        axs[0, 0].plot(self.x, self.results_Sep, label="WI", marker="x")
        axs[0, 0].set_title("SI")
        axs[0, 0].set_xlabel("number of nodes of each type")
        axs[0, 0].set_ylabel("time (ms)")
        axs[0, 0].set_yscale("log")
        axs[0, 0].legend(loc="best", ncol=2, fontsize="10")
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
        # Axes Sep implementation (shuffled)
        axs[1, 0].plot(self.x, self.results_Sep_Shuffled_NI_RI, label="NI_RI", marker="D")
        axs[1, 0].plot(self.x, self.results_Sep_Shuffled_NI, label="NI", marker="s")
        axs[1, 0].plot(self.x, self.results_Sep_Shuffled_RI, label="RI", marker="o") 
        axs[1, 0].plot(self.x, self.results_Sep_Shuffled, label="WI", marker="x")
        axs[1, 0].set_title("SI; Shuffled")
        axs[1, 0].set_xlabel("number of nodes of each type")
        axs[1, 0].set_ylabel("time (ms)")
        axs[1, 0].set_yscale("log")
        #axs[1, 0].legend()
        # Axes Plain implementation (shuffled)
        axs[1, 1].plot(self.x, self.results_Plain_Shuffled_NI_RI, label="NI_RI", marker="D")
        axs[1, 1].plot(self.x, self.results_Plain_Shuffled_NI, label="NI", marker="s")
        axs[1, 1].plot(self.x, self.results_Plain_Shuffled_RI, label="RI", marker="o") 
        axs[1, 1].plot(self.x, self.results_Plain_Shuffled, label="WI", marker="x")
        axs[1, 1].set_title("PI; Shuffled")
        axs[1, 1].set_xlabel("number of nodes of each type")
        axs[1, 1].set_ylabel("time (ms)")
        axs[1, 1].set_yscale("log")
        #axs[1, 1].legend()

        axs[0, 0].set_ylim([20, 100_000])
        axs[0, 1].set_ylim([20, 100_000])
        axs[1, 0].set_ylim([20, 100_000])
        axs[1, 1].set_ylim([20, 100_000])
        plt.savefig("outfigs/FigureExhaustiveIndexesDTA1.png")

    def print_cmd(self):
        print("## Figure for exhaustive comparison of indexes | DTA1 scenario")
        print("# Separate indexes implementation")
        print(f"{self.results_Sep_NI_RI=}")
        print(f"{self.results_Sep_NI=}")
        print(f"{self.results_Sep_RI=}")
        print(f"{self.results_Sep=}")
        print("# Plain implementation")
        print(f"{self.results_Plain_NI_RI=}")
        print(f"{self.results_Plain_NI=}")
        print(f"{self.results_Plain_RI=}")
        print(f"{self.results_Plain=}")
        print("# Separate indexes implementation (shuffled)")
        print(f"{self.results_Sep_Shuffled_NI_RI=}")
        print(f"{self.results_Sep_Shuffled_NI=}")
        print(f"{self.results_Sep_Shuffled_RI=}")
        print(f"{self.results_Sep_Shuffled=}")
        print("# Plain implementation (shuffled)")
        print(f"{self.results_Plain_Shuffled_NI_RI=}")
        print(f"{self.results_Plain_Shuffled_NI=}")
        print(f"{self.results_Plain_Shuffled_RI=}")
        print(f"{self.results_Plain_Shuffled=}")

class FigureComparisonAlternativeApproachesDTA1(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        # results
        self.results_Plain_long = []
        self.results_Shuffled_long = []

    def compute(self):
        # execute the plain implementation of the scenario DTA1
        from scenarios.dta1 import DBLPToAmalgam1Plain
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Plain_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute the plain implementation (shuffled) of the scenario DTA1
        from scenarios.dta1 import DBLPToAmalgam1Plain
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Shuffled_long.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False, shuffle=True))

    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np
        # Figure for comparing alternative implementations | DTA1 scenario
        fig2, ax = plt.subplots(layout="constrained", figsize=(6,3))
        ax.plot(self.x, self.results_Plain_long, label="PI", marker="D")
        ax.plot(self.x, self.results_Shuffled_long, label="PI; Shuffled", marker="s")
        ax.set_title("DBLPToAmalgam1")
        ax.set_xlabel("number of nodes of each type")
        ax.set_ylabel("time (ms)")
        ax.legend()
        
        plt.savefig("outfigs/FigureComparisonAlternativesDTA1.png")

    def print_cmd(self):
        print("## Figure for comparing alternative implementations | DTA1 scenario")
        print("# Comparison of alternative implementations")
        print(f"{self.results_Plain_long=}")
        print(f"{self.results_Shuffled_long=}")
