from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

class FigureComparisonIndexesDTA1(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        # results 
        self.results_Plain_NI_RI = []
        self.results_Plain_NI = []
        self.results_Plain_RI = []
        self.results_Plain = []
        self.results_Shuffled_NI_RI = []
        self.results_Shuffled_NI = []
        self.results_Shuffled_RI = []
        self.results_Shuffled = []

    def compute(self):
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
        # execute the plain implementation (shuffled) of the scenario DTA1
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Shuffled_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True, shuffle=True))
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Shuffled_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False, shuffle=True))
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Shuffled_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True, shuffle=True)) 
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_Shuffled.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False, shuffle=True))
    
    def plot(self):
        # Figure for exhaustive comparison of indexes | DTA1 scenario
        fig1, axs = plt.subplots(1, 2, layout="constrained") 
        fig1.suptitle("DBLPToAmalgam1 scenario", fontsize=16)
        # Axes Plain implementation
        axs[0].plot(self.x, self.results_Plain_NI_RI, label="Indexes on Nodes and Relationships")
        axs[0].plot(self.x, self.results_Plain_NI, label="Indexes on Nodes only")
        axs[0].plot(self.x, self.results_Plain_RI, label="Indexes on Relationships only") 
        axs[0].plot(self.x, self.results_Plain, label="Without indexes")
        axs[0].set_title("Plain implementation")
        axs[0].set_xlabel("number of rows per input relation")
        axs[0].set_ylabel("time (ms)")
        axs[0].set_yscale("log")
        axs[0].legend()
        # Axes Plain implementation (shuffled)
        axs[1].plot(self.x, self.results_Shuffled_NI_RI, label="Indexes on Nodes and Relationships")
        axs[1].plot(self.x, self.results_Shuffled_NI, label="Indexes on Nodes only")
        axs[1].plot(self.x, self.results_Shuffled_RI, label="Indexes on Relationships only") 
        axs[1].plot(self.x, self.results_Shuffled, label="Without indexes")
        axs[1].set_title("Plain implementation; Shuffled")
        axs[1].set_xlabel("number of rows per input relation")
        axs[1].set_ylabel("time (ms)")
        axs[1].set_yscale("log")
        axs[1].legend()

    def print_cmd(self):
        print("## Figure for exhaustive comparison of indexes | DTA1 scenario")
        print("# Plain implementation")
        print(f"{self.results_Plain_NI_RI=}")
        print(f"{self.results_Plain_NI=}")
        print(f"{self.results_Plain_RI=}")
        print(f"{self.results_Plain=}")
        print("# Plain implementation (shuffled)")
        print(f"{self.results_Shuffled_NI_RI=}")
        print(f"{self.results_Shuffled_NI=}")
        print(f"{self.results_Shuffled_RI=}")
        print(f"{self.results_Shuffled=}")

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
        fig2, ax = plt.subplots(layout="constrained")
        ax.plot(self.x, self.results_Plain_long, label="Plain implementation")
        ax.plot(self.x, self.results_Shuffled_long, label="Plain implementation; Shuffled")
        ax.set_title("Comparison of alternative implementations | DBLPToAmalgam1 scenario")
        ax.set_xlabel("number of rows per input relation")
        ax.set_ylabel("time (ms)")
        ax.legend()

    def print_cmd(self):
        print("## Figure for comparing alternative implementations | DTA1 scenario")
        print("# Comparison of alternative implementations")
        print(f"{self.results_Plain_long=}")
        print(f"{self.results_Shuffled_long=}")
