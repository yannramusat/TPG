from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.transforms import Affine2D

class FigureComparisonIndexesGTB(Figure):
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
        # execute the separate indexes implementation of the scenario GTB
        from scenarios.gtb import GUSToBIOSQLSeparateIndexes
        for i in self.x:
            scenario = GUSToBIOSQLSeparateIndexes(self.prefix, size=i)
            self.results_Sep_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = GUSToBIOSQLSeparateIndexes(self.prefix, size=i)
            self.results_Sep_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = GUSToBIOSQLSeparateIndexes(self.prefix, size=i)
            self.results_Sep_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = GUSToBIOSQLSeparateIndexes(self.prefix, size=i)
            self.results_Sep.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False)) 
        # execute the plain implementation of the scenario GTB
        from scenarios.gtb import GUSToBIOSQLPlain
        for i in self.x:
            scenario = GUSToBIOSQLPlain(self.prefix, size=i)
            self.results_Plain_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = GUSToBIOSQLPlain(self.prefix, size=i)
            self.results_Plain_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = GUSToBIOSQLPlain(self.prefix, size=i)
            self.results_Plain_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True))
        for i in self.x:
            scenario = GUSToBIOSQLPlain(self.prefix, size=i)
            self.results_Plain.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the alternative implementation with Conflict Detection of the scenario GTB based on Separate index
        from scenarios.gtb import GUSToBIOSQLCDoverSI
        for i in self.x:
            scenario = GUSToBIOSQLCDoverSI(self.prefix, size=i)
            self.results_CDoverSI_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = GUSToBIOSQLCDoverSI(self.prefix, size=i)
            self.results_CDoverSI_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = GUSToBIOSQLCDoverSI(self.prefix, size=i)
            self.results_CDoverSI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True)) 
        for i in self.x:
            scenario = GUSToBIOSQLCDoverSI(self.prefix, size=i)
            self.results_CDoverSI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
        # execute the alternative implementation with Conflict Detection of the scenario GTB based on Plain implementation
        from scenarios.gtb import GUSToBIOSQLCDoverPlain
        for i in self.x:
            scenario = GUSToBIOSQLCDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_NI_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=True))
        for i in self.x:
            scenario = GUSToBIOSQLCDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_NI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        for i in self.x:
            scenario = GUSToBIOSQLCDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain_RI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=True)) 
        for i in self.x:
            scenario = GUSToBIOSQLCDoverPlain(self.prefix, size=i)
            self.results_CDoverPlain.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=False, relIndex=False))
    
    def plot(self):
        # Figure for exhaustive comparison of indexes | GTB scenario
        fig1, axs = plt.subplots(1, 4, layout="constrained", figsize=(12,3)) 
        fig1.suptitle("GUSToBIOSQL", fontsize=14)
        # Axes Sep implementation
        l1 = axs[0].plot(self.x, self.results_Sep_NI_RI, label="NI_RI", marker="D")
        l2 = axs[0].plot(self.x, self.results_Sep_NI, label="NI", marker="s")
        l3 = axs[0].plot(self.x, self.results_Sep_RI, label="RI", marker="o") 
        l4 = axs[0].plot(self.x, self.results_Sep, label="WI", marker="x")
        axs[0].set_title("SI")
        axs[0].set_xlabel("number of nodes of each type")
        axs[0].set_ylabel("time (ms)")
        axs[0].set_yscale("log")
        #axs[0].legend(loc="best", ncol=2, fontsize="10")
        # Axes Plain implementation
        axs[1].plot(self.x, self.results_Plain_NI_RI, label="NI_RI", marker="D")
        axs[1].plot(self.x, self.results_Plain_NI, label="NI", marker="s")
        axs[1].plot(self.x, self.results_Plain_RI, label="RI", marker="o") 
        axs[1].plot(self.x, self.results_Plain, label="WI", marker="x")
        axs[1].set_title("PI")
        axs[1].set_xlabel("number of nodes of each type")
        axs[1].set_ylabel("time (ms)")
        axs[1].set_yscale("log")
        #axs[0, 1].legend()
        # Axes Conflict Detection over Separate indexes
        axs[2].plot(self.x, self.results_CDoverSI_NI_RI, label="NI_RI", marker="D")
        axs[2].plot(self.x, self.results_CDoverSI_NI, label="NI", marker="s")
        axs[2].plot(self.x, self.results_CDoverSI_RI, label="RI", marker="o") 
        axs[2].plot(self.x, self.results_CDoverSI, label="WI", marker="x")
        axs[2].set_title("CD/SI")
        axs[2].set_xlabel("number of nodes of each type")
        axs[2].set_ylabel("time (ms)")
        axs[2].set_yscale("log")
        #axs[1, 0].legend()
        # Axes Conflict Detection over Plain
        axs[3].plot(self.x, self.results_CDoverPlain_NI_RI, label="NI_RI", marker="D")
        axs[3].plot(self.x, self.results_CDoverPlain_NI, label="NI", marker="s")
        axs[3].plot(self.x, self.results_CDoverPlain_RI, label="RI", marker="o") 
        axs[3].plot(self.x, self.results_CDoverPlain, label="WI", marker="x")
        axs[3].set_title("CD/PI")
        axs[3].set_xlabel("number of nodes of each type")
        axs[3].set_ylabel("time (ms)")
        axs[3].set_yscale("log")
        #axs[1, 1].legend()

        labels = ["NI_RI", "NI", "RI", "WI"]
        fig1.legend([l1, l2, l3, l4], labels=labels, ncol=4, loc="upper right")

        axs[0].set_ylim([20, 100_000])
        axs[1].set_ylim([20, 100_000])
        axs[2].set_ylim([20, 100_000])
        axs[3].set_ylim([20, 100_000])
        plt.savefig("outfigs/FigureExhaustiveIndexesGTB.png")

    def print_cmd(self):
        print("## Figure for exhaustive comparison of indexes | GTB scenario")
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

class FigureComparisonAlternativeApproachesGTB(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        # results
        self.results_Plain_min = []
        self.results_Plain = []
        self.results_Plain_max = []
        self.results_Shuffled_min = []
        self.results_Shuffled = []
        self.results_Shuffled_max = []

    def compute(self):
        # execute the plain implementation of the scenario GTB
        from scenarios.gtb import GUSToBIOSQLPlain
        for i in self.x:
            scenario = GUSToBIOSQLPlain(self.prefix, size=i)
            (rmin, ravg, rmax) = scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False, minmax=True)
            self.results_Plain_min.append(rmin)
            self.results_Plain.append(ravg)
            self.results_Plain_max.append(rmax)
        # execute the plain implementation (shuffled) of the scenario GTB
        from scenarios.gtb import GUSToBIOSQLPlain
        for i in self.x:
            scenario = GUSToBIOSQLPlain(self.prefix, size=i)
            (rmin, ravg, rmax) = scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False, shuffle=True, minmax=True)
            self.results_Shuffled_min.append(rmin)
            self.results_Shuffled.append(ravg)
            self.results_Shuffled_max.append(rmax)

    def plot(self):
        Plain_min_err = [a-b for (a,b) in zip(self.results_Plain, self.results_Plain_min)]
        Plain_max_err = [a-b for (a,b) in zip(self.results_Plain_max, self.results_Plain)]
        Shuffled_min_err = [a-b for (a,b) in zip(self.results_Shuffled, self.results_Shuffled_min)]
        Shuffled_max_err = [a-b for (a,b) in zip(self.results_Shuffled_max, self.results_Shuffled)]

        Plain_err=np.row_stack((Plain_min_err, Plain_max_err))
        Shuffled_err=np.row_stack((Shuffled_min_err, Shuffled_max_err))

        # Figure for comparing alternative implementations | GTB scenario
        fig2, ax = plt.subplots(layout="constrained", figsize=(4,3))
        trans1 = Affine2D().translate(-0.05, 0.0) + ax.transData
        trans2 = Affine2D().translate(+0.05, 0.0) + ax.transData
        #ax.plot(x, results_Plain, label="PI", marker="D", color="blue")
        ax.errorbar([str(p) for p in self.x], self.results_Plain, yerr=Plain_err, fmt='.', linewidth=1, capsize=5, label="PI; Fixed order", color="blue", transform=trans1)
        #ax.plot(x, results_Shuffled, label="PI; Shuffled", marker="s", color="red")
        ax.errorbar([str(p) for p in self.x], self.results_Shuffled, yerr=Shuffled_err, fmt='.', linewidth=1, capsize=5, label="PI; Randomized order", color="red", transform=trans2)
        ax.set_title("GUSToBIOSQL")
        ax.set_xlabel("number of nodes of each type")
        ax.set_ylabel("time (ms)")
        ax.set_yscale("log")
        from matplotlib.ticker import NullFormatter
        ax.yaxis.set_minor_formatter(NullFormatter())
        ax.legend(loc="best")

        plt.savefig("outfigs/FigureComparisonAlternativesGTB.png")

    def print_cmd(self):
        print("## Figure for comparing alternative implementations | GTB scenario")
        print("# Comparison of alternative implementations")
        print(f"{self.results_Plain_min=}")
        print(f"{self.results_Plain=}")
        print(f"{self.results_Plain_max=}")
        print(f"{self.results_Shuffled_min=}")
        print(f"{self.results_Shuffled=}")
        print(f"{self.results_Shuffled_max=}")
