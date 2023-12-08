from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.transforms import Affine2D

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
