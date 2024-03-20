from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

class FigureDTA1RandomConflicts(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True, probs = [50]):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        self.probs = probs
        # results 
        self.results_shuffle_rand_min = []
        self.results_shuffle_rand = []
        self.results_shuffle_rand_max = []

    def compute(self):
        # execute the scenario DTA1 with random generation of conflict
        from scenarios.dta1 import DBLPToAmalgam1RandomConflicts
        for p in self.probs:
            for i in self.x:
                scenario = DBLPToAmalgam1RandomConflicts(self.prefix, size=i, prob_conflict = p)
                (rmin, ravg, rmax) = scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False, minmax=True)
                self.results_shuffle_rand_min.append(rmin)
                self.results_shuffle_rand.append(ravg)
                self.results_shuffle_rand_max.append(rmax)
    
    def plot(self):
        shuffle_rand_min_err = [a-b for (a,b) in zip(self.results_shuffle_rand, self.results_shuffle_rand_min)]
        shuffle_rand_max_err = [a-b for (a,b) in zip(self.results_shuffle_rand_max, self.results_shuffle_rand)]

        shuffle_rand_err=np.row_stack((shuffle_rand_min_err, shuffle_rand_max_err))

        # Figure for evaluating the impact of the frequency of conflicts | DBLPToAmalgam1 scenario
        fig2, ax = plt.subplots(layout="constrained", figsize=(4,3))
        ax.errorbar([str(p) for p in self.probs], self.results_shuffle_rand, yerr=shuffle_rand_err, fmt='.', linewidth=1, capsize=5, label="CD/PI; Randomized order", color="red")
        ax.set_title("DBLPToAmalgam1")
        ax.set_xlabel("likelihood of conflicts (%)")
        ax.set_ylabel("time (ms)")
        ax.set_yscale("log")
        #from matplotlib.ticker import NullFormatter
        #ax.yaxis.set_minor_formatter(NullFormatter())
        ax.legend(loc="best")
        
        plt.savefig("outfigs/FigureExhaustiveRandomDTA1.png")

    def print_cmd(self):
        print("## Figure for evaluating the impact of the frequency of conflicts | DBLPToAmalgam1 scenario")
        print(f"{self.results_shuffle_rand_min=}")
        print(f"{self.results_shuffle_rand=}")
        print(f"{self.results_shuffle_rand_max=}")