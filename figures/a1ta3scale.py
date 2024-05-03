from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

class FigureA1TA3Scale(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True, scale=[2]):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        self.scale = scale
        # results
        self.results_a1ta3_PI = []
        self.results_a1ta3_CD_PI = []

    def compute(self):
        # execute PI_NI for Amalgam1ToAmalgam3 on scale
        from scenarios.a1ta3scale import Amalgam1ToAmalgam3PlainScale
        for i in self.x:
            for s in self.scale:
                scenario = Amalgam1ToAmalgam3PlainScale(self.prefix, size=i, scale=s)
                self.results_a1ta3_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute CD/PI_NI for Amalgam1ToAmalgam3 on scale
        from scenarios.a1ta3scale import Amalgam1ToAmalgam3CDoverPlainScale
        for i in self.x:
            for s in self.scale:
                scenario = Amalgam1ToAmalgam3CDoverPlainScale(self.prefix, size=i, scale=s)
                self.results_a1ta3_CD_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))

    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np

        sizes = [s * self.x[0] * 15 for s in self.scale]

        # Figure for investigating the horizontal scalability | Amalgam1ToAmalgam3 scenario
        fig2, ax = plt.subplots(layout="constrained", figsize=(6,3))
        ax.plot(sizes, self.results_a1ta3_PI, label="PI", marker="D")
        ax.plot(sizes, self.results_a1ta3_CD_PI, label="CD/PI", marker="s")
        ax.set_title("Amalgam1ToAmalgam3")
        ax.set_xlabel("total number of input nodes")
        ax.set_ylabel("time (ms)")
        ax.legend(loc="best", fontsize="10")
        
        plt.savefig("outfigs/FigureHorizontalScaleA1TA3.png")

    def print_cmd(self):
        print("## Figure for investigating the horizontal scalability | Amalgam1ToAmalgam3 scenario")
        print(f"{self.results_a1ta3_PI=}")
        print(f"{self.results_a1ta3_CD_PI=}")
