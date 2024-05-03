from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

class FigurePersonAddressScale(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True, scale=[2]):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        self.scale = scale
        # results
        self.results_personaddress_PI = []
        self.results_personaddress_CD_PI = []

    def compute(self):
        # execute PI_NI for PersonAddress on scale
        from scenarios.personaddressscale import PersonAddressScenarioPlainScale
        for i in self.x:
            for s in self.scale:
                scenario = PersonAddressScenarioPlainScale(self.prefix, size=i, scale=s)
                self.results_personaddress_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute CD/PI_NI for PersonAddress on scale
        from scenarios.personaddressscale import PersonAddressScenarioCDoverPlainScale
        for i in self.x:
            for s in self.scale:
                scenario = PersonAddressScenarioCDoverPlainScale(self.prefix, size=i, scale=s)
                self.results_personaddress_CD_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))

    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np

        sizes = [s * self.x[0] * 2 for s in self.scale]

        # Figure for investigating the horizontal scalability | PersonAddress Scenario
        fig2, ax = plt.subplots(layout="constrained", figsize=(6,3))
        ax.plot(sizes, self.results_personaddress_PI, label="PI", marker="D")
        ax.plot(sizes, self.results_personaddress_CD_PI, label="CD/PI", marker="s")
        ax.set_title("PersonAddress")
        ax.set_xlabel("total number of input nodes")
        ax.set_ylabel("time (ms)")
        ax.legend(loc="best", fontsize="10")
        
        plt.savefig("outfigs/FigureHorizontalScalePA.png")

    def print_cmd(self):
        print("## Figure for investigating the horizontal scalability | PersonAddress Scenario")
        print(f"{self.results_personaddress_PI=}")
        print(f"{self.results_personaddress_CD_PI=}")
