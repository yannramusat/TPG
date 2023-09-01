from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

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
