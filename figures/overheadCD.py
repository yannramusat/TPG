from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

class FigureOverheadCD(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        # results
        self.results_personaddress_PI = []
        self.results_personaddress_CD_PI = []
        self.results_flighthotel_PI = []
        self.results_flighthotel_CD_PI = []
        self.results_persondata_PI = []
        self.results_persondata_CD_PI = []
        self.results_dta1_PI = []
        self.results_dta1_CD_PI = []
        self.results_a1ta3_PI = []
        self.results_a1ta3_CD_PI = []

    def compute(self):
        # execute PI_NI for PersonAddress
        from scenarios.personaddress import PersonAddressScenarioPlain
        for i in self.x:
            scenario = PersonAddressScenarioPlain(self.prefix, size=i)
            self.results_personaddress_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute CD/PI_NI for PersonAddress
        from scenarios.personaddress import PersonAddressScenarioCDoverPlain
        for i in self.x:
            scenario = PersonAddressScenarioCDoverPlain(self.prefix, size=i)
            self.results_personaddress_CD_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute PI_NI for FlightHotel
        from scenarios.flighthotel import FlightHotelScenarioPlain
        for i in self.x:
            scenario = FlightHotelScenarioPlain(self.prefix, size=i)
            self.results_flighthotel_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute CD/PI_NI for FlightHotel
        from scenarios.flighthotel import FlightHotelScenarioCDoverPlain
        for i in self.x:
            scenario = FlightHotelScenarioCDoverPlain(self.prefix, size=i)
            self.results_flighthotel_CD_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute PI_NI for PersonData
        from scenarios.persondatas1 import PersonDataScenarioS1Plain
        for i in self.x:
            scenario = PersonDataScenarioS1Plain(self.prefix, size=i)
            self.results_persondata_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute CD/PI_NI for PersonData
        from scenarios.persondatas1 import PersonDataScenarioS1CDoverPlain
        for i in self.x:
            scenario = PersonDataScenarioS1CDoverPlain(self.prefix, size=i)
            self.results_persondata_CD_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute PI_NI for DBLPToAmalgam1
        from scenarios.dta1 import DBLPToAmalgam1Plain
        for i in self.x:
            scenario = DBLPToAmalgam1Plain(self.prefix, size=i)
            self.results_dta1_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute CD/PI_NI for DBLPToAmalgam1
        from scenarios.dta1 import DBLPToAmalgam1CDoverPlain
        for i in self.x:
            scenario = DBLPToAmalgam1CDoverPlain(self.prefix, size=i)
            self.results_dta1_CD_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute PI_NI for DBLPToAmalgam1
        from scenarios.a1ta3 import Amalgam1ToAmalgam3Plain
        for i in self.x:
            scenario = Amalgam1ToAmalgam3Plain(self.prefix, size=i)
            self.results_a1ta3_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))
        # execute CD/PI_NI for Amalgam1ToAmalgam3
        from scenarios.a1ta3 import Amalgam1ToAmalgam3CDoverPlain
        for i in self.x:
            scenario = Amalgam1ToAmalgam3CDoverPlain(self.prefix, size=i)
            self.results_a1ta3_CD_PI.append(scenario.run(self.app, launches=self.nbLaunches, stats=self.showStats, nodeIndex=True, relIndex=False))

    def plot(self):
        # plot results using matplotlib
        import matplotlib.pyplot as plt
        import numpy as np

        ratioPA = [a/b for a,b in zip(self.results_personaddress_CD_PI, self.results_personaddress_PI)]
        ratioFH = [a/b for a,b in zip(self.results_flighthotel_CD_PI, self.results_flighthotel_PI)]
        ratioPD = [a/b for a,b in zip(self.results_persondata_CD_PI, self.results_persondata_PI)]
        ratioDTA1 = [a/b for a,b in zip(self.results_dta1_CD_PI, self.results_dta1_PI)]
        ratioA1TA3 = [a/b for a,b in zip(self.results_a1ta3_CD_PI, self.results_a1ta3_PI)]

        # Figure for computing the overhead of performing conflict detection
        fig2, ax = plt.subplots(layout="constrained", figsize=(6,5))
        ax.plot(self.x, ratioPA, label="PersonAddress", marker="D")
        ax.plot(self.x, ratioFH, label="FlightHotel", marker="s")
        ax.plot(self.x, ratioPD, label="PersonData", marker="o")
        ax.plot(self.x, ratioDTA1, label="DBLPToAmalgam1", marker="x")
        ax.plot(self.x, ratioA1TA3, label="Amalgam1ToAmalgam3", marker="p")
        ax.set_title("Incurred overhead of Conflict Detection")
        ax.set_xlabel("number of rows per input relation")
        ax.set_ylabel("ratio {CD/PI} / {PI}")
        ax.legend(loc="best")

    def print_cmd(self):
        print("## Figure for computing the overhead of performing conflict detection")
        print(f"{self.results_personaddress_PI=}")
        print(f"{self.results_personaddress_CD_PI=}")
        print(f"{self.results_flighthotel_PI=}")
        print(f"{self.results_flighthotel_CD_PI=}")
        print(f"{self.results_persondata_PI=}")
        print(f"{self.results_persondata_CD_PI=}")
        print(f"{self.results_dta1_PI=}")
        print(f"{self.results_dta1_CD_PI=}")
        print(f"{self.results_a1ta3_PI=}")
        print(f"{self.results_a1ta3_CD_PI=}")
