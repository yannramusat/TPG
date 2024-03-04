from app import App
from figures.figure import Figure
import matplotlib.pyplot as plt
import numpy as np

class FigureFlightHotelRandomConflicts(Figure):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True, probs = [50]):
        super().__init__(app, prefix, values, nbLaunches, showStats)
        self.probs = probs
        # results 
        self.results_rand = []

    def compute(self):
        # execute the scenario FlightHotel with random generation of conflict
        from scenarios.flighthotel import FlightHotelScenarioRandomConflicts
        for p in self.probs:
            for i in self.x:
                scenario = FlightHotelScenarioRandomConflicts(self.prefix, size=i, prob_conflict = p)
                self.results_rand.append(scenario.run(self.app, launches=self.nbLaunches, 
                    stats=self.showStats, nodeIndex=True, relIndex=False))
    
    def plot(self):
        pass

    def print_cmd(self):
        print("## Figure for evaluating the impact of the frequency of conflicts | FlightHotel scenario")
        print(f"{self.results_rand=}")