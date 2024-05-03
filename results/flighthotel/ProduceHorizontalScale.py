x=[5000]        
scale=[1, 2, 5, 10, 20, 50, 100]
## Figure for investigating the horizontal scalability | FlightHotel scenario
results_flighthotel_PI=[416.4, 802.0, 1985.6, 3997.0, 8057.4, 20530.8, 44311.0]
results_flighthotel_CD_PI=[433.8, 834.6, 2043.2, 4105.0, 8371.6, 21275.0, 46609.0]

import matplotlib.pyplot as plt
import numpy as np

sizes = [s * x[0] * 2 for s in scale]

# Figure for investigating the horizontal scalability | FlightHotel scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(6,3))
ax.plot(sizes, results_flighthotel_PI, label="PI", marker="D")
ax.plot(sizes, results_flighthotel_CD_PI, label="CD/PI", marker="s")
ax.set_title("FlightHotel")
ax.set_xlabel("total number of input nodes")
ax.set_ylabel("time (ms)")
ax.legend(loc="best", fontsize="10")
        
plt.savefig("../../outfigs/FigureHorizontalScaleFH.png")
