import matplotlib.pyplot as plt
import numpy as np

## Figure for comparing with the baseline approach | FlightHotel scenario
x=[100, 200, 500, 1_000, 2_000, 5_000]
results_baseline_long=[22.2, 51.0, 307.8, 1069.8, 3447.4, 20351.4]
results_baseline_NI_long=[88.4, 121.8, 256.2, 638.2, 1943.2, 10685.8]
results_PI_NI_long=[164.0, 160.8, 167.4, 203.4, 283.0, 423.8]

# Figure for comparing with the baseline approach | FlightHotel scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(4, 2.5))
ax.plot(x, results_baseline_long, label="B", marker="D") 
ax.plot(x, results_baseline_NI_long, label="B_NI", marker="s")
ax.plot(x, results_PI_NI_long, label="PI_NI", marker= "o")
ax.set_title("FlightHotel")
ax.set_xlabel("number of nodes per input label")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
ax.legend(loc="best", ncol=3)

plt.savefig("../../outfigs/FigureComparisonBaselineFlightHotel.png")
