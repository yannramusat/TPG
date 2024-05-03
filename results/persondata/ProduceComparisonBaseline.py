import matplotlib.pyplot as plt
import numpy as np

## Figure for comparing with the baseline approach | PersonData scenario
x=[100, 200, 500, 1_000, 2_000, 5_000]
results_baseline_long=[16.8, 21.8, 105.2, 329.4, 1177.0, 6629.6]
results_baseline_NI_long=[83.4, 84.0, 83.8, 95.0, 128.4, 217.2]
results_PI_NI_long=[100.2, 102.0, 113.4, 131.0, 167.8, 297.2]

# Figure for comparing with the baseline approach | PersonData scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(4, 2.5))
ax.plot(x, results_baseline_long, label="B", marker="D") 
ax.plot(x, results_baseline_NI_long, label="B_NI", marker="s")
ax.plot(x, results_PI_NI_long, label="PI_NI", marker= "o")
ax.set_title("PersonData")
ax.set_xlabel("number of nodes of each type")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
ax.legend(loc="best", ncol=3)

plt.savefig("../../outfigs/FigureComparisonBaselinePersonData.png")
