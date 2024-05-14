import matplotlib.pyplot as plt
import numpy as np

## Figure for comparing with the baseline approach | PersonAddress scenario
x=[100, 200, 500, 1_000, 2_000, 5_000]
results_baseline_long=[21.6, 61.2, 393.8, 1885.8, 6387.0, 39919.0]
results_baseline_NI_long=[60.0, 77.0, 211.8, 623.2, 2234.2, 13240.4]
results_PI_NI_long=[94.8, 105.4, 116.0, 141.6, 196.4, 366.6]

# Figure for comparing with the baseline approach | PersonAddress scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(4, 2.5))
ax.plot(x, results_baseline_long, label="B", marker="D") 
ax.plot(x, results_baseline_NI_long, label="B_NI", marker="s")
ax.plot(x, results_PI_NI_long, label="PI_NI", marker= "o")
ax.set_title("PersonAddress")
ax.set_xlabel("number of nodes per input label")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
ax.legend(loc="best", ncol=3)

plt.savefig("../../outfigs/FigureComparisonBaselinePersonAddress.png")
