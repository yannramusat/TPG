import matplotlib.pyplot as plt
import numpy as np
from matplotlib.transforms import Affine2D

## Figure for comparing alternative implementations | GTB scenario
x=[100, 200, 500, 1_000, 2_000, 5_000, 10_000]
# Comparison of alternative implementations
results_Plain_min=[277, 291, 336, 409, 544, 941, 1714]
results_Plain=[285.85, 303.0, 352.85, 424.5, 568.85, 988.5, 1752.9]
results_Plain_max=[308, 312, 379, 459, 632, 1060, 1823]
results_Shuffled_min=[259, 276, 306, 371, 505, 891, 1601]
results_Shuffled=[276.6, 296.6, 326.7, 408.7, 534.45, 961.95, 1680.25]
results_Shuffled_max=[315, 318, 369, 450, 591, 1059, 1805]

Plain_min_err = [a-b for (a,b) in zip(results_Plain, results_Plain_min)]
Plain_max_err = [a-b for (a,b) in zip(results_Plain_max, results_Plain)]
Shuffled_min_err = [a-b for (a,b) in zip(results_Shuffled, results_Shuffled_min)]
Shuffled_max_err = [a-b for (a,b) in zip(results_Shuffled_max, results_Shuffled)]

Plain_err=np.row_stack((Plain_min_err, Plain_max_err))
Shuffled_err=np.row_stack((Shuffled_min_err, Shuffled_max_err))

# Figure for comparing alternative implementations | GTB scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(4,2.5))
trans1 = Affine2D().translate(-0.05, 0.0) + ax.transData
trans2 = Affine2D().translate(+0.05, 0.0) + ax.transData
#ax.plot(x, results_Plain, label="PI", marker="D", color="blue")
ax.errorbar([str(p) for p in x], results_Plain, yerr=Plain_err, fmt='.', linewidth=1, capsize=5, label="PI; Fixed order", color="blue", transform=trans1)
#ax.plot(x, results_Shuffled, label="PI; Shuffled", marker="s", color="red")
ax.errorbar([str(p) for p in x], results_Shuffled, yerr=Shuffled_err, fmt='.', linewidth=1, capsize=5, label="PI; Randomized order", color="red", transform=trans2)
ax.set_title("GUSToBIOSQL")
ax.set_xlabel("number of nodes per input label")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
from matplotlib.ticker import NullFormatter
ax.yaxis.set_minor_formatter(NullFormatter())
ax.legend(loc="best")

plt.savefig("../../outfigs/FigureComparisonAlternativesGTB.png")
