import matplotlib.pyplot as plt
import numpy as np
from matplotlib.transforms import Affine2D

## Figure for comparing alternative implementations | DTA1 scenario
x=[100, 200, 500, 1_000, 2_000, 5_000, 10_000]
# Comparison of alternative implementations
results_Plain_min=[431, 460, 501, 590, 790, 1474, 2780]
results_Plain=[473.35, 503.3, 518.9, 606.8, 806.3, 1502.7, 2823.85]
results_Plain_max=[522, 536, 546, 628, 875, 1548, 2864]
results_Shuffled_min=[404, 430, 494, 569, 773, 1464, 2784]
results_Shuffled=[439.3, 458.65, 512.7, 604.0, 800.85, 1503.95, 2841.75]
results_Shuffled_max=[500, 494, 537, 642, 835, 1550, 2907]

Plain_min_err = [a-b for (a,b) in zip(results_Plain, results_Plain_min)]
Plain_max_err = [a-b for (a,b) in zip(results_Plain_max, results_Plain)]
Shuffled_min_err = [a-b for (a,b) in zip(results_Shuffled, results_Shuffled_min)]
Shuffled_max_err = [a-b for (a,b) in zip(results_Shuffled_max, results_Shuffled)]

Plain_err=np.row_stack((Plain_min_err, Plain_max_err))
Shuffled_err=np.row_stack((Shuffled_min_err, Shuffled_max_err))

# Figure for comparing alternative implementations | DTA1 scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(4,2.5))
trans1 = Affine2D().translate(-0.05, 0.0) + ax.transData
trans2 = Affine2D().translate(+0.05, 0.0) + ax.transData
#ax.plot(x, results_Plain, label="PI", marker="D", color="blue")
ax.errorbar([str(p) for p in x], results_Plain, yerr=Plain_err, fmt='.', linewidth=1, capsize=5, label="PI; Fixed order", color="blue", transform=trans1)
#ax.plot(x, results_Shuffled, label="PI; Shuffled", marker="s", color="red")
ax.errorbar([str(p) for p in x], results_Shuffled, yerr=Shuffled_err, fmt='.', linewidth=1, capsize=5, label="PI; Randomized order", color="red", transform=trans2)
ax.set_title("DBLPToAmalgam1")
ax.set_xlabel("number of nodes per input label")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
from matplotlib.ticker import NullFormatter
ax.yaxis.set_minor_formatter(NullFormatter())
ax.legend(loc="best")

plt.savefig("../../outfigs/FigureComparisonAlternativesDTA1.png")
