import matplotlib.pyplot as plt
import numpy as np
from matplotlib.transforms import Affine2D

## Figure for comparing alternative implementations | A1TA3 scenario
x=[100, 200, 500, 1_000, 2_000, 5_000, 10_000]
# Comparison of alternative implementations
results_Plain_min=[416, 451, 541, 694, 1008, 1966, 3587]
results_Plain=[434.55, 466.65, 549.15, 705.15, 1029.8, 1984.85, 3688.5]
results_Plain_max=[474, 496, 560, 718, 1085, 2013, 3869]
results_Shuffled_min=[396, 426, 512, 669, 977, 1967, 3603]
results_Shuffled=[432.4, 456.5, 543.4, 696.6, 1021.45, 1991.8, 3670.7]
results_Shuffled_max=[501, 491, 591, 720, 1080, 2041, 3879]

Plain_min_err = [a-b for (a,b) in zip(results_Plain, results_Plain_min)]
Plain_max_err = [a-b for (a,b) in zip(results_Plain_max, results_Plain)]
Shuffled_min_err = [a-b for (a,b) in zip(results_Shuffled, results_Shuffled_min)]
Shuffled_max_err = [a-b for (a,b) in zip(results_Shuffled_max, results_Shuffled)]

Plain_err=np.row_stack((Plain_min_err, Plain_max_err))
Shuffled_err=np.row_stack((Shuffled_min_err, Shuffled_max_err))

# Figure for comparing alternative implementations | A1TA3 scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(4,2.5))
trans1 = Affine2D().translate(-0.05, 0.0) + ax.transData
trans2 = Affine2D().translate(+0.05, 0.0) + ax.transData
#ax.plot(x, results_Plain, label="PI", marker="D", color="blue")
ax.errorbar([str(p) for p in x], results_Plain, yerr=Plain_err, fmt='.', linewidth=1, capsize=5, label="PI; Fixed order", color="blue", transform=trans1)
#ax.plot(x, results_Shuffled, label="PI; Shuffled", marker="s", color="red")
ax.errorbar([str(p) for p in x], results_Shuffled, yerr=Shuffled_err, fmt='.', linewidth=1, capsize=5, label="PI; Randomized order", color="red", transform=trans2)
ax.set_title("Amalgam1ToAmalgam3")
ax.set_xlabel("nodes of nodes of each type")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
from matplotlib.ticker import NullFormatter
ax.yaxis.set_minor_formatter(NullFormatter())
ax.legend(loc="best")

plt.savefig("../../outfigs/FigureComparisonAlternativesA1TA3.png")
