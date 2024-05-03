import matplotlib.pyplot as plt
import numpy as np

probs = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
## Figure for evaluating the impact of the frequency of conflicts | FlightHotel scenario
results_shuffle_rand_min=[751, 736, 662, 716, 709, 702, 625, 622, 619, 615, 695]
results_shuffle_rand=[830.7, 780.5, 761.1, 756.05, 747.05, 723.65, 720.25, 709.0, 697.15, 698.9, 714.7]
results_shuffle_rand_max=[1038, 904, 911, 781, 822, 771, 806, 760, 733, 717, 746]

shuffle_rand_min_err = [a-b for (a,b) in zip(results_shuffle_rand, results_shuffle_rand_min)]
shuffle_rand_max_err = [a-b for (a,b) in zip(results_shuffle_rand_max, results_shuffle_rand)]

shuffle_rand_err=np.row_stack((shuffle_rand_min_err, shuffle_rand_max_err))

# Figure for evaluating the impact of the frequency of conflicts | FlightHotel scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(4,3))
ax.errorbar([str(p) for p in probs], results_shuffle_rand, yerr=shuffle_rand_err, fmt='.', linewidth=1, capsize=5, label="CD/PI; Randomized order", color="red")
ax.set_title("FlightHotel")
ax.set_xlabel("likelihood of conflicts (%)")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
ax.set_ylim([400, 1_100])
from matplotlib.ticker import NullFormatter
ax.yaxis.set_minor_formatter(NullFormatter())
ax.legend(loc="best")

plt.savefig("../../outfigs/FigureExhaustiveRandomFH.png")
