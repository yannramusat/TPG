import matplotlib.pyplot as plt
import numpy as np

probs = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
## Figure for evaluating the impact of the frequency of conflicts | PersonData scenario
results_shuffle_rand_min=[540, 489, 485, 496, 479, 535, 483, 490, 480, 483, 538]
results_shuffle_rand=[552.0, 546.45, 545.65, 547.45, 545.3, 547.05, 542.6, 545.85, 544.55, 543.7, 547.45]
results_shuffle_rand_max=[570, 572, 591, 592, 591, 566, 564, 575, 576, 567, 586]

shuffle_rand_min_err = [a-b for (a,b) in zip(results_shuffle_rand, results_shuffle_rand_min)]
shuffle_rand_max_err = [a-b for (a,b) in zip(results_shuffle_rand_max, results_shuffle_rand)]

shuffle_rand_err=np.row_stack((shuffle_rand_min_err, shuffle_rand_max_err))

# Figure for evaluating the impact of the frequency of conflicts | PersonData scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(4,3))
ax.errorbar([str(p) for p in probs], results_shuffle_rand, yerr=shuffle_rand_err, fmt='.', linewidth=1, capsize=5, label="CD/PI; Randomized order", color="red")
ax.set_title("PersonData")
ax.set_xlabel("likelihood of conflicts (%)")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
ax.set_ylim([400, 1_100])
from matplotlib.ticker import NullFormatter
ax.yaxis.set_minor_formatter(NullFormatter())
ax.legend(loc="best")

plt.savefig("../../outfigs/FigureExhaustiveRandomPD.png")
