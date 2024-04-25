import matplotlib.pyplot as plt
import numpy as np

probs = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
## Figure for evaluating the impact of the frequency of conflicts | PersonAddress scenario
results_shuffle_rand_min=[770, 768, 761, 750, 746, 759, 759, 737, 752, 749, 758]
results_shuffle_rand=[790.0, 784.25, 780.1, 775.75, 779.6, 775.85, 773.75, 764.6, 772.7, 779.65, 777.6]
results_shuffle_rand_max=[828, 811, 824, 807, 809, 801, 806, 784, 834, 832, 794]

shuffle_rand_min_err = [a-b for (a,b) in zip(results_shuffle_rand, results_shuffle_rand_min)]
shuffle_rand_max_err = [a-b for (a,b) in zip(results_shuffle_rand_max, results_shuffle_rand)]

shuffle_rand_err=np.row_stack((shuffle_rand_min_err, shuffle_rand_max_err))

# Figure for evaluating the impact of the frequency of conflicts | PersonAddress scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(4,3))
ax.errorbar([str(p) for p in probs], results_shuffle_rand, yerr=shuffle_rand_err, fmt='.', linewidth=1, capsize=5, label="CD/PI; Randomized order", color="red")
ax.set_title("PersonAddress")
ax.set_xlabel("likelihood of conflicts (%)")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
ax.set_ylim([400, 1_100])
from matplotlib.ticker import NullFormatter
ax.yaxis.set_minor_formatter(NullFormatter())
ax.legend(loc="best")

plt.savefig("../../outfigs/FigureExhaustiveRandomPA.png")
