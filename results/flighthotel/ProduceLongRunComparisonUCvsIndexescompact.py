import matplotlib.pyplot as plt
import numpy as np

## Figure for comparing Uniqueness Constraints vs Indexes | FlightHotel scenario
x=[20_000, 50_000, 100_000]
# Comparison of alternative implementations
results_Sep_long=[1.2396666666666667, 2.6493333333333335, 5.272333333333333]
results_Plain_long=[1.1256666666666667, 2.7160, 5.517333333333333]
results_Sep_long_UC=[1.2850, 3.2193333333333335, 6.5880]
results_Plain_long_UC=[1.4883333333333333, 3.4183333333333335, 6.805666666666667]
        
# Figure for comparing alternative implementations | FlightHotel scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(6,2))
ax.plot(x, results_Sep_long, label="SI_NI", marker="D")
ax.plot(x, results_Plain_long, label="PI_NI", marker="s")
ax.plot(x, results_Sep_long_UC, label="SI_NUC", marker="o")
ax.plot(x, results_Plain_long_UC, label="PI_NUC", marker="x")
ax.set_title("FlightHotel")
ax.set_xlabel("number of nodes per input label")
ax.set_ylabel("time (s)")
ax.legend(loc="best", ncol=2)

plt.savefig("../../outfigs/FigureComparisonUCvsIndexesFlightHotelcompact.png")
