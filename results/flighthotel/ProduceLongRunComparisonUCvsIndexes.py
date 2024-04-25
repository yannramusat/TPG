import matplotlib.pyplot as plt
import numpy as np

## Figure for comparing Uniqueness Constraints vs Indexes | FlightHotel scenario
x=[100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000]
# Comparison of alternative implementations
results_Sep_long=[136.33333333333334, 150.0, 153.66666666666666, 180.66666666666666, 230.66666666666666, 367.3333333333333, 679.6666666666666, 1239.6666666666667, 2649.3333333333335, 5272.333333333333]
results_Plain_long=[148.33333333333334, 139.66666666666666, 145.33333333333334, 177.33333333333334, 221.66666666666666, 367.6666666666667, 628.0, 1125.6666666666667, 2716.0, 5517.333333333333]
results_Sep_long_UC=[86.33333333333333, 101.33333333333333, 103.0, 134.66666666666666, 201.0, 394.6666666666667, 710.0, 1285.0, 3219.3333333333335, 6588.0]
results_Plain_long_UC=[91.33333333333333, 89.33333333333333, 121.0, 144.0, 221.66666666666666, 392.3333333333333, 795.3333333333334, 1488.3333333333333, 3418.3333333333335, 6805.666666666667]
        
# Figure for comparing alternative implementations | FlightHotel scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(6,3))
ax.plot(x, results_Sep_long, label="SI_NI", marker="D")
ax.plot(x, results_Plain_long, label="PI_NI", marker="s")
ax.plot(x, results_Sep_long_UC, label="SI_NUC", marker="o")
ax.plot(x, results_Plain_long_UC, label="PI_NUC", marker="x")
ax.set_title("FlightHotel")
ax.set_xlabel("number of nodes of each type")
ax.set_ylabel("time (ms)")
ax.legend()

plt.savefig("../../outfigs/FigureComparisonUCvsIndexesFlightHotel.png")
