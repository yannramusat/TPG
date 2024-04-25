x=[5000]        
scale=[1, 2, 5, 10, 20]
## Figure for investigating the horizontal scalability | Amalgam1ToAmalgam3 scenario
results_a1ta3_PI=[2.2510, 4.5350, 11.2396, 22.7400, 52.9480]
results_a1ta3_CD_PI=[3.0326, 5.9308, 14.8592, 29.6832, 67.3820]

import matplotlib.pyplot as plt
import numpy as np

sizes = [s * x[0] * 15 for s in scale]

# Figure for investigating the horizontal scalability | Amalgam1ToAmalgam 3scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(3,2))
ax.plot(sizes, results_a1ta3_PI, label="PI", marker="D")
ax.plot(sizes, results_a1ta3_CD_PI, label="CD/PI", marker="s")
ax.set_title("Amalgam1ToAmalgam3")
ax.set_xlabel("total number of input nodes")
ax.set_ylabel("time (s)")
ax.legend(loc="best", fontsize="10")
        
plt.savefig("../../outfigs/FigureHorizontalScaleA1TA3compact.png")
