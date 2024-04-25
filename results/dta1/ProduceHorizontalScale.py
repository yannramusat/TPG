x=[5000]        
scale=[1, 2, 5, 10, 20, 50]
## Figure for investigating the horizontal scalability | DBLPToAmalgam1 scenario
results_dta1_PI=[1838.2, 3506.6, 8778.8, 20444.0, 39965.0, 101254.0]
results_dta1_CD_PI=[3017.4, 5868.8, 14919.4, 29046.0, 63614.0, 168257.0]

import matplotlib.pyplot as plt
import numpy as np

sizes = [s * x[0] * 7 for s in scale]

# Figure for investigating the horizontal scalability | DBLPToAmalgam1 scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(6,3))
ax.plot(sizes, results_dta1_PI, label="PI", marker="D")
ax.plot(sizes, results_dta1_CD_PI, label="CD/PI", marker="s")
ax.set_title("DBLPToAmalgam1")
ax.set_xlabel("total number of input nodes")
ax.set_ylabel("time (ms)")
ax.legend(loc="best", fontsize="10")
        
plt.savefig("../../outfigs/FigureHorizontalScaleDTA1.png")
