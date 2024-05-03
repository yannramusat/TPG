x=[5000]        
scale=[1, 2, 5, 10, 20, 50]
## Figure for investigating the horizontal scalability | GUSToBIOSQL scenario
results_gtb_PI=[1.1640, 2.3374, 5.8658, 11.7414, 23.6786, 66.4250]
results_gtb_CD_PI=[1.7050, 3.3208, 8.3236, 16.6130, 33.3834, 97.7910]

import matplotlib.pyplot as plt
import numpy as np

sizes = [s * x[0] * 7 for s in scale]

# Figure for investigating the horizontal scalability | GUSToBIOSQL scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(3,2))
ax.plot(sizes, results_gtb_PI, label="PI", marker="D")
ax.plot(sizes, results_gtb_CD_PI, label="CD/PI", marker="s")
ax.set_title("GUSToBIOSQL")
ax.set_xlabel("number of input nodes")
ax.set_ylabel("time (s)")
ax.legend(loc="best", fontsize="10")
        
plt.savefig("../../outfigs/FigureHorizontalScaleGTBcompact.png")
