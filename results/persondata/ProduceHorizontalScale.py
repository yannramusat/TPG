x=[5000]        
scale=[1, 2, 5, 10, 20, 50, 100]
## Figure for investigating the horizontal scalability | PersonData scenario
results_persondata_PI=[.3218, .6070, 1.4890, 2.9886, 6.0174, 15.3956, 34.3380]
results_persondata_CD_PI=[.3468, .6522, 1.6478, 3.3416, 6.4160, 16.2660, 35.8550]

import matplotlib.pyplot as plt
import numpy as np

sizes = [s * x[0] * 3 for s in scale]

# Figure for investigating the horizontal scalability | PersonData scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(3,2))
ax.plot(sizes, results_persondata_PI, label="PI", marker="D")
ax.plot(sizes, results_persondata_CD_PI, label="CD/PI", marker="s")
ax.set_title("PersonData")
ax.set_xlabel("  total number of input nodes", loc='left')
ax.set_ylabel("time (s)")
ax.legend(loc="best", fontsize="10")
        
plt.savefig("../../outfigs/FigureHorizontalScalePDcompact.png")
