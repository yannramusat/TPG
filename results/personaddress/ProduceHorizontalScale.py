x=[5000]        
scale=[1, 2, 5, 10, 20, 50, 100]
## Figure for investigating the horizontal scalability | PersonAddress Scenario
results_personaddress_PI=[414.2, 810.8, 2007.0, 4018.0, 8060.8, 20503.2, 42427.0]
results_personaddress_CD_PI=[432.4, 862.8, 2160.0, 4318.0, 8636.2, 22043.0, 46647.0]

import matplotlib.pyplot as plt
import numpy as np

sizes = [s * x[0] * 2 for s in scale]

# Figure for investigating the horizontal scalability | PersonAddress scenario
fig2, ax = plt.subplots(layout="constrained", figsize=(6,3))
ax.plot(sizes, results_personaddress_PI, label="PI", marker="D")
ax.plot(sizes, results_personaddress_CD_PI, label="CD/PI", marker="s")
ax.set_title("PersonAddress")
ax.set_xlabel("total number of input nodes")
ax.set_ylabel("time (ms)")
ax.legend(loc="best", fontsize="10")
        
plt.savefig("../../outfigs/FigureHorizontalScalePA.png")
