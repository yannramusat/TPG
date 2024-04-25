import matplotlib.pyplot as plt
import numpy as np

## Figure for exhaustive comparison of indexes | PersonData scenario
x=[100, 200, 500, 1000]
# Separate indexes alternative
results_Sep_NI_RI=[94.0, 111.2, 127.8, 143.2]
results_Sep_NI=[103.4, 97.0, 128.0, 152.2]
results_Sep_RI=[123.0, 165.8, 264.2, 639.8]
results_Sep=[26.4, 42.6, 209.2, 576.4]
# Plain implementation
results_Plain_NI_RI=[119.0, 126.2, 134.0, 160.6]
results_Plain_NI=[97.8, 104.2, 120.6, 133.4]
results_Plain_RI=[130.0, 191.6, 710.0, 2146.4]
results_Plain=[38.6, 97.8, 548.6, 2069.6]
# Conflict Detection over Separate indexes
results_CDoverSI_NI_RI=[108.8, 115.2, 134.0, 148.4]
results_CDoverSI_NI=[106.4, 102.4, 116.6, 144.2]
results_CDoverSI_RI=[115.4, 137.4, 304.0, 899.8]
results_CDoverSI=[24.0, 52.6, 264.0, 738.8]
# Conflict Detection over Plain
results_CDoverPlain_NI_RI=[144.4, 140.2, 159.6, 183.4]
results_CDoverPlain_NI=[121.8, 109.6, 122.2, 159.6]
results_CDoverPlain_RI=[144.8, 214.8, 773.6, 2638.6]
results_CDoverPlain=[41.0, 100.0, 573.0, 2159.2]

# Figure for exhaustive comparison of indexes | PersonData scenario
fig1, axs = plt.subplots(1, 4, layout="constrained", figsize=(12,3)) 
fig1.suptitle("PersonData", fontsize=14)
# Axes Separate indexes
l1 = axs[0].plot(x, results_Sep_NI_RI, label="NI_RI", marker="D")
l2 = axs[0].plot(x, results_Sep_NI, label="NI", marker="s")
l3 = axs[0].plot(x, results_Sep_RI, label="RI", marker="o")
l4 = axs[0].plot(x, results_Sep, label="WI", marker="x")
axs[0].set_title("SI")
axs[0].set_xlabel("number of nodes of each type")
axs[0].set_ylabel("time (ms)")
axs[0].set_yscale("log")
#axs[0].legend(loc="best", ncol=2, fontsize="10")
# Axes Plain implementation
axs[1].plot(x, results_Plain_NI_RI, label="NI_RI", marker="D")
axs[1].plot(x, results_Plain_NI, label="NI", marker="s")
axs[1].plot(x, results_Plain_RI, label="RI", marker="o") 
axs[1].plot(x, results_Plain, label="WI", marker="x")
axs[1].set_title("PI")
axs[1].set_xlabel("number of nodes of each type")
axs[1].set_ylabel("time (ms)")
axs[1].set_yscale("log")
#axs[0, 1].legend()
# Axes Conflict Detection over Separate indexes
axs[2].plot(x, results_CDoverSI_NI_RI, label="NI_RI", marker="D")
axs[2].plot(x, results_CDoverSI_NI, label="NI", marker="s")
axs[2].plot(x, results_CDoverSI_RI, label="RI", marker="o") 
axs[2].plot(x, results_CDoverSI, label="WI", marker="x")
axs[2].set_title("CD/SI")
axs[2].set_xlabel("number of nodes of each type")
axs[2].set_ylabel("time (ms)")
axs[2].set_yscale("log")
#axs[1, 0].legend()
# Axes Conflict Detection over Plain
axs[3].plot(x, results_CDoverPlain_NI_RI, label="NI_RI", marker="D")
axs[3].plot(x, results_CDoverPlain_NI, label="NI", marker="s")
axs[3].plot(x, results_CDoverPlain_RI, label="RI", marker="o") 
axs[3].plot(x, results_CDoverPlain, label="WI", marker="x")
axs[3].set_title("CD/PI")
axs[3].set_xlabel("number of nodes of each type")
axs[3].set_ylabel("time (ms)")
axs[3].set_yscale("log")
#axs[1, 1].legend()

labels = ["NI_RI", "NI", "RI", "WI"]
fig1.legend([l1, l2, l3, l4], labels=labels, ncol=4, loc="upper right")

axs[0].set_ylim([20, 10_000])
axs[1].set_ylim([20, 10_000])
axs[2].set_ylim([20, 10_000])
axs[3].set_ylim([20, 10_000])
plt.savefig("../../outfigs/FigureExhaustiveIndexesPersonData.png")
