import matplotlib.pyplot as plt
import numpy as np

## Figure for exhaustive comparison of indexes | PersonAddress scenario
x=[100, 200, 500, 1000]
# Separate indexes alternative
results_Sep_NI_RI=[415.4, 508.8, 902.0, 2381.4]
results_Sep_NI=[184.4, 163.0, 166.4, 169.8]
results_Sep_RI=[189.4, 313.6, 1144.6, 4151.0]
results_Sep=[41.6, 84.8, 476.0, 1843.6]
# Plain implementation
results_Plain_NI_RI=[151.8, 215.8, 678.6, 2268.6]
results_Plain_NI=[122.8, 130.8, 139.8, 161.8]
results_Plain_RI=[175.8, 354.0, 1611.6, 6127.8]
results_Plain=[59.0, 166.2, 980.2, 4039.0]
# Conflict Detection over Separate indexes
results_CDoverSI_NI_RI=[206.8, 235.4, 679.8, 2277.2]
results_CDoverSI_NI=[120.6, 126.8, 143.2, 165.2]
results_CDoverSI_RI=[156.2, 276.2, 1110.2, 4104.2]
results_CDoverSI=[40.8, 85.4, 467.2, 1832.6]
# Conflict Detection over Plain
results_CDoverPlain_NI_RI=[162.8, 214.0, 693.4, 2273.0]
results_CDoverPlain_NI=[119.4, 132.4, 149.2, 174.0]
results_CDoverPlain_RI=[179.8, 373.8, 1631.4, 6166.2]
results_CDoverPlain=[64.2, 168.2, 984.8, 3896.2]

# Figure for exhaustive comparison of indexes | PersonAddress scenario
fig1, axs = plt.subplots(1, 4, layout="constrained", figsize=(12,2.5)) 
fig1.suptitle("PersonAddress", fontsize=14)
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
axs[3].set_xlabel("number of node of each type")
axs[3].set_ylabel("time (ms)")
axs[3].set_yscale("log")
#axs[1, 1].legend()

labels = ["NI_RI", "NI", "RI", "WI"]
fig1.legend([l1, l2, l3, l4], labels=labels, ncol=4, loc="upper right")

axs[0].set_ylim([20, 10_000])
axs[1].set_ylim([20, 10_000])
axs[2].set_ylim([20, 10_000])
axs[3].set_ylim([20, 10_000])
plt.savefig("../../outfigs/FigureExhaustiveIndexesPersonAddresscompact.png")
