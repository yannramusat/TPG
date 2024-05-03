import matplotlib.pyplot as plt
import numpy as np

# results ## Figure for exhaustive comparison of indexes | FlightHotel scenario
x=[100, 200, 500, 1000]
# Separate indexes alternative
results_Sep_NI_RI=[141.0, 148.8, 162.0, 182.4]
results_Sep_NI=[137.4, 145.6, 154.8, 181.4]
results_Sep_RI=[131.0, 163.8, 435.8, 1297.8]
results_Sep=[29.6, 64.8, 357.8, 1389.6]
# Plain implementation
results_Plain_NI_RI=[156.2, 154.6, 175.0, 191.6]
results_Plain_NI=[126.4, 136.2, 149.4, 172.6]
results_Plain_RI=[153.8, 245.8, 822.8, 2678.4]
results_Plain=[52.8, 133.6, 1050.4, 3167.2]
# Conflict Detection over Separate indexes
results_CDoverSI_NI_RI=[160.8, 157.0, 175.6, 201.4]
results_CDoverSI_NI=[153.0, 171.2, 184.2, 231.2]
results_CDoverSI_RI=[193.4, 215.6, 544.6, 1662.8]
results_CDoverSI=[34.6, 75.0, 372.2, 1486.0]
# Conflict Detection over Plain
results_CDoverPlain_NI_RI=[165.4, 173.0, 182.0, 229.6]
results_CDoverPlain_NI=[161.6, 163.4, 179.4, 205.0]
results_CDoverPlain_RI=[171.4, 308.4, 1002.8, 3371.0]
results_CDoverPlain=[58.0, 145.4, 861.6, 3395.0]


# Figure for exhaustive comparison of indexes | FlightHotel scenario
fig1, axs = plt.subplots(1, 4, layout="constrained", figsize=(12,3)) 
fig1.suptitle("FlightHotel", fontsize=14)
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
plt.savefig("../../outfigs/FigureExhaustiveIndexesFlightHotel.png")
