import matplotlib.pyplot as plt
import numpy as np

# results ## Figure for exhaustive comparison of indexes | DTA1 scenario
x=[100, 200, 500, 1000]
# Separate indexes implementation
results_Sep_NI_RI=[499.4, 752.0, 2447.0, 7461.8]
results_Sep_NI=[420.0, 435.4, 488.8, 573.2]
results_Sep_RI=[549.8, 998.4, 3886.4, 7780.2]
results_Sep=[153.2, 301.4, 1910.2, 7328.6]
# Plain implementation
results_Plain_NI_RI=[493.8, 761.2, 2461.4, 7472.8]
results_Plain_NI=[421.8, 443.2, 498.2, 589.2]
results_Plain_RI=[784.0, 1997.4, 8644.8, 33372.2]
results_Plain=[389.6, 1265.4, 8083.4, 32949.6]
# Conflict Detection over Separate indexes
results_CDoverSI_NI_RI=[996.2, 1271.8, 2662.4, 7982.8]
results_CDoverSI_NI=[918.0, 958.6, 1004.0, 1110.2]
results_CDoverSI_RI=[1064.6, 1496.4, 4427.6, 9910.6]
results_CDoverSI=[263.0, 562.4, 2009.8, 7423.0]
# Conflict Detection over Plain
results_CDoverPlain_NI_RI=[1026.8, 1231.6, 2566.0, 7938.0]
results_CDoverPlain_NI=[952.4, 965.4, 1018.8, 1137.8]
results_CDoverPlain_RI=[1297.4, 2468.6, 9223.8, 35177.2]
results_CDoverPlain=[489.0, 1284.6, 8607.4, 35226.0]

# Figure for exhaustive comparison of indexes | DTA1 scenario
fig1, axs = plt.subplots(1, 4, layout="constrained", figsize=(6,2), gridspec_kw = {'wspace':0, 'hspace':0}) 
fig1.suptitle("DBLPToAmalgam1                                               ", fontsize=14)
# Axes Separate indexes
l1 = axs[0].plot(x, results_Sep_NI_RI, label="NI_RI", marker="D")
l2 = axs[0].plot(x, results_Sep_NI, label="NI", marker="s")
l3 = axs[0].plot(x, results_Sep_RI, label="RI", marker="o")
l4 = axs[0].plot(x, results_Sep, label="WI", marker="x")
axs[0].set_title("SI")
axs[0].set_xlabel("nodes per type")
axs[0].set_ylabel("time (ms)")
axs[0].set_yscale("log")
#axs[0].legend(loc="best", ncol=2, fontsize="10")
# Axes Plain implementation
axs[1].plot(x, results_Plain_NI_RI, label="NI_RI", marker="D")
axs[1].plot(x, results_Plain_NI, label="NI", marker="s")
axs[1].plot(x, results_Plain_RI, label="RI", marker="o") 
axs[1].plot(x, results_Plain, label="WI", marker="x")
axs[1].set_title("PI")
axs[1].set_xlabel("nodes per type")
#axs[1].set_ylabel("time (ms)")
axs[1].set_yscale("log")
#axs[0, 1].legend()
# Axes Conflict Detection over Separate indexes
axs[2].plot(x, results_CDoverSI_NI_RI, label="NI_RI", marker="D")
axs[2].plot(x, results_CDoverSI_NI, label="NI", marker="s")
axs[2].plot(x, results_CDoverSI_RI, label="RI", marker="o") 
axs[2].plot(x, results_CDoverSI, label="WI", marker="x")
axs[2].set_title("CD/SI")
axs[2].set_xlabel("nodes per type")
#axs[2].set_ylabel("time (ms)")
axs[2].set_yscale("log")
#axs[1, 0].legend()
# Axes Conflict Detection over Plain
axs[3].plot(x, results_CDoverPlain_NI_RI, label="NI_RI", marker="D")
axs[3].plot(x, results_CDoverPlain_NI, label="NI", marker="s")
axs[3].plot(x, results_CDoverPlain_RI, label="RI", marker="o") 
axs[3].plot(x, results_CDoverPlain, label="WI", marker="x")
axs[3].set_title("CD/PI")
axs[3].set_xlabel("nodes per type")
#axs[3].set_ylabel("time (ms)")
axs[3].set_yscale("log")
#axs[1, 1].legend()

labels = ["NI_RI", "NI", "RI", "WI"]
fig1.legend([l1, l2, l3, l4], labels=labels, ncol=4, loc="upper right", bbox_to_anchor=(1, 1.03))

axs[0].set_ylim([60, 100_000])
axs[1].set_ylim([60, 100_000])
axs[2].set_ylim([60, 100_000])
axs[3].set_ylim([60, 100_000])
plt.savefig("../../outfigs/FigureExhaustiveIndexesDTA1compact.png")
