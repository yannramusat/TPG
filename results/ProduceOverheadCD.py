import matplotlib.pyplot as plt
import numpy as np

x = [500, 1000, 2_000, 5_000, 10_000, 20_000]
## Figure for computing the overhead of performing conflict detection
results_personaddress_PI=[116.0, 139.2, 196.6, 382.4, 714.6, 1353.0]
results_personaddress_CD_PI=[129.0, 161.6, 220.4, 403.2, 726.0, 1438.2]
results_flighthotel_PI=[149.6, 186.4, 223.2, 380.4, 647.8, 1196.4]
results_flighthotel_CD_PI=[175.6, 211.8, 254.4, 428.2, 693.8, 1263.4]
results_persondata_PI=[118.4, 142.0, 170.6, 321.4, 527.8, 948.4]
results_persondata_CD_PI=[123.4, 144.4, 184.4, 356.0, 568.8, 952.0]
results_dta1_PI=[579.2, 697.0, 1094.2, 1578.8, 3161.2, 5969.0]
results_dta1_CD_PI=[1284.4, 1440.6, 1852.0, 2438.4, 4400.4, 7357.2]
results_a1ta3_PI=[552.4, 705.0, 1040.6, 2032.2, 3767.8, 7724.8]
results_a1ta3_CD_PI=[970.2, 1136.4, 1466.6, 2504.4, 4306.2, 8330.6]
results_gtb_PI=[361.8, 426.0, 613.2, 1070.8, 1933.4, 4073.8]
results_gtb_CD_PI=[648.6, 699.6, 990.6, 1504.4, 2629.2, 4806.6]

# plot results using matplotlib
import matplotlib.pyplot as plt
import numpy as np

ratioPA = [a/b for a,b in zip(results_personaddress_CD_PI, results_personaddress_PI)]
ratioFH = [a/b for a,b in zip(results_flighthotel_CD_PI, results_flighthotel_PI)]
ratioPD = [a/b for a,b in zip(results_persondata_CD_PI, results_persondata_PI)]
ratioDTA1 = [a/b for a,b in zip(results_dta1_CD_PI, results_dta1_PI)]
ratioA1TA3 = [a/b for a,b in zip(results_a1ta3_CD_PI, results_a1ta3_PI)]
ratioGTB = [a/b for a,b in zip(results_gtb_CD_PI, results_gtb_PI)]

# Figure for computing the overhead of performing conflict detection
fig2, ax = plt.subplots(layout="constrained", figsize=(6,2.5))
ax.plot(x, ratioPA, label="PersonAddress", marker="D")
ax.plot(x, ratioFH, label="FlightHotel", marker="s")
ax.plot(x, ratioPD, label="PersonData", marker="o")
ax.plot(x, ratioGTB, label="GUSToBIOSQL", marker="P")
ax.plot(x, ratioDTA1, label="DBLPToAmalgam1", marker="x")
ax.plot(x, ratioA1TA3, label="Amalgam1ToAmalgam3", marker="p")
ax.set_title("Scalability analysis of Conflict Detection")
ax.set_xlabel("number of nodes of each type")
ax.set_ylabel("ratio {CD/PI} / {PI}")
ax.legend(loc="best", fontsize="10", ncol=2)

plt.savefig("../outfigs/FigureOverheadCD.png")
