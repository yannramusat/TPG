import matplotlib.pyplot as plt
import numpy as np

probs = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
## Figure for evaluating the impact of the frequency of conflicts | FlightHotel scenario
#results_FH=[830.7, 780.5, 761.1, 756.05, 747.05, 723.65, 720.25, 709.0, 697.15, 698.9, 714.7]
results_FH=[830.7, 780.5, 761.1, 756.05, 747.05, 723.65, 720.25, 709.0, 697.15, 698.9, 714.7]
## Figure for evaluating the impact of the frequency of conflicts | PersonAddress scenario
results_PA=[790.0, 784.25, 780.1, 775.75, 779.6, 775.85, 773.75, 764.6, 772.7, 779.65, 777.6]
## Figure for evaluating the impact of the frequency of conflicts | PersonData scenario
results_PD=[552.0, 546.45, 545.65, 547.45, 545.3, 547.05, 542.6, 545.85, 544.55, 543.7, 547.45]
## Figure for evaluating the impact of the frequency of conflicts | GUSToBIOSQL scenario
results_GTB=[3189.6, 3227.1, 3206.05, 3205.8, 3220.45, 3184.1, 3200.45, 3194.4, 3179.2, 3174.45, 3163.1]
## Figure for evaluating the impact of the frequency of conflicts | DBLPToAmalgam1 scenario
results_DTA1=[5395.45, 5408.05, 5417.15, 5415.35, 5383.0, 5355.6, 5360.4, 5362.65, 5350.95, 5331.65, 5283.85]
## Figure for evaluating the impact of the frequency of conflicts | Amalgam1ToAmalgam3 scenario
results_A1TA3=[5684.65, 5719.15, 5735.3, 5721.9, 5660.9, 5687.35, 5707.3, 5688.4, 5678.3, 5664.0, 5671.15]

# Figure for evaluating the impact of the frequency of conflicts
fig2, ax = plt.subplots(layout="constrained", figsize=(6,2.5))
ax.plot(probs, results_PA, label="PersonAddress", marker="D")
ax.plot(probs, results_FH, label="FlightHotel", marker="s")
ax.plot(probs, results_PD, label="PersonData", marker="o")
ax.plot(probs, results_GTB, label="GUSToBIOSQL", marker="P")
ax.plot(probs, results_DTA1, label="DBLPToAmalgam1", marker="x")
ax.plot(probs, results_A1TA3, label="Amalgam1ToAmalgam3", marker="p")
ax.set_title("Analysis of the impact of Conflict Detection")
ax.set_xlabel("likelihood of conflicts (%)")
ax.set_ylabel("time (ms)")
ax.set_yscale("log")
#ax.set_ylim([400, 1_100])
#from matplotlib.ticker import NullFormatter
#ax.yaxis.set_minor_formatter(NullFormatter())
ax.legend(loc="best", ncol=3, fontsize=9)

plt.savefig("../outfigs/FigureExhaustiveRandomAll.png")
