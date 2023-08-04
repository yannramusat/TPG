#!/bin/env python3

from app import App

if __name__ == "__main__":
    # app setup
    scheme = "bolt"
    hostname = "localhost"
    port = "7687"
    uri = f"{scheme}://{hostname}:{port}"
    app = App(uri, "neo4j", verbose=False)

    # common prefix for CSVs; the data has been generated by iBench
    prefix = "file:///home/yann/research/ibench/build/ibench/"
    nbLaunches = 5
    showStats = True
    nodeIndexes = True
    relIndexes = True
    x = [100, 200, 500, 1_000] #, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000]
    y = [100, 200, 500, 1_000, 2_000, 5_000, 10_000] #, 20_000, 50_000, 100_000]
    
    # execute the alternative implementation of the scenario PersonAddress with Separate indexes
    from scenarios.personaddress import PersonAddressScenarioSeparateIndexes
    results_Sep_NI_RI = []
    for i in x:
        scenario = PersonAddressScenarioSeparateIndexes(prefix, size=i)
        results_Sep_NI_RI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=True))
    
    results_Sep_NI = []
    results_Sep_long = []
    for i in x:
        scenario = PersonAddressScenarioSeparateIndexes(prefix, size=i)
        results_Sep_NI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=False))
    for i in y:
        scenario = PersonAddressScenarioSeparateIndexes(prefix, size=i)
        results_Sep_long.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=False))
    
    results_Sep_RI = []
    for i in x:
        scenario = PersonAddressScenarioSeparateIndexes(prefix, size=i)
        results_Sep_RI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=True))
    
    results_Sep = []
    for i in x:
        scenario = PersonAddressScenarioSeparateIndexes(prefix, size=i)
        results_Sep.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=False))

    # execute the plain implementation of the scenario PersonAddress
    from scenarios.personaddress import PersonAddressScenarioPlain
    results_Plain_NI_RI = []
    for i in x:
        scenario = PersonAddressScenarioPlain(prefix, size=i)
        results_Plain_NI_RI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=True))
    
    results_Plain_NI = []
    results_Plain_long = []
    for i in x:
        scenario = PersonAddressScenarioPlain(prefix, size=i)
        results_Plain_NI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=False))
    for i in y:
        scenario = PersonAddressScenarioPlain(prefix, size=i)
        results_Plain_long.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=False))

    results_Plain_RI = []
    for i in x:
        scenario = PersonAddressScenarioPlain(prefix, size=i)
        results_Plain_RI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=True))
    
    results_Plain = []
    for i in x:
        scenario = PersonAddressScenarioPlain(prefix, size=i)
        results_Plain.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=False))

    # execute the alternative implementation with Conflict Detection of the scenario PersonAddress based on Separate index
    from scenarios.personaddress import PersonAddressScenarioCDoverSI
    results_CDoverSI_NI_RI = []
    for i in x:
        scenario = PersonAddressScenarioCDoverSI(prefix, size=i)
        results_CDoverSI_NI_RI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=True))
    
    results_CDoverSI_NI = []
    results_CDoverSI_long = []
    for i in x:
        scenario = PersonAddressScenarioCDoverSI(prefix, size=i)
        results_CDoverSI_NI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=False))
    for i in y:
        scenario = PersonAddressScenarioCDoverSI(prefix, size=i)
        results_CDoverSI_long.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=False))
    
    results_CDoverSI_RI = []
    for i in x:
        scenario = PersonAddressScenarioCDoverSI(prefix, size=i)
        results_CDoverSI_RI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=True))
    
    results_CDoverSI = []
    for i in x:
        scenario = PersonAddressScenarioCDoverSI(prefix, size=i)
        results_CDoverSI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=False))

    # execute the alternative implementation with Conflict Detection of the scenario PersonAddress based on Plain implementation
    from scenarios.personaddress import PersonAddressScenarioCDoverPlain
    results_CDoverPlain_NI_RI = []
    for i in x:
        scenario = PersonAddressScenarioCDoverPlain(prefix, size=i)
        results_CDoverPlain_NI_RI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=True))
    
    results_CDoverPlain_NI = []
    results_CDoverPlain_long = []
    for i in x:
        scenario = PersonAddressScenarioCDoverPlain(prefix, size=i)
        results_CDoverPlain_NI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=False))
    for i in y:
        scenario = PersonAddressScenarioCDoverPlain(prefix, size=i)
        results_CDoverPlain_long.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=False))
    
    results_CDoverPlain_RI = []
    for i in x:
        scenario = PersonAddressScenarioCDoverPlain(prefix, size=i)
        results_CDoverPlain_RI.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=True))
    
    results_CDoverPlain = []
    for i in x:
        scenario = PersonAddressScenarioCDoverPlain(prefix, size=i)
        results_CDoverPlain.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=False))

    # optional printing of the results in the console
    if showStats:
        print("Separate indexes alternative")
        print(results_Sep_NI_RI)
        print(results_Sep_NI)
        print(results_Sep_RI)
        print(results_Sep)
        print("Plain implementation")
        print(results_Plain_NI_RI)
        print(results_Plain_NI)
        print(results_Plain_RI)
        print(results_Plain)
        print("Conflict Detection over Separate indexes")
        print(results_CDoverSI_NI_RI)
        print(results_CDoverSI_NI)
        print(results_CDoverSI_RI)
        print(results_CDoverSI)
        print("Conflict Detection over Plain")
        print(results_CDoverPlain_NI_RI)
        print(results_CDoverPlain_NI)
        print(results_CDoverPlain_RI)
        print(results_CDoverPlain)

    if showStats:
        print("Comparison of alternative implementations")
        print(results_Sep_long)
        print(results_Plain_long)
        print(results_CDoverSI_long)
        print(results_CDoverPlain_long)

    # plot results using matplotlib
    import matplotlib.pyplot as plt
    import numpy as np

    # Figure for exhaustive comparison of indexes | PersonAddress scenario
    fig1, axs = plt.subplots(2, 2, layout="constrained") 
    fig1.suptitle("PersonAddress scenario", fontsize=16)
    # Axes Separate indexes
    axs[0, 0].plot(x, results_Sep_NI_RI, label="Indexes on Nodes and Relations")
    axs[0, 0].plot(x, results_Sep_NI, label="Indexes on Nodes only")
    axs[0, 0].plot(x, results_Sep_RI, label="Indexes on Relations only")
    axs[0, 0].plot(x, results_Sep, label="Without indexes")
    axs[0, 0].set_title("Separate indexes alternative implementation")
    axs[0, 0].set_xlabel("number of rows in each input relation")
    axs[0, 0].set_ylabel("time (ms)")
    axs[0, 0].set_yscale("log")
    axs[0, 0].legend()
    # Axes Plain implementation
    axs[0, 1].plot(x, results_Plain_NI_RI, label="Indexes on Nodes and Relations")
    axs[0, 1].plot(x, results_Plain_NI, label="Indexes on Nodes only")
    axs[0, 1].plot(x, results_Plain_RI, label="Indexes on Relations only") 
    axs[0, 1].plot(x, results_Plain, label="Without indexes")
    axs[0, 1].set_title("Plain implementation")
    axs[0, 1].set_xlabel("number of rows in each input relation")
    axs[0, 1].set_ylabel("time (ms)")
    axs[0, 1].set_yscale("log")
    axs[0, 1].legend()
    # Axes Conflict Detection over Separate indexes
    axs[1, 0].plot(x, results_CDoverSI_NI_RI, label="Indexes on Nodes and Relations")
    axs[1, 0].plot(x, results_CDoverSI_NI, label="Indexes on Nodes only")
    axs[1, 0].plot(x, results_CDoverSI_RI, label="Indexes on Relations only") 
    axs[1, 0].plot(x, results_CDoverSI, label="Without indexes")
    axs[1, 0].set_title("Conflict Detection over Separate indexes")
    axs[1, 0].set_xlabel("number of rows in each input relation")
    axs[1, 0].set_ylabel("time (ms)")
    axs[1, 0].set_yscale("log")
    axs[1, 0].legend()
    # Axes Conflict Detection over Plain
    axs[1, 1].plot(x, results_CDoverPlain_NI_RI, label="Indexes on Nodes and Relations")
    axs[1, 1].plot(x, results_CDoverPlain_NI, label="Indexes on Nodes only")
    axs[1, 1].plot(x, results_CDoverPlain_RI, label="Indexes on Relations only") 
    axs[1, 1].plot(x, results_CDoverPlain, label="Without indexes")
    axs[1, 1].set_title("Conflict Detection over Plain implementation")
    axs[1, 1].set_xlabel("number of rows in each input relation")
    axs[1, 1].set_ylabel("time (ms)")
    axs[1, 1].set_yscale("log")
    axs[1, 1].legend()

    # Figure for comparing alternative implementations | PersonAddress scenario
    fig2, ax = plt.subplots(layout="constrained")
    ax.plot(y, results_Sep_long, label="Separate indexes alternative")
    ax.plot(y, results_Plain_long, label="Plain implementation")
    ax.plot(y, results_CDoverSI_long, label="Conflict Detection over Separate indexes")
    ax.plot(y, results_CDoverPlain_long, label="Conflict Detection over Plain implementation")
    ax.set_title("Comparison of alternative implementations | PersonAddress scenario")
    ax.set_xlabel("number of rows in each input relation")
    ax.set_ylabel("time (ms)")
    #ax.set_yscale("log")
    ax.legend()

    # plot all figures
    plt.show()
 
    # TEMPORARY BREAK POINT
    exit()

    # execute the Optimized alternative implementation of the scenario FlightHotel
    from scenarios.flighthotel import FlightHotelScenarioSeparateIndexes
    resultsOptiFH = []
    for i in x:
        scenario = FlightHotelScenarioSeparateIndexes(prefix, size=i)
        resultsOptiFH.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=True, relIndex=True))

    # execute the Optimized alternative implementation of the scenario FlightHotel (without Indexes!)
    resultsNoIndexFH = []
    for i in x:
        scenario = FlightHotelScenarioSeparateIndexes(prefix, size=i)
        resultsNoIndexFH.append(scenario.run(app, launches=nbLaunches, stats=showStats, nodeIndex=False, relIndex=False))

    # optional printing of the results in the console
    if showStats:
        print(resultsOptiFH)
        print(resultsNoIndexFH)
   
    #####
    # TEMPORARY PLOTTING
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(layout="constrained")
    ax.plot(x, resultsOptiFH, label="Optimized")
    ax.plot(x, resultsNoIndexFH, label="Without indexes")
    ax.set_title("$\mathtt{FlightHotel}$ scenario")
    ax.set_xlabel("cardinality of input relations")
    ax.set_ylabel("time [ms]")
    ax.set_yscale("log")
    ax.legend()
    plt.show()
    #####

    # close connection
    app.close()
