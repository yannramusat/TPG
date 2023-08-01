#!/bin/env python3

from app import App
from structures import InputRelation, InputSchema, TransformationRule, Scenario

if __name__ == "__main__":
    # app setup
    scheme = "bolt"
    hostname = "localhost"
    port = "7687"
    uri = f"{scheme}://{hostname}:{port}"
    app = App(uri, "neo4j", verbose=False)

    # common prefix for CSVs; the data has been generated by iBench
    prefix = "file:///home/yann/research/ibench/build/ibench/"
    nbLaunches = 1
    showStats = True
    useIndexes = True
    x = [100, 200, 500, 1_000, 2_000]

    # execute the Optimized alternative implementation of the scenario PersonAddress
    from scenarios.person_address import PersonAddressScenarioWithIndexes
    resultsOpti = []
    for i in x:
        scenario = PersonAddressScenarioWithIndexes(prefix, size=i)
        resultsOpti.append(scenario.run(app, launches=nbLaunches, stats=showStats, index=useIndexes))

    # execute the Naive alternative implementation of the scenario PersonAddress
    from scenarios.person_address import PersonAddressScenarioNaive
    resultsNaive = []
    for i in x:
        scenario = PersonAddressScenarioNaive(prefix, size=i)
        resultsNaive.append(scenario.run(app, launches=nbLaunches, stats=showStats, index=useIndexes))

    # execute the alternative implementation with Conflict Detection of the scenario PersonAddress
    from scenarios.person_address import PersonAddressScenarioWithConflictDetection
    resultsCD = []
    for i in x:
        scenario = PersonAddressScenarioWithConflictDetection(prefix, size=i)
        resultsCD.append(scenario.run(app, launches=nbLaunches, stats=showStats, index=useIndexes))

    if showStats:
        print(resultsOpti)
        print(resultsNaive)
        print(resultsCD)

    # plot results using matplotlib
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(layout="constrained")
    ax.plot(x, resultsOpti, label="Optimized")
    ax.plot(x, resultsNaive, label="Naive")
    ax.plot(x, resultsCD, label="Conflict Detection")
    ax.set_title("$\mathtt{PersonAddress}$ Scenario")
    ax.set_xlabel("input relation sizes")
    ax.set_ylabel("time [ms]")
    ax.set_yscale("log")
    ax.legend()
    plt.show()

    # close connection
    app.close()
