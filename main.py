#!/bin/env python3
from app import App
import matplotlib.pyplot as plt
import numpy as np

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
    x = [100, 200, 500, 1_000]#, 2_000, 5_000] #, 10_000, 20_000, 50_000, 100_000]
    y = [100, 200, 500]#, 1_000, 2_000, 5_000, 10_000] #, 20_000, 50_000, 100_000] 
    
    # choose which figures to use and their parameters
    from figures.personaddress import *
    from figures.flighthotel import *
    figures = [
        #FigureComparisonIndexesPersonAddress(app, prefix=prefix, values=x, nbLaunches=nbLaunches, showStats=showStats),
        #FigureComparisonAlternativeApproachesPersonAddress(app, prefix=prefix, values=y, nbLaunches=nbLaunches, showStats=showStats),
        FigureComparisonIndexesFlightHotel(app, prefix=prefix, values=x, nbLaunches=nbLaunches, showStats=showStats),
    ]
    
    # compute results    
    for fig in figures:
        fig.compute()
    
    # (optional) print results to the terminal
    if showStats:
        for fig in figures:
            fig.print_cmd()
    
    # generate the figures
    for fig in figures:
        fig.plot()
    
    # show all figures
    plt.show()

    # close connection
    app.close()
