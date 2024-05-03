# Manual to replicate experiments

This manual provides a guide on how to replicate the experiments on the ICIJ database.

In particular it contains the following information:
 - Instructions on how to set up a local Neo4j database with the ICIJ database in it
 - Instructions on how to install DTGraph, a Python library to automatically compile our transformation rules into openCypher scripts
 - Instructions on how to run the jupyter notebook

## Setting up a local database

Follow the ![procedure](https://github.com/GraphDatabaseExperiments/normalization_experiments/blob/main/experiments_manual/README.md) on how to set up Neo4j Desktop and import the dump files for the ICIJ data.

## Installing DTGraph

You can follow the instructions on how to install DTGraph from the ![GitHub](https://github.com/yannramusat/DTGraph/) repository of the project.

## Running the notebook

The source of the experiments on the ICIJ dataset consists of a *Jupyter notebook* (.ipynb file). 
To open this file, you will need to install **Jupyter labs**:
```
pip install jupyterlab
```

Then run:
```
jupyter lab
```

