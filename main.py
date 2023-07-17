#!/bin/env python3

import os
from app import App
from structures import InputRelation, InputSchema, TransformationRule, Scenario
from premade import PersonAddressScenario

if __name__ == "__main__":
    # app setup
    scheme = "bolt"
    hostname = "localhost"
    port = "7687"
    uri = f"{scheme}://{hostname}:{port}"
    app = App(uri, "neo4j", verbose=False)

    # common prefix for CSVs; the data has been generated by ibench
    prefix = "file:///home/yann/research/ibench/build/ibench/"

    # execute buit-in scenario PersonAddress
    scenario = PersonAddressScenario(prefix)
    scenario.run(app, stats = True)

    # close connection
    app.close()
