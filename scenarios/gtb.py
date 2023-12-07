import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class GUSToBIOSQL(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # csv#1
        rel_gusgene_cmd = """MERGE (n:GUSGene {
            geneID: row[1], 
            name: row[2],
            geneSymbol: row[3],
            geneCategoryID: row[4],
            reviewStatusID: row[5],
            description: row[6],
            reviewerSummary: row[7],
            sequenceOntologyID: row[8]
        })"""
        param_string = "gtb/gusgene"+str(size)+"-"+str(lstring)+".csv"
        rel_gusgene = InputRelation(os.path.join(prefix, param_string), rel_gusgene_cmd)
        # csv#2
        rel_gusgenesynonym_cmd = """MERGE (n:GUSGeneSynonym {
            geneSynonymID: row[1], 
            geneID: row[2], 
            synonymName: row[3], 
            reviewStatusID: row[4], 
            isObsolete: row[5]
        })"""
        param_string = "gtb/gusgenesynonym"+str(size)+"-"+str(lstring)+".csv"
        rel_gusgenesynonym = InputRelation(os.path.join(prefix, param_string), rel_gusgenesynonym_cmd)
        # csv#3
        rel_gusgorelationshipid_cmd = """MERGE (n:GUSGORelationship {
            goRelationshipID: row[1],
            parentTermID: row[2],
            childTermID: row[3],
            goRelationshipTypeID: row[4]
        })"""
        param_string = "gtb/gusgorelationship"+str(size)+"-"+str(lstring)+".csv"
        rel_gusgorelationshipid = InputRelation(os.path.join(prefix, param_string), rel_gusgorelationshipid_cmd)
        # csv#4
        rel_gusgosynonym_cmd = """MERGE (n:GUSGOSynonym {
            goSynonymID: row[1],
            externalDatabaseReleaseID: row[2],
            sourceID: row[3],
            goTermID: row[4],
            text: row[5]
        })"""
        param_string = "gtb/gusgosynonym"+str(size)+"-"+str(lstring)+".csv"
        rel_gusgosynonym = InputRelation(os.path.join(prefix, param_string), rel_gusgosynonym_cmd)
        # csv#5
        rel_gusgoterm_cmd = """MERGE (n:GUSGoTerm {
            goTermID: row[1],
            goID: row[2],
            externalDatabaseReleaseID: row[3],
            sourceID: row[4],
            name: row[5],
            definition: row[6],
            commentString: row[7],
            minimumLevel: row[8],
            maximumLevel: row[9],
            numberOfLevels: row[10],
            ancestorGoTermID: row[11],
            isObsolete: row[12]
        })"""
        param_string = "gtb/gusgoterm"+str(size)+"-"+str(lstring)+".csv"
        rel_gusgoterm = InputRelation(os.path.join(prefix, param_string), rel_gusgoterm_cmd)
        # csv#6
        rel_gustaxon_cmd = """MERGE (n:GUSTaxon {
            taxonID: row[1],
            ncbiTaxID: row[2],
            parentID: row[3],
            rank: row[4],
            geneticCodeID: row[5],
            mitochondrialGeneticCodeID: row[6]
        })"""
        param_string = "gtb/gustaxon"+str(size)+"-"+str(lstring)+".csv"
        rel_gustaxon = InputRelation(os.path.join(prefix, param_string), rel_gustaxon_cmd)
        # csv#7
        rel_gustaxonname_cmd = """MERGE (n:GUSTaxonName {
            taxonNameID: row[1],
            taxonID: row[2],
            name: row[3],
            uniqueNameVariant: row[4],
            nameClass: row[5]
        })"""
        param_string = "gtb/gustaxonname"+str(size)+"-"+str(lstring)+".csv"
        rel_gustaxonname = InputRelation(os.path.join(prefix, param_string), rel_gustaxonname_cmd)

        # source schema
        self.schema = InputSchema([
            rel_gusgene, 
            rel_gusgenesynonym,
            rel_gusgorelationshipid,
            rel_gusgosynonym,
            rel_gusgoterm,
            rel_gustaxon,
            rel_gustaxonname
        ])

    def run(self, app, launches=5, stats=False, nodeIndex=True, relIndex=True, shuffle=False, minmax=False):
        ttime = 0.0
        min_rtime = float("inf")
        max_rtime = 0
        for i in range(launches):
            self.prepare(app, stats=stats)
            # shuffle rules in place; if requested
            if shuffle:
                import random
                random.shuffle(self.rules)
                print(f"The rules have been shuffled.")
            # resume to the classic run procedure
            if(nodeIndex):
                self.addNodeIndexes(app, stats=stats)
            if(relIndex):
                self.addRelIndexes(app, stats=stats)
            # statistics about runtime
            rtime = self.transform(app, stats=stats)
            ttime += rtime
            if(rtime < min_rtime):
                min_rtime = rtime
            if(rtime > max_rtime):
                max_rtime = rtime
            # resume to classic run procedure
            if(nodeIndex):
                self.delNodeIndexes(app, stats=stats)
            if(relIndex):
                self.delRelIndexes(app, stats=stats)
        avg_time = ttime / launches
        if(stats):
            print(f"The transformation: {self}  averaged {avg_time} ms over {launches} run(s).")
        if(minmax):
            return (min_rtime, avg_time, max_rtime)
        else:
            return avg_time

class GUSToBIOSQLPlain(GUSToBIOSQL):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#2 using our framework
        rule2 = TransformationRule("""
        MATCH (gt:GUSTaxon)
        MERGE (x:_dummy { 
            _id: "(" + elementId(gt) + ")" 
        })
        SET x:BIOSQLTaxon,
            x.taxonID = gt.taxonID,
            x.ncbiTaxonID = gt.ncbiTaxonID,
            x.parentTaxonID = gt.parentTaxonID,
            x.nodeRank = gt.rank,
            x.geneticCode = gt.geneticCodeID,
            x.mitoGeneticCode = gt.mitochondialGeneticCodeID,
            x.leftValue = "SK1(" + gt.taxonID + ")",
            x.rightValue = "SK2(" + gt.taxonID + ")" 
        """)

        # transformation rules
        self.rules = [rule2]

    def addNodeIndexes(self, app, stats=False):
        # index on _dummy
        indexDummy = """
        CREATE INDEX idx_dummy IF NOT EXISTS
        FOR (n:_dummy)
        ON (n._id)
        """
        app.addIndex(indexDummy, stats)
    
    def delNodeIndexes(self, app, stats=False):
        # drop index on _dummy
        dropDummy = """
        DROP INDEX idx_dummy IF EXISTS
        """
        app.dropIndex(dropDummy, stats)
