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

    def addRelIndexes(self, app, stats=False):
        # index on taxonHasName
        indexTaxonHasName = """
        CREATE INDEX idx_taxonHasName IF NOT EXISTS
        FOR ()-[r:TAXON_HAS_NAME]-()
        ON (r._id)
        """
        app.addIndex(indexTaxonHasName, stats)
        # index on hasTaxon
        indexHasTaxon = """
        CREATE INDEX idx_hasTaxon IF NOT EXISTS
        FOR ()-[r:HAS_TAXON]-()
        ON (r._id)
        """
        app.addIndex(indexHasTaxon, stats)
        # index on hasSynonym
        indexHasSynonym = """
        CREATE INDEX idx_hasSynonym IF NOT EXISTS
        FOR ()-[r:HAS_SYNONYM]-()
        ON (r._id)
        """
        app.addIndex(indexHasSynonym, stats)
        # index on termRelationship
        indexTermRelationship = """
        CREATE INDEX idx_termRelationship IF NOT EXISTS
        FOR ()-[r:TERM_RELATIONSHIP]-()
        ON (r._id)
        """
        app.addIndex(indexTermRelationship, stats)
    
    def delRelIndexes(self, app, stats=False):
        # drop index on taxonHasName
        dropTaxonHasName = """
        DROP INDEX idx_taxonHasName IF EXISTS
        """
        app.dropIndex(dropTaxonHasName, stats)
        # drop index on hasTaxon
        dropHasTaxon = """
        DROP INDEX idx_hasTaxon IF EXISTS
        """
        app.dropIndex(dropHasTaxon, stats)
        # drop index on hasSynonym
        dropHasSynonym = """
        DROP INDEX idx_hasSynonym IF EXISTS
        """
        app.dropIndex(dropHasSynonym, stats)
        # drop index on termRelationship
        dropTermRelationship = """
        DROP INDEX idx_termRelationship IF EXISTS
        """
        app.dropIndex(dropTermRelationship, stats)

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

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (gtn:GUSTaxonName)
        MATCH (gt:GUSTaxon)
        WHERE gtn.taxonID = gt.taxonID
        MERGE(x:_dummy {
            _id: "(" + gtn.taxonID + ")"
        })
        SET x:BIOSQLTaxonName,
            x.name = gtn.name,
            x.nameClass = gtn.nameClass
        MERGE (y:_dummy { 
            _id: "(" + elementId(gt) + ")" 
        })
        SET y:BIOSQLTaxon,
            y.taxonID = gt.taxonID,
            y.ncbiTaxonID = gt.ncbiTaxonID,
            y.parentTaxonID = gt.parentTaxonID,
            y.nodeRank = gt.rank,
            y.geneticCode = gt.geneticCodeID,
            y.mitoGeneticCode = gt.mitochondialGeneticCodeID,
            y.leftValue = "SK1(" + gt.taxonID + ")",
            y.rightValue = "SK2(" + gt.taxonID + ")" 
        MERGE (x)-[:TAXON_HAS_NAME {
            _id: "(TAXON_HAS_NAME:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
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
        # rule#3 using our framework
        rule3 = TransformationRule("""
        MATCH (gg:GUSGene)
        MERGE (x:_dummy { 
            _id: "(" + elementId(gg) + ")" 
        })
        SET x:BIOSQLBioEntry,
            x.bioEntryID = gg.geneID,
            x.bioDatabaseEntry = "SK3(" + gg.geneSymbol + ")",
            x.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
            x.name = gg.name,
            x.accession = gg.geneSymbol,
            x.identifier = gg.sequenceOntologyID,
            x.division = gg.geneCategoryID,
            x.description = gg.description,
            x.version = "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")" 
        MERGE (y:_dummy { 
            _id: "(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" 
        })
        SET y:BIOSQLTaxon,
            y.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
            y.ncbiTaxonID = "SK6(" + gg.geneID + ")",
            y.parentTaxonID = "SK7(" + gg.geneID + ")",
            y.nodeRank = "SK8(" + gg.geneID + ")",
            y.geneticCode = "SK9(" + gg.geneID + ")",
            y.mitoGeneticCode = "SK10(" + gg.geneID + ")",
            y.leftValue = "SK11(" + gg.geneID + ")",
            y.rightValue ="SK12(" + gg.geneID + ")"
        MERGE (x)-[:HAS_TAXON {
            _id: "(HAS_TAXON:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#4 using our framework
        rule4 = TransformationRule("""
        MATCH (ggs:GUSGeneSynonym)
        MATCH (gg:GUSGene)
        WHERE ggs.geneID = gg.geneID
        MERGE (x:_dummy {
            _id: "(" + elementID(ggs)  + ")"
        })
        SET x:BIOSQLTermSynonym,
            x.synonym = ggs.geneSynonymID,
            x.termID = ggs.geneID
        MERGE (y:_dummy {
            _id: "(" + elementID(gg) + ")"
        })
        SET y:BIOSQLTerm,
            y.termID = gg.geneID,
            y.name = gg.name,
            y.definition = gg.description,
            y.identifier = "SK13(" + gg.geneID + ")",
            y.isObsolete = ggs.isObsolete,
            y.ontologyID = "SK15(" + gg.sequenceOntologyID + ")"
        MERGE (x)-[:HAS_SYNONYM {
            _id: "(HAS_SYNONYM:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#5 using our framework
        rule5 = TransformationRule("""
        MATCH (ggt:GUSGoTerm) 
        MATCH (x:_dummy {
            _id: elementID(ggt) 
        })
        SET x:BIOSQLTerm,
            x.termID = ggt.goTermID,
            x.name = ggt.name,
            x.definition = ggt.definition,
            x.identifier = ggt.goID,
            x.isObsolete = ggt.isObsolete,
            x.ontologyID = "SK15(" + ggt.goTermID + ")"
        """)
        # rule#6 using our framework
        rule6 = TransformationRule("""
        MATCH (ggs:GUSGoSynonym)
        MATCH (ggt:GUSGoTerm)
        WHERE ggs.goTermID = ggt.goTermID
        MERGE (x:_dummy {
            _id: "(" + elementID(ggs)  + ")"
        })
        SET x:BIOSQLTermSynonym,
            x.synonym = ggs.goSynonymID,
            x.termID = ggs.goTermID
        MERGE (y:_dummy {
            _id: "(" + elementID(ggt) + ")"
        })
        SET y:BIOSQLTerm,
            y.termID = ggt.goTermID,
            y.name = ggt.name,
            y.definition = ggt.definition,
            y.identifier = ggt.goID,
            y.isObsolete = ggt.isObsolete,
            y.ontologyID = "SK15(" + ggt.goTermID + ")"
        MERGE (x)-[:HAS_SYNONYM {
            _id: "(HAS_SYNONYM:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#7 using our framework
        rule7 = TransformationRule("""
        MATCH (ggr:GUSGoRelationship)
        MATCH (ggt1:GUSGoTerm)
        MATCH (ggt2:GUSGoTerm)
        MERGE (x:_dummy {
            _id: "(" + elementID(ggt1) + ")"
        })
        SET x:BIOSQLTerm,
            x.termID = ggt1.goTermID,
            x.name = ggt1.name,
            x.definition = ggt1.definition,
            x.identifier = ggt1.goID,
            x.isObsolete = ggt1.isObsolete,
            x.ontologyID = "SK21(" + ggt1.goTermID + ")"
        MERGE (y:_dummy {
            _id: "(" + elementID(ggt2) + ")"
        })
        SET y:BIOSQLTerm,
            y.termID = ggt2.goTermID,
            y.name = ggt2.name,
            y.definition = ggt2.definition,
            y.identifier = ggt2.goID,
            y.isObsolete = ggt2.isObsolete,
            y.ontologyID = "SK22(" + ggt2.goTermID + ")"
        MERGE (x)-[z:TERM_RELATIONSHIP {
            _id: "(TERM_RELATIONSHIP:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        SET z.termRelationshipID = ggr.goRelationshipID,
            z.subjectTermID = ggr.parentTermID,
            z.predicateTermID = ggr.goRelationshipTypeID,
            z.objectTermID = ggr.childTermID,
            z.ontologyID = "SK20(" + ggr.goRelationshipID + ")" 
        """)
        # rule#8 using our framework
        rule8 = TransformationRule("""
        MATCH (gg:GUSGene)
        MATCH (x:_dummy {
            _id: elementID(gg) 
        })
        SET x:BIOSQLTerm,
            x.termID = gg.geneID,
            x.name = gg.name,
            x.definition = gg.description,
            x.identifier = "SK13(" + gg.geneID + ")", 
            x.isObsolete = "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")",
            x.ontologyID = "SK14(" + gg.sequenceOntologyID + ")"
        """)

        # transformation rules
        self.rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8]

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
