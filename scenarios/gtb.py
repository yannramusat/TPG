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
        MERGE (x:_dummy {
            _id: "(" + elementID(ggt) + ")"
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
        MERGE (x:_dummy {
            _id: "(" + elementID(gg) + ")"
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

class GUSToBIOSQLSeparateIndexes(GUSToBIOSQL):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (gtn:GUSTaxonName)
        MATCH (gt:GUSTaxon)
        WHERE gtn.taxonID = gt.taxonID
        MERGE(x:BIOSQLTaxonName {
            _id: "(" + gtn.taxonID + ")"
        })
        SET x.name = gtn.name,
            x.nameClass = gtn.nameClass
        MERGE (y:BIOSQLTaxon { 
            _id: "(" + elementId(gt) + ")" 
        })
        SET y.taxonID = gt.taxonID,
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
        MERGE (x:BIOSQLTaxon { 
            _id: "(" + elementId(gt) + ")" 
        })
        SET x.taxonID = gt.taxonID,
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
        MERGE (x:BIOSQLBioEntry { 
            _id: "(" + elementId(gg) + ")" 
        })
        SET x.bioEntryID = gg.geneID,
            x.bioDatabaseEntry = "SK3(" + gg.geneSymbol + ")",
            x.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
            x.name = gg.name,
            x.accession = gg.geneSymbol,
            x.identifier = gg.sequenceOntologyID,
            x.division = gg.geneCategoryID,
            x.description = gg.description,
            x.version = "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")" 
        MERGE (y:BIOSQLTaxon { 
            _id: "(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" 
        })
        SET y.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
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
        MERGE (x:BIOSQLTermSynonym {
            _id: "(" + elementID(ggs)  + ")"
        })
        SET x.synonym = ggs.geneSynonymID,
            x.termID = ggs.geneID
        MERGE (y:BIOSQLTerm {
            _id: "(" + elementID(gg) + ")"
        })
        SET y.termID = gg.geneID,
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
        MERGE (x:BIOSQLTerm {
            _id: "(" + elementID(ggt) = ")"
        })
        SET x.termID = ggt.goTermID,
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
        MERGE (x:BIOSQLTermSynonym {
            _id: "(" + elementID(ggs)+ ")"
        })
        SET x.synonym = ggs.goSynonymID,
            x.termID = ggs.goTermID
        MERGE (y:BIOSQLTerm {
            _id: "(" + elementID(ggt) + ")"
        })
        SET y.termID = ggt.goTermID,
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
        MERGE (x:BIOSQLTerm {
            _id: "(" + elementID(ggt1) + ")"
        })
        SET x.termID = ggt1.goTermID,
            x.name = ggt1.name,
            x.definition = ggt1.definition,
            x.identifier = ggt1.goID,
            x.isObsolete = ggt1.isObsolete,
            x.ontologyID = "SK21(" + ggt1.goTermID + ")"
        MERGE (y:BIOSQLTerm {
            _id: "(" + elementID(ggt2) + ")"
        })
        SET y.termID = ggt2.goTermID,
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
        MERGE (x:BIOSQLTerm {
            _id: "(" + elementID(gg) + ")"
        })
        SET x.termID = gg.geneID,
            x.name = gg.name,
            x.definition = gg.description,
            x.identifier = "SK13(" + gg.geneID + ")", 
            x.isObsolete = "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")",
            x.ontologyID = "SK14(" + gg.sequenceOntologyID + ")"
        """)

        # transformation rules
        self.rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8]

    def addNodeIndexes(self, app, stats=False):
        # index on BIOSQLTaxonName
        indexBIOSQLTaxonName = """
        CREATE INDEX idx_BIOSQLTaxonName IF NOT EXISTS
        FOR (n:BIOSQLTaxonName)
        ON (n._id)
        """
        app.addIndex(indexBIOSQLTaxonName, stats)

        # index on BIOSQLTaxon
        indexBIOSQLTaxon = """
        CREATE INDEX idx_BIOSQLTaxon IF NOT EXISTS
        FOR (n:BIOSQLTaxon)
        ON (n._id)
        """
        app.addIndex(indexBIOSQLTaxon, stats)
 
        # index on BIOSQLBioEntry
        indexBIOSQLBioEntry = """
        CREATE INDEX idx_BIOSQLBioEntry IF NOT EXISTS
        FOR (n:BIOSQLBioEntry)
        ON (n._id)
        """
        app.addIndex(indexBIOSQLBioEntry, stats)

        # index on BIOSQLTermSynonym
        indexBIOSQLTermSynonym = """
        CREATE INDEX idx_BIOSQLTermSynonym IF NOT EXISTS
        FOR (n:BIOSQLTermSynonym)
        ON (n._id)
        """
        app.addIndex(indexBIOSQLTermSynonym, stats)
 
        # index on BIOSQLTerm
        indexBIOSQLTerm = """
        CREATE INDEX idx_BIOSQLTerm IF NOT EXISTS
        FOR (n:BIOSQLTerm)
        ON (n._id)
        """
        app.addIndex(indexBIOSQLTerm, stats) 
    
    def delNodeIndexes(self, app, stats=False):
        # drop index on BIOSQLTaxonName
        dropBIOSQLTaxonName = """
        DROP INDEX idx_BIOSQLTaxonName IF EXISTS
        """
        app.dropIndex(dropBIOSQLTaxonName, stats)

        # drop index on BIOSQLTaxon
        dropBIOSQLTaxon = """
        DROP INDEX idx_BIOSQLTaxon IF EXISTS
        """
        app.dropIndex(dropBIOSQLTaxon, stats)

        # drop index on BIOSQLBioEntry
        dropBIOSQLBioEntry = """
        DROP INDEX idx_BIOSQLBioEntry IF EXISTS
        """
        app.dropIndex(dropBIOSQLBioEntry, stats)

        # drop index on BIOSQLTermSynonym
        dropBIOSQLTermSynonym = """
        DROP INDEX idx_BIOSQLTermSynonym IF EXISTS
        """
        app.dropIndex(dropBIOSQLTermSynonym, stats)

        # drop index on BIOSQLTerm
        dropBIOSQLTerm = """
        DROP INDEX idx_BIOSQLTerm IF EXISTS
        """
        app.dropIndex(dropBIOSQLTerm, stats)

class GUSToBIOSQLCDoverPlain(GUSToBIOSQLPlain):
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
        ON CREATE
            SET x:BIOSQLTaxonName,
                x.name = gtn.name,
                x.nameClass = gtn.nameClass
        ON MATCH
            SET x:BIOSQLTaxonName,
                x.name =
                CASE
                    WHEN x.name <> gtn.name
                        THEN "Conflict detected!"
                    ELSE
                        gtn.name
                END,
                x.nameClass =
                CASE
                    WHEN x.nameClass <> gtn.nameClass
                        THEN "Conflict detected!"
                    ELSE
                        gtn.nameClass
                END
        MERGE (y:_dummy { 
            _id: "(" + elementId(gt) + ")" 
        })
        ON CREATE
            SET y:BIOSQLTaxon,
                y.taxonID = gt.taxonID,
                y.ncbiTaxonID = gt.ncbiTaxonID,
                y.parentTaxonID = gt.parentTaxonID,
                y.nodeRank = gt.rank,
                y.geneticCode = gt.geneticCodeID,
                y.mitoGeneticCode = gt.mitochondialGeneticCodeID,
                y.leftValue = "SK1(" + gt.taxonID + ")",
                y.rightValue = "SK2(" + gt.taxonID + ")" 
        ON MATCH
            SET y:BIOSQLTaxon,
                y.taxonID =
                CASE
                    WHEN y.taxonID <> gt.taxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.taxonID
                END,
                y.ncbiTaxonID =
                CASE
                    WHEN y.ncbiTaxonID <> gt.ncbiTaxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.ncbiTaxonID
                END,
                y.parentTaxonID =
                CASE
                    WHEN y.parentTaxonID <> gt.parentTaxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.parentTaxonID
                END,
                y.nodeRank =
                CASE
                    WHEN y.nodeRank <> gt.rank
                        THEN "Conflict detected!"
                    ELSE
                        gt.rank
                END,
                y.geneticCode =
                CASE
                    WHEN y.geneticCode <> gt.geneticCodeID
                        THEN "Conflict detected!"
                    ELSE
                        gt.geneticCodeID
                END,
                y.mitoGeneticCode =
                CASE
                    WHEN y.mitoGeneticCode <> gt.mitochondrialGeneticCode
                        THEN "Conflict detected!"
                    ELSE
                        gt.mitochondrialGeneticCode
                END,
                y.leftValue =
                CASE
                    WHEN y.leftValue <> "SK1(" + gt.taxonID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK1(" + gt.taxonID + ")"
                END,
                y.rightValue =
                CASE
                    WHEN y.rightValue <> "SK2(" + gt.taxonID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK2(" + gt.taxonID + ")"
                END
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
        ON CREATE
            SET x:BIOSQLTaxon,
                x.taxonID = gt.taxonID,
                x.ncbiTaxonID = gt.ncbiTaxonID,
                x.parentTaxonID = gt.parentTaxonID,
                x.nodeRank = gt.rank,
                x.geneticCode = gt.geneticCodeID,
                x.mitoGeneticCode = gt.mitochondialGeneticCodeID,
                x.leftValue = "SK1(" + gt.taxonID + ")",
                x.rightValue = "SK2(" + gt.taxonID + ")" 
        ON MATCH
            SET x:BIOSQLTaxon,
                x.taxonID =
                CASE
                    WHEN x.taxonID <> gt.taxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.taxonID
                END,
                x.ncbiTaxonID =
                CASE
                    WHEN x.ncbiTaxonID <> gt.ncbiTaxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.ncbiTaxonID
                END,
                x.parentTaxonID =
                CASE
                    WHEN x.parentTaxonID <> gt.parentTaxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.parentTaxonID
                END,
                x.nodeRank =
                CASE
                    WHEN x.nodeRank <> gt.rank
                        THEN "Conflict detected!"
                    ELSE
                        gt.rank
                END,
                x.geneticCode =
                CASE
                    WHEN x.geneticCode <> gt.geneticCodeID
                        THEN "Conflict detected!"
                    ELSE
                        gt.geneticCodeID
                END,
                x.mitoGeneticCode =
                CASE
                    WHEN x.mitoGeneticCode <> gt.mitochondrialGeneticCode
                        THEN "Conflict detected!"
                    ELSE
                        gt.mitochondrialGeneticCode
                END,
                x.leftValue =
                CASE
                    WHEN x.leftValue <> "SK1(" + gt.taxonID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK1(" + gt.taxonID + ")"
                END,
                x.rightValue =
                CASE
                    WHEN x.rightValue <> "SK2(" + gt.taxonID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK2(" + gt.taxonID + ")"
                END
        """)
        # rule#3 using our framework
        rule3 = TransformationRule("""
        MATCH (gg:GUSGene)
        MERGE (x:_dummy { 
            _id: "(" + elementId(gg) + ")" 
        })
        ON CREATE
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
        ON MATCH
            SET x:BIOSQLBioEntry,
                x.bioEntryID =
                CASE
                    WHEN x.bioEntryID <> gg.geneID
                        THEN "Conflict detected!"
                    ELSE
                        gg.bioEntryID
                END,
                x.bioDatabaseEntry =
                CASE
                    WHEN x.bioDatabaseEntry <> "SK3(" + gg.geneSymbol + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK3(" + gg.geneSymbol + ")"
                END,
                x.taxonID =
                CASE
                    WHEN x.taxonID <> "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")"
                END,
                x.name =
                CASE
                    WHEN x.name <> gg.name
                        THEN "Conflict detected!"
                    ELSE
                        gg.name
                END,
                x.accession =
                CASE
                    WHEN x.accession <> gg.geneSymbol
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneSymbol
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> gg.sequenceOntologyID
                        THEN "Conflict detected!"
                    ELSE
                        gg.sequenceOntologyID
                END,
                x.division =
                CASE
                    WHEN x.division <> gg.geneCategoryID
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneCategoryID
                END,
                x.description =
                CASE
                    WHEN x.description <> gg.description
                        THEN "Conflict detected!"
                    ELSE
                        gg.description
                END,
                x.version =
                CASE
                    WHEN x.version <> "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")"
                END
        MERGE (y:_dummy { 
            _id: "(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" 
        })
        ON CREATE
            SET y:BIOSQLTaxon,
                y.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
                y.ncbiTaxonID = "SK6(" + gg.geneID + ")",
                y.parentTaxonID = "SK7(" + gg.geneID + ")",
                y.nodeRank = "SK8(" + gg.geneID + ")",
                y.geneticCode = "SK9(" + gg.geneID + ")",
                y.mitoGeneticCode = "SK10(" + gg.geneID + ")",
                y.leftValue = "SK11(" + gg.geneID + ")",
                y.rightValue ="SK12(" + gg.geneID + ")"
        ON MATCH
            SET y:BIOSQLTaxon,
                y.taxonID =
                CASE
                    WHEN y.taxonID <> "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")"
                END,
                y.ncbiTaxonID =
                CASE
                    WHEN y.ncbiTaxonID <> "SK6(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK6(" + gg.geneID + ")"
                END,
                y.parentTaxonID =
                CASE
                    WHEN y.parentTaxonID <> "SK7(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK7(" + gg.geneID + ")"
                END,
                y.nodeRank =
                CASE
                    WHEN y.nodeRank <> "SK8(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK8(" + gg.geneID + ")"
                END,
                y.geneticCode =
                CASE
                    WHEN y.geneticCode <> "SK9(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK9(" + gg.geneID + ")"
                END,
                y.mitoGeneticCode =
                CASE
                    WHEN y.mitoGeneticCode <> "SK10(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK10(" + gg.geneID + ")"
                END,
                y.leftValue =
                CASE
                    WHEN y.leftValue <> "SK11(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK11(" + gg.geneID + ")"
                END,
                y.rightValue =
                CASE
                    WHEN y.rightValue <> "SK12(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK12(" + gg.geneID + ")"
                END
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
        ON CREATE
            SET x:BIOSQLTermSynonym,
                x.synonym = ggs.geneSynonymID,
                x.termID = ggs.geneID
        ON MATCH
            SET x:BIOSQLTermSynonym,
                x.synonym =
                CASE
                    WHEN x.synonym <> ggs.geneSynonymID
                        THEN "Conflict detected!"
                    ELSE
                        ggs.geneSynonymID
                END,
                x.termID =
                CASE
                    WHEN x.termID <> ggs.geneID
                        THEN "Conflict detected!"
                    ELSE
                        ggs.geneID
                END
        MERGE (y:_dummy {
            _id: "(" + elementID(gg) + ")"
        })
        ON CREATE
            SET y:BIOSQLTerm,
                y.termID = gg.geneID,
                y.name = gg.name,
                y.definition = gg.description,
                y.identifier = "SK13(" + gg.geneID + ")",
                y.isObsolete = ggs.isObsolete,
                y.ontologyID = "SK15(" + gg.sequenceOntologyID + ")"
        ON MATCH
            SET y:BIOSQLTerm,
                y.termID =
                CASE
                    WHEN y.termID <> gg.geneID
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneID
                END,
                y.name =
                CASE
                    WHEN y.name <> gg.name
                        THEN "Conflict detected!"
                    ELSE
                        gg.name
                END,
                y.definition =
                CASE
                    WHEN y.definition <> gg.description
                        THEN "Conflict detected!"
                    ELSE
                        gg.description
                END,
                y.identifier =
                CASE
                    WHEN y.identifier <> "SK13(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK13(" + gg.geneID + ")"
                END,
                y.isObsolete =
                CASE
                    WHEN y.isObsolete <> ggs.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggs.isObsolete
                END,
                y.ontologyID =
                CASE
                    WHEN y.ontologyID <> "SK15(" + gg.sequenceOntologyID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + gg.sequenceOntologyID + ")"
                END
        MERGE (x)-[:HAS_SYNONYM {
            _id: "(HAS_SYNONYM:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#5 using our framework
        rule5 = TransformationRule("""
        MATCH (ggt:GUSGoTerm) 
        MERGE (x:_dummy {
            _id: "(" + elementID(ggt) + ")"
        })
        ON CREATE
            SET x:BIOSQLTerm,
                x.termID = ggt.goTermID,
                x.name = ggt.name,
                x.definition = ggt.definition,
                x.identifier = ggt.goID,
                x.isObsolete = ggt.isObsolete,
                x.ontologyID = "SK15(" + ggt.goTermID + ")"
        ON MATCH
            SET x:BIOSQLTerm,
                x.termID =
                CASE
                    WHEN x.termID <> ggt.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goTermID
                END,
                x.name =
                CASE
                    WHEN x.name <> ggt.name
                        THEN "Conflict detected!"
                    ELSE
                        ggt.name
                END,
                x.definition =
                CASE
                    WHEN x.definition <> ggt.definition
                        THEN "Conflict detected!"
                    ELSE
                        ggt.definition
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> ggt.goId
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goID
                END,
                x.isObsolete =
                CASE
                    WHEN x.isObsolete <> ggt.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggt.isObsolete
                END,
                x.ontologyID =
                CASE
                    WHEN x.ontologyID <> "SK15(" + ggt.goTermID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + ggt.goTermID + ")"
                END
        """)
        # rule#6 using our framework
        rule6 = TransformationRule("""
        MATCH (ggs:GUSGoSynonym)
        MATCH (ggt:GUSGoTerm)
        WHERE ggs.goTermID = ggt.goTermID
        MERGE (x:_dummy {
            _id: "(" + elementID(ggs)  + ")"
        })
        ON CREATE
            SET x:BIOSQLTermSynonym,
                x.synonym = ggs.goSynonymID,
                x.termID = ggs.goTermID
        ON MATCH
            SET x:BIOSQLTermSynonym,
                x.synonym =
                CASE
                    WHEN x.synonym <> ggs.goSynonymID
                        THEN "Conflict detected!"
                    ELSE
                        ggs.goSynonymID
                END,
                x.termID =
                CASE
                    WHEN x.termID <> ggs.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggs.goTermID
                END
        MERGE (y:_dummy {
            _id: "(" + elementID(ggt) + ")"
        })
        ON CREATE
            SET y:BIOSQLTerm,
                y.termID = ggt.goTermID,
                y.name = ggt.name,
                y.definition = ggt.definition,
                y.identifier = ggt.goID,
                y.isObsolete = ggt.isObsolete,
                y.ontologyID = "SK15(" + ggt.goTermID + ")"
        ON MATCH
            SET y:BIOSQLTerm,
                y.termID =
                CASE
                    WHEN y.termID <> ggt.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goTermID
                END,
                y.name =
                CASE
                    WHEN y.name <> ggt.name
                        THEN "Conflict detected!"
                    ELSE
                        ggt.name
                END,
                y.definition =
                CASE
                    WHEN y.definition <> ggt.definition
                        THEN "Conflict detected!"
                    ELSE
                        ggt.definition
                END,
                y.identifier =
                CASE
                    WHEN y.identifier <> ggt.goId
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goID
                END,
                y.isObsolete =
                CASE
                    WHEN y.isObsolete <> ggt.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggt.isObsolete
                END,
                y.ontoloyID =
                CASE
                    WHEN y.ontologyID <> "SK15(" + ggt.goTermID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + ggt.goTermID + ")"
                END
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
        ON CREATE
            SET x:BIOSQLTerm,
                x.termID = ggt1.goTermID,
                x.name = ggt1.name,
                x.definition = ggt1.definition,
                x.identifier = ggt1.goID,
                x.isObsolete = ggt1.isObsolete,
                x.ontologyID = "SK21(" + ggt1.goTermID + ")"
        ON MATCH
            SET x:BIOSQLTerm,
                x.termID =
                CASE
                    WHEN x.termID <> ggt1.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.goTermID
                END,
                x.name =
                CASE
                    WHEN x.name <> ggt1.name
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.name
                END,
                x.definition =
                CASE
                    WHEN x.definition <> ggt1.definition
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.definition
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> ggt1.goId
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.goID
                END,
                x.isObsolete =
                CASE
                    WHEN x.isObsolete <> ggt1.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.isObsolete
                END,
                x.ontoloyID =
                CASE
                    WHEN x.ontologyID <> "SK21(" + ggt1.goTermID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK21(" + ggt1.goTermID + ")"
                END
        MERGE (y:_dummy {
            _id: "(" + elementID(ggt2) + ")"
        })
        ON CREATE
            SET y:BIOSQLTerm,
                y.termID = ggt2.goTermID,
                y.name = ggt2.name,
                y.definition = ggt2.definition,
                y.identifier = ggt2.goID,
                y.isObsolete = ggt2.isObsolete,
                y.ontologyID = "SK22(" + ggt2.goTermID + ")"
        ON MATCH
            SET y:BIOSQLTerm,
                y.termID =
                CASE
                    WHEN y.termID <> ggt2.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.goTermID
                END,
                y.name =
                CASE
                    WHEN y.name <> ggt2.name
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.name
                END,
                y.definition =
                CASE
                    WHEN y.definition <> ggt2.definition
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.definition
                END,
                y.identifier =
                CASE
                    WHEN y.identifier <> ggt2.goId
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.goID
                END,
                y.isObsolete =
                CASE
                    WHEN y.isObsolete <> ggt2.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.isObsolete
                END,
                y.ontoloyID =
                CASE
                    WHEN y.ontologyID <> "SK22(" + ggt2.goTermID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK22(" + ggt2.goTermID + ")"
                END
        MERGE (x)-[z:TERM_RELATIONSHIP {
            _id: "(TERM_RELATIONSHIP:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        ON CREATE
            SET z.termRelationshipID = ggr.goRelationshipID,
                z.subjectTermID = ggr.parentTermID,
                z.predicateTermID = ggr.goRelationshipTypeID,
                z.objectTermID = ggr.childTermID,
                z.ontologyID = "SK20(" + ggr.goRelationshipID + ")"
        ON MATCH
            SET z.termRelationshipID =
                CASE
                    WHEN z.termRelationshipID <> ggr.goRelationshipID
                        THEN "Conflict detected!"
                    ELSE
                        ggr.goRelationshipID
                END,
                z.subjectTermID =
                CASE
                    WHEN z.subjectTermID <> ggr.parentTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggr.parentTermID
                END,
                z.predicateTermID =
                CASE
                    WHEN z.predicateTermID <> ggr.goRelationshipTypeID
                        THEN "Conflict detected!"
                    ELSE
                        ggr.goRelationshipTypeID
                END,
                z.objectTermID =
                CASE
                    WHEN z.objectTermID <> ggr.childTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggr.childTermID
                END,
                z.ontoloyID =
                CASE
                    WHEN z.ontologyID <> "SK20(" + ggr.goRelationshipID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK20(" + ggr.goRelationshipID + ")"
                END
        """)
        # rule#8 using our framework
        rule8 = TransformationRule("""
        MATCH (gg:GUSGene)
        MERGE (x:_dummy {
            _id: "(" + elementID(gg) + ")"
        })
        ON CREATE
            SET x:BIOSQLTerm,
                x.termID = gg.geneID,
                x.name = gg.name,
                x.definition = gg.description,
                x.identifier = "SK13(" + gg.geneID + ")", 
                x.isObsolete = "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")",
                x.ontologyID = "SK14(" + gg.sequenceOntologyID + ")"
        ON MATCH
            SET x:BIOSQLTerm,
                x.termID =
                CASE
                    WHEN x.termID <> gg.geneID
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneID
                END,
                x.name =
                CASE
                    WHEN x.name <> gg.name
                        THEN "Conflict detected!"
                    ELSE
                        gg.name
                END,
                x.definition =
                CASE
                    WHEN x.definition <> gg.description
                        THEN "Conflict detected!"
                    ELSE
                        gg.description
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> "SK13(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK13(" + gg.geneID + ")"
                END,
                x.isObsolete =
                CASE
                    WHEN x.isObsolete <> "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")"
                END,
                x.ontologyID =
                CASE
                    WHEN x.ontologyID <> "SK14(" + gg.sequenceOntologyID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK14(" + gg.sequenceOntologyID + ")"
                END
        """)

        # transformation rules
        self.rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8]

class GUSToBIOSQLCDoverSI(GUSToBIOSQLSeparateIndexes):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (gtn:GUSTaxonName)
        MATCH (gt:GUSTaxon)
        WHERE gtn.taxonID = gt.taxonID
        MERGE(x:BIOSQLTaxonName {
            _id: "(" + gtn.taxonID + ")"
        })
        ON CREATE
            SET x.name = gtn.name,
                x.nameClass = gtn.nameClass
        ON MATCH
            SET x.name =
                CASE
                    WHEN x.name <> gtn.name
                        THEN "Conflict detected!"
                    ELSE
                        gtn.name
                END,
                x.nameClass =
                CASE
                    WHEN x.nameClass <> gtn.nameClass
                        THEN "Conflict detected!"
                    ELSE
                        gtn.nameClass
                END
        MERGE (y:BIOSQLTaxon { 
            _id: "(" + elementId(gt) + ")" 
        })
        ON CREATE
            SET y.taxonID = gt.taxonID,
                y.ncbiTaxonID = gt.ncbiTaxonID,
                y.parentTaxonID = gt.parentTaxonID,
                y.nodeRank = gt.rank,
                y.geneticCode = gt.geneticCodeID,
                y.mitoGeneticCode = gt.mitochondialGeneticCodeID,
                y.leftValue = "SK1(" + gt.taxonID + ")",
                y.rightValue = "SK2(" + gt.taxonID + ")" 
        ON MATCH
            SET y.taxonID =
                CASE
                    WHEN y.taxonID <> gt.taxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.taxonID
                END,
                y.ncbiTaxonID =
                CASE
                    WHEN y.ncbiTaxonID <> gt.ncbiTaxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.ncbiTaxonID
                END,
                y.parentTaxonID =
                CASE
                    WHEN y.parentTaxonID <> gt.parentTaxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.parentTaxonID
                END,
                y.nodeRank =
                CASE
                    WHEN y.nodeRank <> gt.rank
                        THEN "Conflict detected!"
                    ELSE
                        gt.rank
                END,
                y.geneticCode =
                CASE
                    WHEN y.geneticCode <> gt.geneticCodeID
                        THEN "Conflict detected!"
                    ELSE
                        gt.geneticCodeID
                END,
                y.mitoGeneticCode =
                CASE
                    WHEN y.mitoGeneticCode <> gt.mitochondrialGeneticCode
                        THEN "Conflict detected!"
                    ELSE
                        gt.mitochondrialGeneticCode
                END,
                y.leftValue =
                CASE
                    WHEN y.leftValue <> "SK1(" + gt.taxonID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK1(" + gt.taxonID + ")"
                END,
                y.rightValue =
                CASE
                    WHEN y.rightValue <> "SK2(" + gt.taxonID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK2(" + gt.taxonID + ")"
                END
        MERGE (x)-[:TAXON_HAS_NAME {
            _id: "(TAXON_HAS_NAME:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule("""
        MATCH (gt:GUSTaxon)
        MERGE (x:BIOSQLTaxon { 
            _id: "(" + elementId(gt) + ")" 
        })
        ON CREATE
            SET x.taxonID = gt.taxonID,
                x.ncbiTaxonID = gt.ncbiTaxonID,
                x.parentTaxonID = gt.parentTaxonID,
                x.nodeRank = gt.rank,
                x.geneticCode = gt.geneticCodeID,
                x.mitoGeneticCode = gt.mitochondialGeneticCodeID,
                x.leftValue = "SK1(" + gt.taxonID + ")",
                x.rightValue = "SK2(" + gt.taxonID + ")" 
        ON MATCH
            SET x.taxonID =
                CASE
                    WHEN x.taxonID <> gt.taxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.taxonID
                END,
                x.ncbiTaxonID =
                CASE
                    WHEN x.ncbiTaxonID <> gt.ncbiTaxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.ncbiTaxonID
                END,
                x.parentTaxonID =
                CASE
                    WHEN x.parentTaxonID <> gt.parentTaxonID
                        THEN "Conflict detected!"
                    ELSE
                        gt.parentTaxonID
                END,
                x.nodeRank =
                CASE
                    WHEN x.nodeRank <> gt.rank
                        THEN "Conflict detected!"
                    ELSE
                        gt.rank
                END,
                x.geneticCode =
                CASE
                    WHEN x.geneticCode <> gt.geneticCodeID
                        THEN "Conflict detected!"
                    ELSE
                        gt.geneticCodeID
                END,
                x.mitoGeneticCode =
                CASE
                    WHEN x.mitoGeneticCode <> gt.mitochondrialGeneticCode
                        THEN "Conflict detected!"
                    ELSE
                        gt.mitochondrialGeneticCode
                END,
                x.leftValue =
                CASE
                    WHEN x.leftValue <> "SK1(" + gt.taxonID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK1(" + gt.taxonID + ")"
                END,
                x.rightValue =
                CASE
                    WHEN x.rightValue <> "SK2(" + gt.taxonID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK2(" + gt.taxonID + ")"
                END
        """)
        # rule#3 using our framework
        rule3 = TransformationRule("""
        MATCH (gg:GUSGene)
        MERGE (x:BIOSQLBioEntry { 
            _id: "(" + elementId(gg) + ")" 
        })
        ON CREATE
            SET x.bioEntryID = gg.geneID,
                x.bioDatabaseEntry = "SK3(" + gg.geneSymbol + ")",
                x.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
                x.name = gg.name,
                x.accession = gg.geneSymbol,
                x.identifier = gg.sequenceOntologyID,
                x.division = gg.geneCategoryID,
                x.description = gg.description,
                x.version = "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")" 
        ON MATCH
            SET x.bioEntryID =
                CASE
                    WHEN x.bioEntryID <> gg.geneID
                        THEN "Conflict detected!"
                    ELSE
                        gg.bioEntryID
                END,
                x.bioDatabaseEntry =
                CASE
                    WHEN x.bioDatabaseEntry <> "SK3(" + gg.geneSymbol + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK3(" + gg.geneSymbol + ")"
                END,
                x.taxonID =
                CASE
                    WHEN x.taxonID <> "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")"
                END,
                x.name =
                CASE
                    WHEN x.name <> gg.name
                        THEN "Conflict detected!"
                    ELSE
                        gg.name
                END,
                x.accession =
                CASE
                    WHEN x.accession <> gg.geneSymbol
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneSymbol
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> gg.sequenceOntologyID
                        THEN "Conflict detected!"
                    ELSE
                        gg.sequenceOntologyID
                END,
                x.division =
                CASE
                    WHEN x.division <> gg.geneCategoryID
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneCategoryID
                END,
                x.description =
                CASE
                    WHEN x.description <> gg.description
                        THEN "Conflict detected!"
                    ELSE
                        gg.description
                END,
                x.version =
                CASE
                    WHEN x.version <> "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")"
                END
        MERGE (y:BIOSQLTaxon { 
            _id: "(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" 
        })
        ON CREATE
            SET y.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
                y.ncbiTaxonID = "SK6(" + gg.geneID + ")",
                y.parentTaxonID = "SK7(" + gg.geneID + ")",
                y.nodeRank = "SK8(" + gg.geneID + ")",
                y.geneticCode = "SK9(" + gg.geneID + ")",
                y.mitoGeneticCode = "SK10(" + gg.geneID + ")",
                y.leftValue = "SK11(" + gg.geneID + ")",
                y.rightValue ="SK12(" + gg.geneID + ")"
        ON MATCH
            SET y.taxonID =
                CASE
                    WHEN y.taxonID <> "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")"
                END,
                y.ncbiTaxonID =
                CASE
                    WHEN y.ncbiTaxonID <> "SK6(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK6(" + gg.geneID + ")"
                END,
                y.parentTaxonID =
                CASE
                    WHEN y.parentTaxonID <> "SK7(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK7(" + gg.geneID + ")"
                END,
                y.nodeRank =
                CASE
                    WHEN y.nodeRank <> "SK8(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK8(" + gg.geneID + ")"
                END,
                y.geneticCode =
                CASE
                    WHEN y.geneticCode <> "SK9(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK9(" + gg.geneID + ")"
                END,
                y.mitoGeneticCode =
                CASE
                    WHEN y.mitoGeneticCode <> "SK10(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK10(" + gg.geneID + ")"
                END,
                y.leftValue =
                CASE
                    WHEN y.leftValue <> "SK11(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK11(" + gg.geneID + ")"
                END,
                y.rightValue =
                CASE
                    WHEN y.rightValue <> "SK12(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK12(" + gg.geneID + ")"
                END
        MERGE (x)-[:HAS_TAXON {
            _id: "(HAS_TAXON:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#4 using our framework
        rule4 = TransformationRule("""
        MATCH (ggs:GUSGeneSynonym)
        MATCH (gg:GUSGene)
        WHERE ggs.geneID = gg.geneID
        MERGE (x:BIOSQLTermSynonym {
            _id: "(" + elementID(ggs)  + ")"
        })
        ON CREATE
            SET x.synonym = ggs.geneSynonymID,
                x.termID = ggs.geneID
        ON MATCH
            SET x.synonym =
                CASE
                    WHEN x.synonym <> ggs.geneSynonymID
                        THEN "Conflict detected!"
                    ELSE
                        ggs.geneSynonymID
                END,
                x.termID =
                CASE
                    WHEN x.termID <> ggs.geneID
                        THEN "Conflict detected!"
                    ELSE
                        ggs.geneID
                END
        MERGE (y:BIOSQLTerm {
            _id: "(" + elementID(gg) + ")"
        })
        ON CREATE
            SET y.termID = gg.geneID,
                y.name = gg.name,
                y.definition = gg.description,
                y.identifier = "SK13(" + gg.geneID + ")",
                y.isObsolete = ggs.isObsolete,
                y.ontologyID = "SK15(" + gg.sequenceOntologyID + ")"
        ON MATCH
            SET y.termID =
                CASE
                    WHEN y.termID <> gg.geneID
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneID
                END,
                y.name =
                CASE
                    WHEN y.name <> gg.name
                        THEN "Conflict detected!"
                    ELSE
                        gg.name
                END,
                y.definition =
                CASE
                    WHEN y.definition <> gg.description
                        THEN "Conflict detected!"
                    ELSE
                        gg.description
                END,
                y.identifier =
                CASE
                    WHEN y.identifier <> "SK13(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK13(" + gg.geneID + ")"
                END,
                y.isObsolete =
                CASE
                    WHEN y.isObsolete <> ggs.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggs.isObsolete
                END,
                y.ontologyID =
                CASE
                    WHEN y.ontologyID <> "SK15(" + gg.sequenceOntologyID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + gg.sequenceOntologyID + ")"
                END
        MERGE (x)-[:HAS_SYNONYM {
            _id: "(HAS_SYNONYM:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#5 using our framework
        rule5 = TransformationRule("""
        MATCH (ggt:GUSGoTerm) 
        MERGE (x:BIOSQLTerm {
            _id: "(" + elementID(ggt) + ")"
        })
        ON CREATE
            SET x.termID = ggt.goTermID,
                x.name = ggt.name,
                x.definition = ggt.definition,
                x.identifier = ggt.goID,
                x.isObsolete = ggt.isObsolete,
                x.ontologyID = "SK15(" + ggt.goTermID + ")"
        ON MATCH
            SET x.termID =
                CASE
                    WHEN x.termID <> ggt.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goTermID
                END,
                x.name =
                CASE
                    WHEN x.name <> ggt.name
                        THEN "Conflict detected!"
                    ELSE
                        ggt.name
                END,
                x.definition =
                CASE
                    WHEN x.definition <> ggt.definition
                        THEN "Conflict detected!"
                    ELSE
                        ggt.definition
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> ggt.goId
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goID
                END,
                x.isObsolete =
                CASE
                    WHEN x.isObsolete <> ggt.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggt.isObsolete
                END,
                x.ontologyID =
                CASE
                    WHEN x.ontologyID <> "SK15(" + ggt.goTermID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + ggt.goTermID + ")"
                END
        """)
        # rule#6 using our framework
        rule6 = TransformationRule("""
        MATCH (ggs:GUSGoSynonym)
        MATCH (ggt:GUSGoTerm)
        WHERE ggs.goTermID = ggt.goTermID
        MERGE (x:BIOSQLTermSynonym {
            _id: "(" + elementID(ggs)  + ")"
        })
        ON CREATE
            SET x.synonym = ggs.goSynonymID,
                x.termID = ggs.goTermID
        ON MATCH
            SET x.synonym =
                CASE
                    WHEN x.synonym <> ggs.goSynonymID
                        THEN "Conflict detected!"
                    ELSE
                        ggs.goSynonymID
                END,
                x.termID =
                CASE
                    WHEN x.termID <> ggs.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggs.goTermID
                END
        MERGE (y:BIOSQLTerm {
            _id: "(" + elementID(ggt) + ")"
        })
        ON CREATE
            SET y.termID = ggt.goTermID,
                y.name = ggt.name,
                y.definition = ggt.definition,
                y.identifier = ggt.goID,
                y.isObsolete = ggt.isObsolete,
                y.ontologyID = "SK15(" + ggt.goTermID + ")"
        ON MATCH
            SET y.termID =
                CASE
                    WHEN y.termID <> ggt.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goTermID
                END,
                y.name =
                CASE
                    WHEN y.name <> ggt.name
                        THEN "Conflict detected!"
                    ELSE
                        ggt.name
                END,
                y.definition =
                CASE
                    WHEN y.definition <> ggt.definition
                        THEN "Conflict detected!"
                    ELSE
                        ggt.definition
                END,
                y.identifier =
                CASE
                    WHEN y.identifier <> ggt.goId
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goID
                END,
                y.isObsolete =
                CASE
                    WHEN y.isObsolete <> ggt.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggt.isObsolete
                END,
                y.ontoloyID =
                CASE
                    WHEN y.ontologyID <> "SK15(" + ggt.goTermID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + ggt.goTermID + ")"
                END
        MERGE (x)-[:HAS_SYNONYM {
            _id: "(HAS_SYNONYM:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#7 using our framework
        rule7 = TransformationRule("""
        MATCH (ggr:GUSGoRelationship)
        MATCH (ggt1:GUSGoTerm)
        MATCH (ggt2:GUSGoTerm)
        MERGE (x:BIOSQLTerm {
            _id: "(" + elementID(ggt1) + ")"
        })
        ON CREATE
            SET x.termID = ggt1.goTermID,
                x.name = ggt1.name,
                x.definition = ggt1.definition,
                x.identifier = ggt1.goID,
                x.isObsolete = ggt1.isObsolete,
                x.ontologyID = "SK21(" + ggt1.goTermID + ")"
        ON MATCH
            SET x.termID =
                CASE
                    WHEN x.termID <> ggt1.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.goTermID
                END,
                x.name =
                CASE
                    WHEN x.name <> ggt1.name
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.name
                END,
                x.definition =
                CASE
                    WHEN x.definition <> ggt1.definition
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.definition
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> ggt1.goId
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.goID
                END,
                x.isObsolete =
                CASE
                    WHEN x.isObsolete <> ggt1.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.isObsolete
                END,
                x.ontoloyID =
                CASE
                    WHEN x.ontologyID <> "SK21(" + ggt1.goTermID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK21(" + ggt1.goTermID + ")"
                END
        MERGE (y:BIOSQLTerm {
            _id: "(" + elementID(ggt2) + ")"
        })
        ON CREATE
            SET y.termID = ggt2.goTermID,
                y.name = ggt2.name,
                y.definition = ggt2.definition,
                y.identifier = ggt2.goID,
                y.isObsolete = ggt2.isObsolete,
                y.ontologyID = "SK22(" + ggt2.goTermID + ")"
        ON MATCH
            SET y.termID =
                CASE
                    WHEN y.termID <> ggt2.goTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.goTermID
                END,
                y.name =
                CASE
                    WHEN y.name <> ggt2.name
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.name
                END,
                y.definition =
                CASE
                    WHEN y.definition <> ggt2.definition
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.definition
                END,
                y.identifier =
                CASE
                    WHEN y.identifier <> ggt2.goId
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.goID
                END,
                y.isObsolete =
                CASE
                    WHEN y.isObsolete <> ggt2.isObsolete
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.isObsolete
                END,
                y.ontoloyID =
                CASE
                    WHEN y.ontologyID <> "SK22(" + ggt2.goTermID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK22(" + ggt2.goTermID + ")"
                END
        MERGE (x)-[z:TERM_RELATIONSHIP {
            _id: "(TERM_RELATIONSHIP:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        ON CREATE
            SET z.termRelationshipID = ggr.goRelationshipID,
                z.subjectTermID = ggr.parentTermID,
                z.predicateTermID = ggr.goRelationshipTypeID,
                z.objectTermID = ggr.childTermID,
                z.ontologyID = "SK20(" + ggr.goRelationshipID + ")"
        ON MATCH
            SET z.termRelationshipID =
                CASE
                    WHEN z.termRelationshipID <> ggr.goRelationshipID
                        THEN "Conflict detected!"
                    ELSE
                        ggr.goRelationshipID
                END,
                z.subjectTermID =
                CASE
                    WHEN z.subjectTermID <> ggr.parentTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggr.parentTermID
                END,
                z.predicateTermID =
                CASE
                    WHEN z.predicateTermID <> ggr.goRelationshipTypeID
                        THEN "Conflict detected!"
                    ELSE
                        ggr.goRelationshipTypeID
                END,
                z.objectTermID =
                CASE
                    WHEN z.objectTermID <> ggr.childTermID
                        THEN "Conflict detected!"
                    ELSE
                        ggr.childTermID
                END,
                z.ontoloyID =
                CASE
                    WHEN z.ontologyID <> "SK20(" + ggr.goRelationshipID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK20(" + ggr.goRelationshipID + ")"
                END
        """)
        # rule#8 using our framework
        rule8 = TransformationRule("""
        MATCH (gg:GUSGene)
        MERGE (x:BIOSQLTerm {
            _id: "(" + elementID(gg) + ")"
        })
        ON CREATE
            SET x.termID = gg.geneID,
                x.name = gg.name,
                x.definition = gg.description,
                x.identifier = "SK13(" + gg.geneID + ")", 
                x.isObsolete = "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")",
                x.ontologyID = "SK14(" + gg.sequenceOntologyID + ")"
        ON MATCH
            SET x.termID =
                CASE
                    WHEN x.termID <> gg.geneID
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneID
                END,
                x.name =
                CASE
                    WHEN x.name <> gg.name
                        THEN "Conflict detected!"
                    ELSE
                        gg.name
                END,
                x.definition =
                CASE
                    WHEN x.definition <> gg.description
                        THEN "Conflict detected!"
                    ELSE
                        gg.description
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> "SK13(" + gg.geneID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK13(" + gg.geneID + ")"
                END,
                x.isObsolete =
                CASE
                    WHEN x.isObsolete <> "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")"
                END,
                x.ontologyID =
                CASE
                    WHEN x.ontologyID <> "SK14(" + gg.sequenceOntologyID + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK14(" + gg.sequenceOntologyID + ")"
                END
        """)

        # transformation rules
        self.rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8]

class GUSToBIOSQLRandomConflicts(GUSToBIOSQLCDoverPlain):
    def __init__(self, prefix, size = 100, lstring = 5, prob_conflict = 50):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule(f"""
        MATCH (gtn:GUSTaxonName)
        MATCH (gt:GUSTaxon)
        WHERE gtn.taxonID = gt.taxonID
        MERGE(x:_dummy {{
            _id: "(" + gtn.taxonID + ")"
        }})
        ON CREATE
            SET x:BIOSQLTaxonName,
                x.name = gtn.name + "1",
                x.nameClass = gtn.nameClass + "1"
        ON MATCH
            SET x:BIOSQLTaxonName,
                x.name =
                CASE
                    WHEN x.name <> gtn.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gtn.name + "1"
                END,
                x.nameClass =
                CASE
                    WHEN x.nameClass <> gtn.nameClass + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gtn.nameClass + "1"
                END
        MERGE (y:_dummy {{ 
            _id: "(" + elementId(gt) + ")" 
        }})
        ON CREATE
            SET y:BIOSQLTaxon,
                y.taxonID = gt.taxonID + "1",
                y.ncbiTaxonID = gt.ncbiTaxonID + "1",
                y.parentTaxonID = gt.parentTaxonID + "1",
                y.nodeRank = gt.rank + "1",
                y.geneticCode = gt.geneticCodeID + "1",
                y.mitoGeneticCode = gt.mitochondialGeneticCodeID + "1",
                y.leftValue = "SK1(" + gt.taxonID + ")" + "1",
                y.rightValue = "SK2(" + gt.taxonID + ")"  + "1"
        ON MATCH
            SET y:BIOSQLTaxon,
                y.taxonID =
                CASE
                    WHEN y.taxonID <> gt.taxonID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.taxonID + "1"
                END,
                y.ncbiTaxonID =
                CASE
                    WHEN y.ncbiTaxonID <> gt.ncbiTaxonID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.ncbiTaxonID + "1"
                END,
                y.parentTaxonID =
                CASE
                    WHEN y.parentTaxonID <> gt.parentTaxonID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.parentTaxonID + "1"
                END,
                y.nodeRank =
                CASE
                    WHEN y.nodeRank <> gt.rank + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.rank + "1"
                END,
                y.geneticCode =
                CASE
                    WHEN y.geneticCode <> gt.geneticCodeID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.geneticCodeID + "1"
                END,
                y.mitoGeneticCode =
                CASE
                    WHEN y.mitoGeneticCode <> gt.mitochondrialGeneticCode + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.mitochondrialGeneticCode + "1"
                END,
                y.leftValue =
                CASE
                    WHEN y.leftValue <> "SK1(" + gt.taxonID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK1(" + gt.taxonID + ")" + "1"
                END,
                y.rightValue =
                CASE
                    WHEN y.rightValue <> "SK2(" + gt.taxonID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK2(" + gt.taxonID + ")" + "1"
                END
        MERGE (x)-[:TAXON_HAS_NAME {{
            _id: "(TAXON_HAS_NAME:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule(f"""
        MATCH (gt:GUSTaxon)
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(gt) + ")" 
        }})
        ON CREATE
            SET x:BIOSQLTaxon,
                x.taxonID = gt.taxonID + "1",
                x.ncbiTaxonID = gt.ncbiTaxonID + "1",
                x.parentTaxonID = gt.parentTaxonID + "1",
                x.nodeRank = gt.rank + "1",
                x.geneticCode = gt.geneticCodeID + "1",
                x.mitoGeneticCode = gt.mitochondialGeneticCodeID + "1",
                x.leftValue = "SK1(" + gt.taxonID + ")" + "1",
                x.rightValue = "SK2(" + gt.taxonID + ")" + "1"
        ON MATCH
            SET x:BIOSQLTaxon,
                x.taxonID =
                CASE
                    WHEN x.taxonID <> gt.taxonID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.taxonID + "1"
                END,
                x.ncbiTaxonID =
                CASE
                    WHEN x.ncbiTaxonID <> gt.ncbiTaxonID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.ncbiTaxonID + "1"
                END,
                x.parentTaxonID =
                CASE
                    WHEN x.parentTaxonID <> gt.parentTaxonID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.parentTaxonID + "1"
                END,
                x.nodeRank =
                CASE
                    WHEN x.nodeRank <> gt.rank + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.rank + "1"
                END,
                x.geneticCode =
                CASE
                    WHEN x.geneticCode <> gt.geneticCodeID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.geneticCodeID + "1"
                END,
                x.mitoGeneticCode =
                CASE
                    WHEN x.mitoGeneticCode <> gt.mitochondrialGeneticCode + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gt.mitochondrialGeneticCode + "1"
                END,
                x.leftValue =
                CASE
                    WHEN x.leftValue <> "SK1(" + gt.taxonID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK1(" + gt.taxonID + ")" + "1"
                END,
                x.rightValue =
                CASE
                    WHEN x.rightValue <> "SK2(" + gt.taxonID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK2(" + gt.taxonID + ")" + "1"
                END
        """)
        # rule#3 using our framework
        rule3 = TransformationRule(f"""
        MATCH (gg:GUSGene)
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(gg) + ")" 
        }})
        ON CREATE
            SET x:BIOSQLBioEntry,
                x.bioEntryID = gg.geneID + "1",
                x.bioDatabaseEntry = "SK3(" + gg.geneSymbol + ")" + "1",
                x.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" + "1", 
                x.name = gg.name + "1",
                x.accession = gg.geneSymbol + "1",
                x.identifier = gg.sequenceOntologyID + "1",
                x.division = gg.geneCategoryID + "1",
                x.description = gg.description + "1",
                x.version = "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")" + "1"
        ON MATCH
            SET x:BIOSQLBioEntry,
                x.bioEntryID =
                CASE
                    WHEN x.bioEntryID <> gg.geneID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.bioEntryID + "1"
                END,
                x.bioDatabaseEntry =
                CASE
                    WHEN x.bioDatabaseEntry <> "SK3(" + gg.geneSymbol + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK3(" + gg.geneSymbol + ")" + "1"
                END,
                x.taxonID =
                CASE
                    WHEN x.taxonID <> "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" + "1"
                END,
                x.name =
                CASE
                    WHEN x.name <> gg.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.name + "1"
                END,
                x.accession =
                CASE
                    WHEN x.accession <> gg.geneSymbol + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneSymbol + "1"
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> gg.sequenceOntologyID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.sequenceOntologyID + "1"
                END,
                x.division =
                CASE
                    WHEN x.division <> gg.geneCategoryID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneCategoryID + "1"
                END,
                x.description =
                CASE
                    WHEN x.description <> gg.description + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.description + "1"
                END,
                x.version =
                CASE
                    WHEN x.version <> "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")" + "1"
                END
        MERGE (y:_dummy {{ 
            _id: "(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" 
        }})
        ON CREATE
            SET y:BIOSQLTaxon,
                y.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" + "1", 
                y.ncbiTaxonID = "SK6(" + gg.geneID + ")" + "1",
                y.parentTaxonID = "SK7(" + gg.geneID + ")" + "1",
                y.nodeRank = "SK8(" + gg.geneID + ")" + "1",
                y.geneticCode = "SK9(" + gg.geneID + ")" + "1",
                y.mitoGeneticCode = "SK10(" + gg.geneID + ")" + "1",
                y.leftValue = "SK11(" + gg.geneID + ")" + "1",
                y.rightValue ="SK12(" + gg.geneID + ")" + "1"
        ON MATCH
            SET y:BIOSQLTaxon,
                y.taxonID =
                CASE
                    WHEN y.taxonID <> "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")" + "1"
                END,
                y.ncbiTaxonID =
                CASE
                    WHEN y.ncbiTaxonID <> "SK6(" + gg.geneID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK6(" + gg.geneID + ")" + "1"
                END,
                y.parentTaxonID =
                CASE
                    WHEN y.parentTaxonID <> "SK7(" + gg.geneID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK7(" + gg.geneID + ")" + "1"
                END,
                y.nodeRank =
                CASE
                    WHEN y.nodeRank <> "SK8(" + gg.geneID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK8(" + gg.geneID + ")" + "1"
                END,
                y.geneticCode =
                CASE
                    WHEN y.geneticCode <> "SK9(" + gg.geneID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK9(" + gg.geneID + ")" + "1"
                END,
                y.mitoGeneticCode =
                CASE
                    WHEN y.mitoGeneticCode <> "SK10(" + gg.geneID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK10(" + gg.geneID + ")" + "1"
                END,
                y.leftValue =
                CASE
                    WHEN y.leftValue <> "SK11(" + gg.geneID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK11(" + gg.geneID + ")" + "1"
                END,
                y.rightValue =
                CASE
                    WHEN y.rightValue <> "SK12(" + gg.geneID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK12(" + gg.geneID + ")" + "1"
                END
        MERGE (x)-[:HAS_TAXON {{
            _id: "(HAS_TAXON:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#4 using our framework
        rule4 = TransformationRule(f"""
        MATCH (ggs:GUSGeneSynonym)
        MATCH (gg:GUSGene)
        WHERE ggs.geneID = gg.geneID
        MERGE (x:_dummy {{
            _id: "(" + elementID(ggs)  + ")"
        }})
        ON CREATE
            SET x:BIOSQLTermSynonym,
                x.synonym = ggs.geneSynonymID + "1",
                x.termID = ggs.geneID + "1"
        ON MATCH
            SET x:BIOSQLTermSynonym,
                x.synonym =
                CASE
                    WHEN x.synonym <> ggs.geneSynonymID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggs.geneSynonymID + "1"
                END,
                x.termID =
                CASE
                    WHEN x.termID <> ggs.geneID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggs.geneID + "1"
                END
        MERGE (y:_dummy {{
            _id: "(" + elementID(gg) + ")"
        }})
        ON CREATE
            SET y:BIOSQLTerm,
                y.termID = gg.geneID + "1",
                y.name = gg.name + "1",
                y.definition = gg.description + "1",
                y.identifier = "SK13(" + gg.geneID + ")" + "1",
                y.isObsolete = ggs.isObsolete + "1",
                y.ontologyID = "SK15(" + gg.sequenceOntologyID + ")" + "1"
        ON MATCH
            SET y:BIOSQLTerm,
                y.termID =
                CASE
                    WHEN y.termID <> gg.geneID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneID + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> gg.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.name + "1"
                END,
                y.definition =
                CASE
                    WHEN y.definition <> gg.description + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.description + "1"
                END,
                y.identifier =
                CASE
                    WHEN y.identifier <> "SK13(" + gg.geneID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK13(" + gg.geneID + ")" + "1"
                END,
                y.isObsolete =
                CASE
                    WHEN y.isObsolete <> ggs.isObsolete + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggs.isObsolete + "1"
                END,
                y.ontologyID =
                CASE
                    WHEN y.ontologyID <> "SK15(" + gg.sequenceOntologyID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + gg.sequenceOntologyID + ")" + "1"
                END
        MERGE (x)-[:HAS_SYNONYM {{
            _id: "(HAS_SYNONYM:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#5 using our framework
        rule5 = TransformationRule(f"""
        MATCH (ggt:GUSGoTerm) 
        MERGE (x:_dummy {{
            _id: "(" + elementID(ggt) + ")"
        }})
        ON CREATE
            SET x:BIOSQLTerm,
                x.termID = ggt.goTermID + "1",
                x.name = ggt.name + "1",
                x.definition = ggt.definition + "1",
                x.identifier = ggt.goID + "1",
                x.isObsolete = ggt.isObsolete + "1",
                x.ontologyID = "SK15(" + ggt.goTermID + ")" + "1"
        ON MATCH
            SET x:BIOSQLTerm,
                x.termID =
                CASE
                    WHEN x.termID <> ggt.goTermID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goTermID + "1"
                END,
                x.name =
                CASE
                    WHEN x.name <> ggt.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.name + "1"
                END,
                x.definition =
                CASE
                    WHEN x.definition <> ggt.definition + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.definition + "1"
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> ggt.goId + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goID + "1"
                END,
                x.isObsolete =
                CASE
                    WHEN x.isObsolete <> ggt.isObsolete + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.isObsolete + "1"
                END,
                x.ontologyID =
                CASE
                    WHEN x.ontologyID <> "SK15(" + ggt.goTermID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + ggt.goTermID + ")" + "1"
                END
        """)
        # rule#6 using our framework
        rule6 = TransformationRule(f"""
        MATCH (ggs:GUSGoSynonym)
        MATCH (ggt:GUSGoTerm)
        WHERE ggs.goTermID = ggt.goTermID
        MERGE (x:_dummy {{
            _id: "(" + elementID(ggs)  + ")"
        }})
        ON CREATE
            SET x:BIOSQLTermSynonym,
                x.synonym = ggs.goSynonymID + "1",
                x.termID = ggs.goTermID + "1"
        ON MATCH
            SET x:BIOSQLTermSynonym,
                x.synonym =
                CASE
                    WHEN x.synonym <> ggs.goSynonymID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggs.goSynonymID + "1"
                END,
                x.termID =
                CASE
                    WHEN x.termID <> ggs.goTermID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggs.goTermID + "1"
                END
        MERGE (y:_dummy {{
            _id: "(" + elementID(ggt) + ")"
        }})
        ON CREATE
            SET y:BIOSQLTerm,
                y.termID = ggt.goTermID + "1",
                y.name = ggt.name + "1",
                y.definition = ggt.definition + "1",
                y.identifier = ggt.goID + "1",
                y.isObsolete = ggt.isObsolete + "1",
                y.ontologyID = "SK15(" + ggt.goTermID + ")" + "1"
        ON MATCH
            SET y:BIOSQLTerm,
                y.termID =
                CASE
                    WHEN y.termID <> ggt.goTermID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goTermID + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> ggt.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.name + "1"
                END,
                y.definition =
                CASE
                    WHEN y.definition <> ggt.definition + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.definition + "1"
                END,
                y.identifier =
                CASE
                    WHEN y.identifier <> ggt.goId + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.goID + "1"
                END,
                y.isObsolete =
                CASE
                    WHEN y.isObsolete <> ggt.isObsolete + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt.isObsolete + "1"
                END,
                y.ontoloyID =
                CASE
                    WHEN y.ontologyID <> "SK15(" + ggt.goTermID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + ggt.goTermID + ")" + "1"
                END
        MERGE (x)-[:HAS_SYNONYM {{
            _id: "(HAS_SYNONYM:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#7 using our framework
        rule7 = TransformationRule(f"""
        MATCH (ggr:GUSGoRelationship)
        MATCH (ggt1:GUSGoTerm)
        MATCH (ggt2:GUSGoTerm)
        MERGE (x:_dummy {{
            _id: "(" + elementID(ggt1) + ")"
        }})
        ON CREATE
            SET x:BIOSQLTerm,
                x.termID = ggt1.goTermID + "1",
                x.name = ggt1.name + "1",
                x.definition = ggt1.definition + "1",
                x.identifier = ggt1.goID + "1",
                x.isObsolete = ggt1.isObsolete + "1",
                x.ontologyID = "SK21(" + ggt1.goTermID + ")" + "1"
        ON MATCH
            SET x:BIOSQLTerm,
                x.termID =
                CASE
                    WHEN x.termID <> ggt1.goTermID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.goTermID + "1"
                END,
                x.name =
                CASE
                    WHEN x.name <> ggt1.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.name + "1"
                END,
                x.definition =
                CASE
                    WHEN x.definition <> ggt1.definition + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.definition + "1"
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> ggt1.goId + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.goID + "1"
                END,
                x.isObsolete =
                CASE
                    WHEN x.isObsolete <> ggt1.isObsolete + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt1.isObsolete + "1"
                END,
                x.ontoloyID =
                CASE
                    WHEN x.ontologyID <> "SK21(" + ggt1.goTermID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK21(" + ggt1.goTermID + ")" + "1"
                END
        MERGE (y:_dummy {{
            _id: "(" + elementID(ggt2) + ")"
        }})
        ON CREATE
            SET y:BIOSQLTerm,
                y.termID = ggt2.goTermID + "1",
                y.name = ggt2.name + "1",
                y.definition = ggt2.definition + "1",
                y.identifier = ggt2.goID + "1",
                y.isObsolete = ggt2.isObsolete + "1",
                y.ontologyID = "SK22(" + ggt2.goTermID + ")" + "1"
        ON MATCH
            SET y:BIOSQLTerm,
                y.termID =
                CASE
                    WHEN y.termID <> ggt2.goTermID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.goTermID + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> ggt2.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.name + "1"
                END,
                y.definition =
                CASE
                    WHEN y.definition <> ggt2.definition + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.definition + "1"
                END,
                y.identifier =
                CASE
                    WHEN y.identifier <> ggt2.goId + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.goID + "1"
                END,
                y.isObsolete =
                CASE
                    WHEN y.isObsolete <> ggt2.isObsolete + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggt2.isObsolete + "1"
                END,
                y.ontoloyID =
                CASE
                    WHEN y.ontologyID <> "SK22(" + ggt2.goTermID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK22(" + ggt2.goTermID + ")" + "1"
                END
        MERGE (x)-[z:TERM_RELATIONSHIP {{
            _id: "(TERM_RELATIONSHIP:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        ON CREATE
            SET z.termRelationshipID = ggr.goRelationshipID + "1",
                z.subjectTermID = ggr.parentTermID + "1",
                z.predicateTermID = ggr.goRelationshipTypeID + "1",
                z.objectTermID = ggr.childTermID + "1",
                z.ontologyID = "SK20(" + ggr.goRelationshipID + ")" + "1"
        ON MATCH
            SET z.termRelationshipID =
                CASE
                    WHEN z.termRelationshipID <> ggr.goRelationshipID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggr.goRelationshipID + "1"
                END,
                z.subjectTermID =
                CASE
                    WHEN z.subjectTermID <> ggr.parentTermID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggr.parentTermID + "1"
                END,
                z.predicateTermID =
                CASE
                    WHEN z.predicateTermID <> ggr.goRelationshipTypeID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggr.goRelationshipTypeID + "1"
                END,
                z.objectTermID =
                CASE
                    WHEN z.objectTermID <> ggr.childTermID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ggr.childTermID + "1"
                END,
                z.ontoloyID =
                CASE
                    WHEN z.ontologyID <> "SK20(" + ggr.goRelationshipID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK20(" + ggr.goRelationshipID + ")" + "1"
                END
        """)
        # rule#8 using our framework
        rule8 = TransformationRule(f"""
        MATCH (gg:GUSGene)
        MERGE (x:_dummy {{
            _id: "(" + elementID(gg) + ")"
        }})
        ON CREATE
            SET x:BIOSQLTerm,
                x.termID = gg.geneID + "1",
                x.name = gg.name + "1",
                x.definition = gg.description + "1",
                x.identifier = "SK13(" + gg.geneID + ")" + "1", 
                x.isObsolete = "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")" + "1",
                x.ontologyID = "SK14(" + gg.sequenceOntologyID + ")" + "1"
        ON MATCH
            SET x:BIOSQLTerm,
                x.termID =
                CASE
                    WHEN x.termID <> gg.geneID + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.geneID + "1"
                END,
                x.name =
                CASE
                    WHEN x.name <> gg.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.name + "1"
                END,
                x.definition =
                CASE
                    WHEN x.definition <> gg.description + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        gg.description + "1"
                END,
                x.identifier =
                CASE
                    WHEN x.identifier <> "SK13(" + gg.geneID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK13(" + gg.geneID + ")" + "1"
                END,
                x.isObsolete =
                CASE
                    WHEN x.isObsolete <> "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")" + "1"
                END,
                x.ontologyID =
                CASE
                    WHEN x.ontologyID <> "SK14(" + gg.sequenceOntologyID + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK14(" + gg.sequenceOntologyID + ")" + "1"
                END
        """)

        # transformation rules
        self.rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8]
    
    def run(self, app, launches=5, stats=False, nodeIndex=True, relIndex=False, shuffle=True, minmax=True):
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