import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class GUSToBIOSQLScale(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        relations = []
        # csv#1
        for i in range(scale):
            rel_gusgene_cmd = f"""MERGE (n:GUSGene{i} {{
                geneID: row[1], 
                name: row[2],
                geneSymbol: row[3],
                geneCategoryID: row[4],
                reviewStatusID: row[5],
                description: row[6],
                reviewerSummary: row[7],
                sequenceOntologyID: row[8]
            }})"""
            param_string = "gtb/gusgene"+str(size)+"-"+str(lstring)+".csv"
            rel_gusgene = InputRelation(os.path.join(prefix, param_string), rel_gusgene_cmd)
            relations.append(rel_gusgene)
        # csv#2
        for i in range(scale):
            rel_gusgenesynonym_cmd = f"""MERGE (n:GUSGeneSynonym{i} {{
                geneSynonymID: row[1], 
                geneID: row[2], 
                synonymName: row[3], 
                reviewStatusID: row[4], 
                isObsolete: row[5]
            }})"""
            param_string = "gtb/gusgenesynonym"+str(size)+"-"+str(lstring)+".csv"
            rel_gusgenesynonym = InputRelation(os.path.join(prefix, param_string), rel_gusgenesynonym_cmd)
            relations.append(rel_gusgenesynonym)
        # csv#3
        for i in range(scale):
            rel_gusgorelationshipid_cmd = f"""MERGE (n:GUSGORelationship{i} {{
                goRelationshipID: row[1],
                parentTermID: row[2],
                childTermID: row[3],
                goRelationshipTypeID: row[4]
            }})"""
            param_string = "gtb/gusgorelationship"+str(size)+"-"+str(lstring)+".csv"
            rel_gusgorelationshipid = InputRelation(os.path.join(prefix, param_string), rel_gusgorelationshipid_cmd)
            relations.append(rel_gusgorelationshipid)
        # csv#4
        for i in range(scale):
            rel_gusgosynonym_cmd = f"""MERGE (n:GUSGOSynonym{i} {{
                goSynonymID: row[1],
                externalDatabaseReleaseID: row[2],
                sourceID: row[3],
                goTermID: row[4],
                text: row[5]
            }})"""
            param_string = "gtb/gusgosynonym"+str(size)+"-"+str(lstring)+".csv"
            rel_gusgosynonym = InputRelation(os.path.join(prefix, param_string), rel_gusgosynonym_cmd)
            relations.append(rel_gusgosynonym)
        # csv#5
        for i in range(scale):
            rel_gusgoterm_cmd = f"""MERGE (n:GUSGoTerm{i} {{
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
            }})"""
            param_string = "gtb/gusgoterm"+str(size)+"-"+str(lstring)+".csv"
            rel_gusgoterm = InputRelation(os.path.join(prefix, param_string), rel_gusgoterm_cmd)
            relations.append(rel_gusgoterm)
        # csv#6
        for i in range(scale):
            rel_gustaxon_cmd = f"""MERGE (n:GUSTaxon{i} {{
                taxonID: row[1],
                ncbiTaxID: row[2],
                parentID: row[3],
                rank: row[4],
                geneticCodeID: row[5],
                mitochondrialGeneticCodeID: row[6]
            }})"""
            param_string = "gtb/gustaxon"+str(size)+"-"+str(lstring)+".csv"
            rel_gustaxon = InputRelation(os.path.join(prefix, param_string), rel_gustaxon_cmd)
            relations.append(rel_gustaxon)
        # csv#7
        for i in range(scale):
            rel_gustaxonname_cmd = f"""MERGE (n:GUSTaxonName{i} {{
                taxonNameID: row[1],
                taxonID: row[2],
                name: row[3],
                uniqueNameVariant: row[4],
                nameClass: row[5]
            }})"""
            param_string = "gtb/gustaxonname"+str(size)+"-"+str(lstring)+".csv"
            rel_gustaxonname = InputRelation(os.path.join(prefix, param_string), rel_gustaxonname_cmd)
            relations.append(rel_gustaxonname)
        # source schema
        self.schema = InputSchema(relations)

class GUSToBIOSQLPlainScale(GUSToBIOSQLScale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (gtn:GUSTaxonName{i})
            MATCH (gt:GUSTaxon{i})
            WHERE gtn.taxonID = gt.taxonID
            MERGE(x:_dummy {{
                _id: "(" + gtn.taxonID + ",{i})"
            }})
            SET x:BIOSQLTaxonName{i},
                x.name = gtn.name,
                x.nameClass = gtn.nameClass
            MERGE (y:_dummy {{ 
                _id: "(" + elementId(gt) + ",{i})" 
            }})
            SET y:BIOSQLTaxon{i},
                y.taxonID = gt.taxonID,
                y.ncbiTaxonID = gt.ncbiTaxonID,
                y.parentTaxonID = gt.parentTaxonID,
                y.nodeRank = gt.rank,
                y.geneticCode = gt.geneticCodeID,
                y.mitoGeneticCode = gt.mitochondialGeneticCodeID,
                y.leftValue = "SK1(" + gt.taxonID + ")",
                y.rightValue = "SK2(" + gt.taxonID + ")" 
            MERGE (x)-[:TAXON_HAS_NAME{i} {{
                _id: "(TAXON_HAS_NAME{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule1)
        # rule#2 using our framework
        for i in range(scale):
            rule2 = TransformationRule(f"""
            MATCH (gt:GUSTaxon{i})
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(gt) + ",{i})"
            }})
            SET x:BIOSQLTaxon{i},
                x.taxonID = gt.taxonID,
                x.ncbiTaxonID = gt.ncbiTaxonID,
                x.parentTaxonID = gt.parentTaxonID,
                x.nodeRank = gt.rank,
                x.geneticCode = gt.geneticCodeID,
                x.mitoGeneticCode = gt.mitochondialGeneticCodeID,
                x.leftValue = "SK1(" + gt.taxonID + ")",
                x.rightValue = "SK2(" + gt.taxonID + ")" 
            """)
            rules.append(rule2)
        # rule#3 using our framework
        for i in range(scale):
            rule3 = TransformationRule(f"""
            MATCH (gg:GUSGene{i})
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(gg) + ",{i})"
            }})
            SET x:BIOSQLBioEntry{i},
                x.bioEntryID = gg.geneID,
                x.bioDatabaseEntry = "SK3(" + gg.geneSymbol + ")",
                x.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
                x.name = gg.name,
                x.accession = gg.geneSymbol,
                x.identifier = gg.sequenceOntologyID,
                x.division = gg.geneCategoryID,
                x.description = gg.description,
                x.version = "SK5(" + gg.geneID + "," + gg.reviewStatusID + ")" 
            MERGE (y:_dummy {{ 
                _id: "(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ",{i})" 
            }})
            SET y:BIOSQLTaxon{i},
                y.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
                y.ncbiTaxonID = "SK6(" + gg.geneID + ")",
                y.parentTaxonID = "SK7(" + gg.geneID + ")",
                y.nodeRank = "SK8(" + gg.geneID + ")",
                y.geneticCode = "SK9(" + gg.geneID + ")",
                y.mitoGeneticCode = "SK10(" + gg.geneID + ")",
                y.leftValue = "SK11(" + gg.geneID + ")",
                y.rightValue ="SK12(" + gg.geneID + ")"
            MERGE (x)-[:HAS_TAXON{i} {{
                _id: "(HAS_TAXON{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule3)
        # rule#4 using our framework
        for i in range(scale):
            rule4 = TransformationRule(f"""
            MATCH (ggs:GUSGeneSynonym{i})
            MATCH (gg:GUSGene{i})
            WHERE ggs.geneID = gg.geneID
            MERGE (x:_dummy {{
                _id: "(" + elementID(ggs)  + ",{i})"
            }})
            SET x:BIOSQLTermSynonym{i},
                x.synonym = ggs.geneSynonymID,
                x.termID = ggs.geneID
            MERGE (y:_dummy {{
                _id: "(" + elementID(gg) + ",{i})"
            }})
            SET y:BIOSQLTerm{i},
                y.termID = gg.geneID,
                y.name = gg.name,
                y.definition = gg.description,
                y.identifier = "SK13(" + gg.geneID + ")",
                y.isObsolete = ggs.isObsolete,
                y.ontologyID = "SK15(" + gg.sequenceOntologyID + ")"
            MERGE (x)-[:HAS_SYNONYM{i} {{
                _id: "(HAS_SYNONYM{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule4)
        # rule#5 using our framework
        for i in range(scale):
            rule5 = TransformationRule(f"""
            MATCH (ggt:GUSGoTerm{i}) 
            MERGE (x:_dummy {{
                _id: "(" + elementID(ggt) + ",{i})"
            }})
            SET x:BIOSQLTerm,
                x.termID = ggt.goTermID,
                x.name = ggt.name,
                x.definition = ggt.definition,
                x.identifier = ggt.goID,
                x.isObsolete = ggt.isObsolete,
                x.ontologyID = "SK15(" + ggt.goTermID + ",{i})"
            """)
            rules.append(rule5)
        # rule#6 using our framework
        for i in range(scale):
            rule6 = TransformationRule(f"""
            MATCH (ggs:GUSGoSynonym{i})
            MATCH (ggt:GUSGoTerm{i})
            WHERE ggs.goTermID = ggt.goTermID
            MERGE (x:_dummy {{
                _id: "(" + elementID(ggs)  + ",{i})"
            }})
            SET x:BIOSQLTermSynonym{i},
                x.synonym = ggs.goSynonymID,
                x.termID = ggs.goTermID
            MERGE (y:_dummy {{
                _id: "(" + elementID(ggt) + ",{i})"
            }})
            SET y:BIOSQLTerm{i},
                y.termID = ggt.goTermID,
                y.name = ggt.name,
                y.definition = ggt.definition,
                y.identifier = ggt.goID,
                y.isObsolete = ggt.isObsolete,
                y.ontologyID = "SK15(" + ggt.goTermID + ")"
            MERGE (x)-[:HAS_SYNONYM{i} {{
                _id: "(HAS_SYNONYM{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule6)
        # rule#7 using our framework
        for i in range(scale):
            rule7 = TransformationRule(f"""
            MATCH (ggr:GUSGoRelationship{i})
            MATCH (ggt1:GUSGoTerm{i})
            MATCH (ggt2:GUSGoTerm{i})
            MERGE (x:_dummy {{
                _id: "(" + elementID(ggt1) + ",{i})"
            }})
            SET x:BIOSQLTerm{i},
                x.termID = ggt1.goTermID,
                x.name = ggt1.name,
                x.definition = ggt1.definition,
                x.identifier = ggt1.goID,
                x.isObsolete = ggt1.isObsolete,
                x.ontologyID = "SK21(" + ggt1.goTermID + ")"
            MERGE (y:_dummy {{
                _id: "(" + elementID(ggt2) + ",{i})"
            }})
            SET y:BIOSQLTerm{i},
                y.termID = ggt2.goTermID,
                y.name = ggt2.name,
                y.definition = ggt2.definition,
                y.identifier = ggt2.goID,
                y.isObsolete = ggt2.isObsolete,
                y.ontologyID = "SK22(" + ggt2.goTermID + ")"
            MERGE (x)-[z:TERM_RELATIONSHIP{i} {{
                _id: "(TERM_RELATIONSHIP{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            SET z.termRelationshipID = ggr.goRelationshipID,
                z.subjectTermID = ggr.parentTermID,
                z.predicateTermID = ggr.goRelationshipTypeID,
                z.objectTermID = ggr.childTermID,
                z.ontologyID = "SK20(" + ggr.goRelationshipID + ")"
            """)
            rules.append(rule7)
        # rule#8 using our framework
        for i in range(scale):
            rule8 = TransformationRule(f"""
            MATCH (gg:GUSGene{i})
            MERGE (x:_dummy {{
                _id: "(" + elementID(gg) + ",{i})"
            }})
            SET x:BIOSQLTerm{i},
                x.termID = gg.geneID,
                x.name = gg.name,
                x.definition = gg.description,
                x.identifier = "SK13(" + gg.geneID + ")", 
                x.isObsolete = "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")",
                x.ontologyID = "SK14(" + gg.sequenceOntologyID + ")"
            """)
            rules.append(rule8)
        # transformation rules
        self.rules = rules

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

class GUSToBIOSQLCDoverPlainScale(GUSToBIOSQLPlainScale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (gtn:GUSTaxonName{i})
            MATCH (gt:GUSTaxon{i})
            WHERE gtn.taxonID = gt.taxonID
            MERGE(x:_dummy {{
                _id: "(" + gtn.taxonID + ",{i})"
            }})
            ON CREATE
                SET x:BIOSQLTaxonName{i},
                    x.name = gtn.name,
                    x.nameClass = gtn.nameClass
            ON MATCH
                SET x:BIOSQLTaxonName{i},
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
            MERGE (y:_dummy {{ 
                _id: "(" + elementId(gt) + ",{i})"
            }})
            ON CREATE
                SET y:BIOSQLTaxon{i},
                    y.taxonID = gt.taxonID,
                    y.ncbiTaxonID = gt.ncbiTaxonID,
                    y.parentTaxonID = gt.parentTaxonID,
                    y.nodeRank = gt.rank,
                    y.geneticCode = gt.geneticCodeID,
                    y.mitoGeneticCode = gt.mitochondialGeneticCodeID,
                    y.leftValue = "SK1(" + gt.taxonID + ")",
                    y.rightValue = "SK2(" + gt.taxonID + ")" 
            ON MATCH
                SET y:BIOSQLTaxon{i},
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
            MERGE (x)-[:TAXON_HAS_NAME{i} {{
                _id: "(TAXON_HAS_NAME{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule1)
        # rule#2 using our framework
        for i in range(scale):
            rule2 = TransformationRule(f"""
            MATCH (gt:GUSTaxon{i})
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(gt) + ",{i})"
            }})
            ON CREATE
                SET x:BIOSQLTaxon{i},
                    x.taxonID = gt.taxonID,
                    x.ncbiTaxonID = gt.ncbiTaxonID,
                    x.parentTaxonID = gt.parentTaxonID,
                    x.nodeRank = gt.rank,
                    x.geneticCode = gt.geneticCodeID,
                    x.mitoGeneticCode = gt.mitochondialGeneticCodeID,
                    x.leftValue = "SK1(" + gt.taxonID + ")",
                    x.rightValue = "SK2(" + gt.taxonID + ")" 
            ON MATCH
                SET x:BIOSQLTaxon{i},
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
            rules.append(rule2)
        # rule#3 using our framework
        for i in range(scale):
            rule3 = TransformationRule(f"""
            MATCH (gg:GUSGene{i})
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(gg) + ",{i})" 
            }})
            ON CREATE
                SET x:BIOSQLBioEntry{i},
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
                SET x:BIOSQLBioEntry{i},
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
            MERGE (y:_dummy {{ 
                _id: "(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ",{i})"
            }})
            ON CREATE
                SET y:BIOSQLTaxon{i},
                    y.taxonID = "SK4(" + gg.geneID + "," + gg.geneSymbol + "," + gg.geneCategoryID + ")", 
                    y.ncbiTaxonID = "SK6(" + gg.geneID + ")",
                    y.parentTaxonID = "SK7(" + gg.geneID + ")",
                    y.nodeRank = "SK8(" + gg.geneID + ")",
                    y.geneticCode = "SK9(" + gg.geneID + ")",
                    y.mitoGeneticCode = "SK10(" + gg.geneID + ")",
                    y.leftValue = "SK11(" + gg.geneID + ")",
                    y.rightValue ="SK12(" + gg.geneID + ")"
            ON MATCH
                SET y:BIOSQLTaxon{i},
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
            MERGE (x)-[:HAS_TAXON{i} {{
                _id: "(HAS_TAXON{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule3)
        # rule#4 using our framework
        for i in range(scale):
            rule4 = TransformationRule(f"""
            MATCH (ggs:GUSGeneSynonym{i})
            MATCH (gg:GUSGene{i})
            WHERE ggs.geneID = gg.geneID
            MERGE (x:_dummy {{
                _id: "(" + elementID(ggs) + ",{i})"
            }})
            ON CREATE
                SET x:BIOSQLTermSynonym{i},
                    x.synonym = ggs.geneSynonymID,
                    x.termID = ggs.geneID
            ON MATCH
                SET x:BIOSQLTermSynonym{i},
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
            MERGE (y:_dummy {{
                _id: "(" + elementID(gg) + ",{i})"
            }})
            ON CREATE
                SET y:BIOSQLTerm{i},
                    y.termID = gg.geneID,
                    y.name = gg.name,
                    y.definition = gg.description,
                    y.identifier = "SK13(" + gg.geneID + ")",
                    y.isObsolete = ggs.isObsolete,
                    y.ontologyID = "SK15(" + gg.sequenceOntologyID + ")"
            ON MATCH
                SET y:BIOSQLTerm{i},
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
            MERGE (x)-[:HAS_SYNONYM{i} {{
                _id: "(HAS_SYNONYM{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule4)
        # rule#5 using our framework
        for i in range(scale):
            rule5 = TransformationRule(f"""
            MATCH (ggt:GUSGoTerm{i}) 
            MERGE (x:_dummy {{
                _id: "(" + elementID(ggt) + ",{i})"
            }})
            ON CREATE
                SET x:BIOSQLTerm{i},
                    x.termID = ggt.goTermID,
                    x.name = ggt.name,
                    x.definition = ggt.definition,
                    x.identifier = ggt.goID,
                    x.isObsolete = ggt.isObsolete,
                    x.ontologyID = "SK15(" + ggt.goTermID + ")"
            ON MATCH
                SET x:BIOSQLTerm{i},
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
            rules.append(rule5)
        # rule#6 using our framework
        for i in range(scale):
            rule6 = TransformationRule(f"""
            MATCH (ggs:GUSGoSynonym{i})
            MATCH (ggt:GUSGoTerm{i})
            WHERE ggs.goTermID = ggt.goTermID
            MERGE (x:_dummy {{
                _id: "(" + elementID(ggs) + ",{i})"
            }})
            ON CREATE
                SET x:BIOSQLTermSynonym{i},
                    x.synonym = ggs.goSynonymID,
                    x.termID = ggs.goTermID
            ON MATCH
                SET x:BIOSQLTermSynonym{i},
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
            MERGE (y:_dummy {{
                _id: "(" + elementID(ggt) + ",{i})"
            }})
            ON CREATE
                SET y:BIOSQLTerm{i},
                    y.termID = ggt.goTermID,
                    y.name = ggt.name,
                    y.definition = ggt.definition,
                    y.identifier = ggt.goID,
                    y.isObsolete = ggt.isObsolete,
                    y.ontologyID = "SK15(" + ggt.goTermID + ")"
            ON MATCH
                SET y:BIOSQLTerm{i},
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
            MERGE (x)-[:HAS_SYNONYM{i} {{
                _id: "(HAS_SYNONYM{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule6)
        # rule#7 using our framework
        for i in range(scale):
            rule7 = TransformationRule(f"""
            MATCH (ggr:GUSGoRelationship{i})
            MATCH (ggt1:GUSGoTerm{i})
            MATCH (ggt2:GUSGoTerm{i})
            MERGE (x:_dummy {{
                _id: "(" + elementID(ggt1) + ",{i})"
            }})
            ON CREATE
                SET x:BIOSQLTerm{i},
                    x.termID = ggt1.goTermID,
                    x.name = ggt1.name,
                    x.definition = ggt1.definition,
                    x.identifier = ggt1.goID,
                    x.isObsolete = ggt1.isObsolete,
                    x.ontologyID = "SK21(" + ggt1.goTermID + ")"
            ON MATCH
                SET x:BIOSQLTerm{i},
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
            MERGE (y:_dummy {{
                _id: "(" + elementID(ggt2) + ",{i})"
            }})
            ON CREATE
                SET y:BIOSQLTerm{i},
                    y.termID = ggt2.goTermID,
                    y.name = ggt2.name,
                    y.definition = ggt2.definition,
                    y.identifier = ggt2.goID,
                    y.isObsolete = ggt2.isObsolete,
                    y.ontologyID = "SK22(" + ggt2.goTermID + ")"
            ON MATCH
                SET y:BIOSQLTerm{i},
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
            MERGE (x)-[z:TERM_RELATIONSHIP{i} {{
                _id: "(TERM_RELATIONSHIP{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
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
            rules.append(rule7)
        # rule#8 using our framework
        for i in range(scale):
            rule8 = TransformationRule(f"""
            MATCH (gg:GUSGene{i})
            MERGE (x:_dummy {{
                _id: "(" + elementID(gg) + ",{i})"
            }})
            ON CREATE
                SET x:BIOSQLTerm{i},
                    x.termID = gg.geneID,
                    x.name = gg.name,
                    x.definition = gg.description,
                    x.identifier = "SK13(" + gg.geneID + ")", 
                    x.isObsolete = "SK18(" + gg.geneID + "," + gg.reviewStatusID + ")",
                    x.ontologyID = "SK14(" + gg.sequenceOntologyID + ")"
            ON MATCH
                SET x:BIOSQLTerm{i},
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
            rules.append(rule8)
        # transformation rules
        self.rules = rules