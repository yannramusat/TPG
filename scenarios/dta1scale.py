import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class DBLPToAmalgam1Scale(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        relations = []
        # csv#1
        for i in range(scale):
            rel_dinproceedings_cmd = f"""MERGE (n:DInProceedings{i} {{
                pid: row[1], 
                title: row[2],
                pages: row[3],
                booktitle: row[4],
                url: row[5],
                cdrom: row[6],
                month: row[7],
                year: row[8]
            }})"""
            param_string = "dta1/dinproceedings"+str(size)+"-"+str(lstring)+".csv"
            rel_dinproceedings = InputRelation(os.path.join(prefix, param_string), rel_dinproceedings_cmd)
            relations.append(rel_dinproceedings)
        # csv#2
        for i in range(scale):
            rel_darticle_cmd = f"""MERGE (n:DArticle{i} {{
                pid: row[1], 
                title: row[2], 
                pages: row[3], 
                cdrom: row[4], 
                month: row[5], 
                year: row[6], 
                volume: row[7], 
                journal: row[8], 
                number: row[9], 
                url: row[10]
            }})"""
            param_string = "dta1/darticle"+str(size)+"-"+str(lstring)+".csv"
            rel_darticle = InputRelation(os.path.join(prefix, param_string), rel_darticle_cmd)
            relations.append(rel_darticle)
        # csv#3
        for i in range(scale):
            rel_pubauthors_cmd = f"""MERGE (n:PubAuthors{i} {{
                pid: row[1],
                author: row[2]
            }})"""
            param_string = "dta1/pubauthors"+str(size)+"-"+str(lstring)+".csv"
            rel_pubauthors = InputRelation(os.path.join(prefix, param_string), rel_pubauthors_cmd)
            relations.append(rel_pubauthors)
        # csv#4
        for i in range(scale):
            rel_dbook_cmd = f"""MERGE (n:DBook{i} {{
                pid: row[1],
                editor: row[2],
                title: row[3],
                publisher: row[4],
                year: row[5],
                isbn: row[6],
                cdrom: row[7],
                citel: row[8],
                url: row[9]
            }})"""
            param_string = "dta1/dbook"+str(size)+"-"+str(lstring)+".csv"
            rel_dbook = InputRelation(os.path.join(prefix, param_string), rel_dbook_cmd)
            relations.append(rel_dbook)
        # csv#5
        for i in range(scale):
            rel_masterthesis_cmd = f"""MERGE (n:MasterThesis{i} {{
                author: row[1],
                title: row[2],
                year: row[3],
                school: row[4]
            }})"""
            param_string = "dta1/masterthesis"+str(size)+"-"+str(lstring)+".csv"
            rel_masterthesis = InputRelation(os.path.join(prefix, param_string), rel_masterthesis_cmd)
            relations.append(rel_masterthesis)
        # csv#6
        for i in range(scale):
            rel_phdthesis_cmd = f"""MERGE (n:PhDThesis{i} {{
                author: row[1],
                title: row[2],
                year: row[3],
                series: row[4],
                number: row[5],
                month: row[6],
                school: row[7],
                publisher: row[8],
                isbn: row[9]
            }})"""
            param_string = "dta1/phdthesis"+str(size)+"-"+str(lstring)+".csv"
            rel_phdthesis = InputRelation(os.path.join(prefix, param_string), rel_phdthesis_cmd)
            relations.append(rel_phdthesis)
        # csv#7
        for i in range(scale):
            rel_www_cmd = f"""MERGE (n:WWW{i} {{
                pid: row[1],
                title: row[2],
                year: row[3],
                url: row[4]
            }})"""
            param_string = "dta1/www"+str(size)+"-"+str(lstring)+".csv"
            rel_www = InputRelation(os.path.join(prefix, param_string), rel_www_cmd)
            relations.append(rel_www)
        # source schema
        self.schema = InputSchema(relations)

class DBLPToAmalgam1PlainScale(DBLPToAmalgam1Scale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # csv#1
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (dip:DInProceedings{i})
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(dip) + ",{i})" 
            }})
            SET x:InProceedings{i},
                x.pid = "SK1(" + dip.pid + ")",
                x.title = dip.title,
                x.bktitle = dip.booktitle,
                x.year = dip.year,
                x.month = dip.month,
                x.pages = dip.pages,
                x.vol = "SK2(" + dip.booktitle + "," + dip.year + ")",
                x.num = "SK3(" + dip.booktitle + "," + dip.year + "," + dip.month + ")", 
                x.loc = "SK4(" + dip.booktitle + "," + dip.year + "," + dip.month + ")", 
                x.class = "SK6(" + dip.pid + ")",
                x.note = "SK7(" + dip.pid + ")",
                x.annote = "SK8(" + dip.pid + ")"
            """)
            rules.append(rule1)
        # rule#2 using our framework
        for i in range(scale):
            rule2 = TransformationRule(f"""
            MATCH (dip:DInProceedings{i})
            MATCH (pa:PubAuthors{i})
            WHERE pa.pid = dip.pid
            MERGE (a:_dummy {{
                _id: "(" + pa.author + ",{i})"
            }})
            SET a:Author{i},
                a.name = pa.author
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(dip) + ",{i})" 
            }})
            SET x:InProceedings{i},
                x.pid = "SK1(" + dip.pid + ")",
                x.title = dip.title,
                x.bktitle = dip.booktitle,
                x.year = dip.year,
                x.month = dip.month,
                x.pages = dip.pages,
                x.vol = "SK2(" + dip.booktitle + "," + dip.year + ")",
                x.num = "SK3(" + dip.booktitle + "," + dip.year + "," + dip.month + ")", 
                x.loc = "SK4(" + dip.booktitle + "," + dip.year + "," + dip.month + ")", 
                x.class = "SK6(" + dip.pid + ")",
                x.note = "SK7(" + dip.pid + ")",
                x.annote = "SK8(" + dip.pid + ")"
            MERGE (x)-[:IN_PROC_PUBLISHED{i} {{
                _id: "(IN_PROC_PUBLISHED{i}:" + elementId(x) + "," + elementId(a) + "),{i}"
            }}]-(a)
            """)
            rules.append(rule2)
        # rule#3 using our framework
        for i in range(scale):
            rule3 = TransformationRule(f"""
            MATCH (w:WWW{i})
            MERGE (m:_dummy {{
                _id: "(" + elementId(w) + ",{i})"
            }})
            SET m:Misc{i},
                m.miscid = "SK11(" + w.pid + ")",
                m.howpub = "SK12(" + w.pid + ")",
                m.confloc = "SK13(" + w.pid + ")",
                m.year = w.year,
                m.month = "SK14(" + w.pid + ")",
                m.pages = "SK15(" + w.pid + ")",
                m.vol = "SK16(" + w.pid + ")",
                m.num = "SK17(" + w.pid + ")",
                m.loc = "SK18(" + w.pid + ")",
                m.class ="SK19(" + w.pid + ")",
                m.note = "SK20(" + w.pid + ")",
                m.annote = "SK21(" + w.pid + ")"
            """)
            rules.append(rule3)
        # rule#4 using our framework
        for i in range(scale):
            rule4 = TransformationRule(f"""
            MATCH (w:WWW{i})
            MATCH (pa:PubAuthors{i})
            WHERE pa.pid = w.pid
            MERGE (a:_dummy {{
                _id: "(" + pa.author + ",{i})"
            }})
            SET a:Author{i},
                a.name = pa.author
            MERGE (m:_dummy {{
                _id: "(" + elementId(w) + ",{i})"
            }})
            SET m:Misc{i},
                m.miscid = "SK11(" + w.pid + ")",
                m.howpub = "SK12(" + w.pid + ")",
                m.confloc = "SK13(" + w.pid + ")",
                m.year = w.year,
                m.month = "SK14(" + w.pid + ")",
                m.pages = "SK15(" + w.pid + ")",
                m.vol = "SK16(" + w.pid + ")",
                m.num = "SK17(" + w.pid + ")",
                m.loc = "SK18(" + w.pid + ")",
                m.class ="SK19(" + w.pid + ")",
                m.note = "SK20(" + w.pid + ")",
                m.annote = "SK21(" + w.pid + ")"
            MERGE (m)-[:MISC_PUBLISHED{i} {{
                _id: "(MISC_PUBLISHED{i}:" + elementId(m) + "," + elementId(a) + ",{i})"
            }}]-(a)
            """)
            rules.append(rule4)
        # rule#5 using our framework
        for i in range(scale):
            rule5 = TransformationRule(f"""
            MATCH (da:DArticle{i})
            MERGE (a:_dummy {{ 
                _id: "(" + elementId(da) + ",{i})" 
            }})
            SET a:Article{i},
                a.articleid = "SK22(" + da.pid + ")",
                a.title = da.title,
                a.journal = da.journal,
                a.year = da.year,
                a.month = da.month,
                a.pages = da.pages,
                a.vol = da.volume,
                a.num = da.number, 
                a.loc = "SK23(" + da.pid + ")", 
                a.class = "SK24(" + da.pid + ")",
                a.note = "SK25(" + da.pid + ")",
                a.annote = "SK26(" + da.pid + ")"
            """)
            rules.append(rule5)
        # rule#6 using our framework
        for i in range(scale):
            rule6 = TransformationRule(f"""
            MATCH (da:DArticle{i})
            MATCH (pa:PubAuthors{i})
            WHERE pa.pid = da.pid
            MERGE (au:_dummy {{
                _id: "(" + pa.author + ",{i})"
            }})
            SET au:Author{i},
                au.name = pa.author
            MERGE (a:_dummy {{ 
                _id: "(" + elementId(da) + ",{i})"
            }})
            SET a:Article{i},
                a.articleid = "SK22(" + da.pid + ")",
                a.title = da.title,
                a.journal = da.journal,
                a.year = da.year,
                a.month = da.month,
                a.pages = da.pages,
                a.vol = da.volume,
                a.num = da.number, 
                a.loc = "SK23(" + da.pid + ")", 
                a.class = "SK24(" + da.pid + ")",
                a.note = "SK25(" + da.pid + ")",
                a.annote = "SK26(" + da.pid + ")"
            MERGE (a)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(a) + "," + elementId(au) + ",{i})"
            }}]-(au)
            """)
            rules.append(rule6)
        # rule#7 using our framework
        for i in range(scale):
            rule7 = TransformationRule(f"""
            MATCH (db:DBook{i})
            MERGE (b:_dummy {{ 
                _id: "(" + elementId(db) + ",{i})" 
            }})
            SET b:Book{i},
                b.bookID = "SK27(" + db.pid + ")",
                b.title = db.title,
                b.publisher = db.publisher,
                b.year = db.year,
                b.month = "SK28(" + db.pid + ")",
                b.pages = "SK29(" + db.pid + ")",
                b.vol = "SK30(" + db.pid + ")",
                b.num = "SK31(" + db.pid + ")", 
                b.loc = "SK32(" + db.pid + ")", 
                b.class = "SK33(" + db.pid + ")",
                b.note = "SK34(" + db.pid + ")",
                b.annote = "SK35(" + db.pid + ")"
            """)
            rules.append(rule7)
        # rule#8 using our framework
        for i in range(scale):
            rule8 = TransformationRule(f"""
            MATCH (db:DBook{i})
            MATCH (pa:PubAuthors{i})
            WHERE pa.pid = db.pid
            MERGE (au:_dummy {{
                _id: "(" + pa.author + ",{i})"
            }})
            SET au:Author{i},
                au.name = pa.author
            MERGE (b:_dummy {{ 
                _id: "(" + elementId(db) + ",{i})" 
            }})
            SET b:Book{i},
                b.bookID = "SK27(" + db.pid + ")",
                b.title = db.title,
                b.publisher = db.publisher,
                b.year = db.year,
                b.month = "SK28(" + db.pid + ")",
                b.pages = "SK29(" + db.pid + ")",
                b.vol = "SK30(" + db.pid + ")",
                b.num = "SK31(" + db.pid + ")", 
                b.loc = "SK32(" + db.pid + ")", 
                b.class = "SK33(" + db.pid + ")",
                b.note = "SK34(" + db.pid + ")",
                b.annote = "SK35(" + db.pid + ")"
            MERGE (b)-[:BOOK_PUBLISHED{i} {{
                _id: "(BOOK_PUBLISHED{i}:" + elementId(b) + "," + elementId(au) + ",{i})"
            }}]-(au)
            """)
            rules.append(rule8)
        # rule#9 using our framework
        for i in range(scale):
            rule9 = TransformationRule(f"""
            MATCH (t:PhDThesis{i})
            MERGE (au:_dummy {{
                _id: "(" + t.author + ",{i})"
            }})
            SET au:Author{i},
                au.name = t.author
            MERGE (m:_dummy {{
                _id: "(" + elementId(t) + ",{i})"
            }})
            SET m:Misc{i},
                m.miscid = "SK36(" + t.author + "," + t.title + ")",
                m.title = t.title,
                m.howpub = "SK37(" + t.author + "," + t.title + ")",
                m.confloc = "SK38(" + t.author + "," + t.title + ")",
                m.year = t.year,
                m.month = t.month,
                m.pages = "SK39(" + t.author + "," + t.title + ")",
                m.vol = "SK40(" + t.author + "," + t.title + ")",
                m.num = t.number,
                m.loc = "SK41(" + t.author + "," + t.title + ")",
                m.class = "SK42(" + t.author + "," + t.title + ")",
                m.note = "SK43(" + t.author + "," + t.title + ")",
                m.annote = t.school
            MERGE (m)-[:MISC_PUBLISHED{i} {{
                _id: "(MISC_PUBLISHED{i}:" + elementId(m) + "," + elementId(au) + ",{i})"
            }}]-(au)
            """)
            rules.append(rule9)
        # rule#10 using our framework
        for i in range(scale):
            rule10 = TransformationRule(f"""
            MATCH (t:MasterThesis{i})
            MERGE (au:_dummy {{
                _id: "(" + t.author + ",{i})"
            }})
            SET au:Author{i},
                au.name = t.author
            MERGE (m:_dummy {{
                _id: "(" + elementId(t) + ",{i})"
            }})
            SET m:Misc{i},
                m.miscid = "SK44(" + t.author + "," + t.title + ")",
                m.title = t.title,
                m.howpub = "SK45(" + t.author + "," + t.title + ")",
                m.confloc = "SK46(" + t.author + "," + t.title + ")",
                m.year = t.year,
                m.month = "SK47(" + t.author + "," + t.title + ")",
                m.pages = "SK48(" + t.author + "," + t.title + ")",
                m.vol = "SK49(" + t.author + "," + t.title + ")",
                m.num = "SK50(" + t.author + "," + t.title + ")",
                m.loc = "SK51(" + t.author + "," + t.title + ")",
                m.class = "SK52(" + t.author + "," + t.title + ")",
                m.note = "SK53(" + t.author + "," + t.title + ")",
                m.annote = t.school
            MERGE (m)-[:MISC_PUBLISHED{i} {{
                _id: "(MISC_PUBLISHED{i}:" + elementId(m) + "," + elementId(au) + ",{i})"
            }}]-(au)
            """)
            rules.append(rule10)
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

class DBLPToAmalgam1CDoverPlainScale(DBLPToAmalgam1PlainScale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (dip:DInProceedings{i})
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(dip) + ",{i})" 
            }})
            ON CREATE
                SET x:InProceedings{i},
                    x.pid = "SK1(" + dip.pid + ")",
                    x.title = dip.title,
                    x.bktitle = dip.booktitle,
                    x.year = dip.year,
                    x.month = dip.month,
                    x.pages = dip.pages,
                    x.vol = "SK2(" + dip.booktitle + "," + dip.year + ")",
                    x.num = "SK3(" + dip.booktitle + "," + dip.year + "," + dip.month + ")", 
                    x.loc = "SK4(" + dip.booktitle + "," + dip.year + "," + dip.month + ")", 
                    x.class = "SK6(" + dip.pid + ")",
                    x.note = "SK7(" + dip.pid + ")",
                    x.annote = "SK8(" + dip.pid + ")"
            ON MATCH
                SET x:InProceedings{i},
                    x.pid =
                    CASE
                        WHEN x.pid <> "SK1(" + dip.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK1(" + dip.pid + ")"
                    END,
                    x.title =
                    CASE
                        WHEN x.title <> dip.title
                            THEN "Conflict detected!"
                        ELSE
                            dip.title
                    END,
                    x.bktitle =
                    CASE
                        WHEN x.bktitle <> dip.booktitle
                            THEN "Conflict detected!"
                        ELSE
                            dip.booktitle
                    END,
                    x.year =
                    CASE
                        WHEN x.year <> dip.year
                            THEN "Conflict detected!"
                        ELSE
                            dip.year
                    END,
                    x.month =
                    CASE
                        WHEN x.month <> dip.month
                            THEN "Conflict detected!"
                        ELSE
                            dip.month
                    END,
                    x.pages =
                    CASE
                        WHEN x.pages <> dip.pages
                            THEN "Conflict detected!"
                        ELSE
                            dip.pages
                    END,
                    x.vol =
                    CASE
                        WHEN x.vol <> "SK2(" + dip.booktitle + "," + dip.year + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK2(" + dip.booktitle + "," + dip.year + ")"
                    END,
                    x.num =
                    CASE
                        WHEN x.num <> "SK3(" + dip.booktitle + "," + dip.year + "," + dip.month + ")"
                            THEN "Conflict detected!"
                        ELSE    
                            "SK3(" + dip.booktitle + "," + dip.year + "," + dip.month + ")"
                    END,
                    x.loc =
                    CASE
                        WHEN x.loc <> "SK4(" + dip.booktitle + "," + dip.year + "," + dip.month + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK4(" + dip.booktitle + "," + dip.year + "," + dip.month + ")"
                    END,
                    x.class =
                    CASE
                        WHEN x.class <> "SK6(" + dip.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK6(" + dip.pid + ")"
                    END,
                    x.note =
                    CASE
                        WHEN x.note <> "SK7(" + dip.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK7(" + dip.pid + ")"
                    END,
                    x.annote =
                    CASE
                        WHEN x.annote <> "SK8(" + dip.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK8(" + dip.pid + ")"
                    END
            """)
            rules.append(rule1)
        # rule#2 using our framework
        for i in range(scale):
            rule2 = TransformationRule(f"""
            MATCH (dip:DInProceedings{i})
            MATCH (pa:PubAuthors{i})
            WHERE pa.pid = dip.pid
            MERGE (a:_dummy {{
                _id: "(" + pa.author + ",{i})"
            }})
            ON CREATE
                SET a:Author{i},
                    a.name = pa.author
            ON MATCH
                SET a:Author{i},
                    a.name =
                    CASE
                        WHEN a.name <> pa.author
                            THEN "Conflict detected!"
                        ELSE
                            pa.author
                    END
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(dip) + ",{i})"
            }})
            ON CREATE
                SET x:InProceedings{i},
                    x.pid = "SK1(" + dip.pid + ")",
                    x.title = dip.title,
                    x.bktitle = dip.booktitle,
                    x.year = dip.year,
                    x.month = dip.month,
                    x.pages = dip.pages,
                    x.vol = "SK2(" + dip.booktitle + "," + dip.year + ")",
                    x.num = "SK3(" + dip.booktitle + "," + dip.year + "," + dip.month + ")", 
                    x.loc = "SK4(" + dip.booktitle + "," + dip.year + "," + dip.month + ")", 
                    x.class = "SK6(" + dip.pid + ")",
                    x.note = "SK7(" + dip.pid + ")",
                    x.annote = "SK8(" + dip.pid + ")"
            ON MATCH
                SET x:InProceedings{i},
                    x.pid =
                    CASE
                        WHEN x.pid <> "SK1(" + dip.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK1(" + dip.pid + ")"
                    END,
                    x.title =
                    CASE
                        WHEN x.title <> dip.title
                            THEN "Conflict detected!"
                        ELSE
                            dip.title
                    END,
                    x.bktitle =
                    CASE
                        WHEN x.bktitle <> dip.booktitle
                            THEN "Conflict detected!"
                        ELSE
                            dip.booktitle
                    END,
                    x.year =
                    CASE
                        WHEN x.year <> dip.year
                            THEN "Conflict detected!"
                        ELSE
                            dip.year
                    END,
                    x.month =
                    CASE
                        WHEN x.month <> dip.month
                            THEN "Conflict detected!"
                        ELSE
                            dip.month
                    END,
                    x.pages =
                    CASE
                        WHEN x.pages <> dip.pages
                            THEN "Conflict detected!"
                        ELSE
                            dip.pages
                    END,
                    x.vol =
                    CASE
                        WHEN x.vol <> "SK2(" + dip.booktitle + "," + dip.year + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK2(" + dip.booktitle + "," + dip.year + ")"
                    END,
                    x.num =
                    CASE
                        WHEN x.num <> "SK3(" + dip.booktitle + "," + dip.year + "," + dip.month + ")"
                            THEN "Conflict detected!"
                        ELSE    
                            "SK3(" + dip.booktitle + "," + dip.year + "," + dip.month + ")"
                    END,
                    x.loc =
                    CASE
                        WHEN x.loc <> "SK4(" + dip.booktitle + "," + dip.year + "," + dip.month + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK4(" + dip.booktitle + "," + dip.year + "," + dip.month + ")"
                    END,
                    x.class =
                    CASE
                        WHEN x.class <> "SK6(" + dip.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK6(" + dip.pid + ")"
                    END,
                    x.note =
                    CASE
                        WHEN x.note <> "SK7(" + dip.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK7(" + dip.pid + ")"
                    END,
                    x.annote =
                    CASE
                        WHEN x.annote <> "SK8(" + dip.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK8(" + dip.pid + ")"
                    END
            MERGE (x)-[:IN_PROC_PUBLISHED{i} {{
                _id: "(IN_PROC_PUBLISHED{i}:" + elementId(x) + "," + elementId(a) + ",{i})"
            }}]-(a)
            """)
            rules.append(rule2)
        # rule#3 using our framework
        for i in range(scale):
            rule3 = TransformationRule(f"""
            MATCH (w:WWW{i})
            MERGE (m:_dummy {{
                _id: "(" + elementId(w) + ",{i})"
            }})
            ON CREATE
                SET m:Misc{i},
                    m.miscid = "SK11(" + w.pid + ")",
                    m.howpub = "SK12(" + w.pid + ")",
                    m.confloc = "SK13(" + w.pid + ")",
                    m.year = w.year,
                    m.month = "SK14(" + w.pid + ")",
                    m.pages = "SK15(" + w.pid + ")",
                    m.vol = "SK16(" + w.pid + ")",
                    m.num = "SK17(" + w.pid + ")",
                    m.loc = "SK18(" + w.pid + ")",
                    m.class ="SK19(" + w.pid + ")",
                    m.note = "SK20(" + w.pid + ")",
                    m.annote = "SK21(" + w.pid + ")"
            ON MATCH
                SET m:Misc{i},
                    m.miscid =
                    CASE
                        WHEN m.miscid <> "SK11(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK11(" + w.pid + ")"
                    END,
                    m.howpub =
                    CASE
                        WHEN m.howpub <> "SK12(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK12(" + w.pid + ")"
                    END,
                    m.confloc =
                    CASE
                        WHEN m.confloc <> "SK13(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK13(" + w.pid + ")"
                    END,
                    m.year =
                    CASE
                        WHEN m.year <> w.year
                            THEN "Conflict detected!"
                        ELSE
                            w.year
                    END,
                    m.month =
                    CASE
                        WHEN m.month <> "SK14(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK14(" + w.pid + ")"
                    END,
                    m.pages =
                    CASE
                        WHEN m.pages <> "SK15(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK15(" + w.pid + ")"
                    END,
                    m.vol =
                    CASE
                        WHEN m.vol <> "SK16(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK16(" + w.pid + ")"
                    END,
                    m.num =
                    CASE
                        WHEN m.num <> "SK17(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK17(" + w.pid + ")"
                    END,
                    m.loc =
                    CASE
                        WHEN m.loc <> "SK18(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK18(" + w.pid + ")"
                    END,
                    m.class =
                    CASE
                        WHEN m.class <> "SK19(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK19(" + w.pid + ")"
                    END,
                    m.note =
                    CASE
                        WHEN m.note <> "SK20(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK20(" + w.pid + ")"
                    END,
                    m.annote =
                    CASE
                        WHEN m.annote <> "SK21(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK21(" + w.pid + ")"
                    END
            """)
            rules.append(rule3)
        # rule#4 using our framework
        for i in range(scale):
            rule4 = TransformationRule(f"""
            MATCH (w:WWW{i})
            MATCH (pa:PubAuthors{i})
            WHERE pa.pid = w.pid
            MERGE (a:_dummy {{
                _id: "(" + pa.author + ",{i})"
            }})
            ON CREATE
                SET a:Author{i},
                    a.name = pa.author
            ON MATCH
                SET a:Author{i},
                    a.name =
                    CASE
                        WHEN a.name <> pa.author
                            THEN "Conflict detected!"
                        ELSE
                            pa.author
                    END
            MERGE (m:_dummy {{
                _id: "(" + elementId(w) + ",{i})"
            }})
            ON CREATE
                SET m:Misc{i},
                    m.miscid = "SK11(" + w.pid + ")",
                    m.howpub = "SK12(" + w.pid + ")",
                    m.confloc = "SK13(" + w.pid + ")",
                    m.year = w.year,
                    m.month = "SK14(" + w.pid + ")",
                    m.pages = "SK15(" + w.pid + ")",
                    m.vol = "SK16(" + w.pid + ")",
                    m.num = "SK17(" + w.pid + ")",
                    m.loc = "SK18(" + w.pid + ")",
                    m.class ="SK19(" + w.pid + ")",
                    m.note = "SK20(" + w.pid + ")",
                    m.annote = "SK21(" + w.pid + ")"
            ON MATCH
                SET m:Misc{i},
                    m.miscid =
                    CASE
                        WHEN m.miscid <> "SK11(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK11(" + w.pid + ")"
                    END,
                    m.howpub =
                    CASE
                        WHEN m.howpub <> "SK12(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK12(" + w.pid + ")"
                    END,
                    m.confloc =
                    CASE
                        WHEN m.confloc <> "SK13(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK13(" + w.pid + ")"
                    END,
                    m.year =
                    CASE
                        WHEN m.year <> w.year
                            THEN "Conflict detected!"
                        ELSE
                            w.year
                    END,
                    m.month =
                    CASE
                        WHEN m.month <> "SK14(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK14(" + w.pid + ")"
                    END,
                    m.pages =
                    CASE
                        WHEN m.pages <> "SK15(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK15(" + w.pid + ")"
                    END,
                    m.vol =
                    CASE
                        WHEN m.vol <> "SK16(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK16(" + w.pid + ")"
                    END,
                    m.num =
                    CASE
                        WHEN m.num <> "SK17(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK17(" + w.pid + ")"
                    END,
                    m.loc =
                    CASE
                        WHEN m.loc <> "SK18(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK18(" + w.pid + ")"
                    END,
                    m.class =
                    CASE
                        WHEN m.class <> "SK19(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK19(" + w.pid + ")"
                    END,
                    m.note =
                    CASE
                        WHEN m.note <> "SK20(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK20(" + w.pid + ")"
                    END,
                    m.annote =
                    CASE
                        WHEN m.annote <> "SK21(" + w.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK21(" + w.pid + ")"
                    END
            MERGE (m)-[:MISC_PUBLISHED{i} {{
                _id: "(MISC_PUBLISHED{i}:" + elementId(m) + "," + elementId(a) + ",{i})"
            }}]-(a)
            """)
            rules.append(rule4)
        # rule#5 using our framework
        for i in range(scale):
            rule5 = TransformationRule(f"""
            MATCH (da:DArticle{i})
            MERGE (a:_dummy {{ 
                _id: "(" + elementId(da) + ",{i})" 
            }})
            ON CREATE
                SET a:Article{i},
                    a.articleid = "SK22(" + da.pid + ")",
                    a.title = da.title,
                    a.journal = da.journal,
                    a.year = da.year,
                    a.month = da.month,
                    a.pages = da.pages,
                    a.vol = da.volume,
                    a.num = da.number, 
                    a.loc = "SK23(" + da.pid + ")", 
                    a.class = "SK24(" + da.pid + ")",
                    a.note = "SK25(" + da.pid + ")",
                    a.annote = "SK26(" + da.pid + ")"
            ON MATCH
                SET a:Article{i},
                    a.articleid =
                    CASE
                        WHEN a.articleid <> "SK22(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK22(" + da.pid + ")"
                    END,
                    a.title =
                    CASE
                        WHEN a.title <> da.title
                            THEN "Conflict detected!"
                        ELSE
                            da.title
                    END,
                    a.journal =
                    CASE
                        WHEN a.journal <> da.journal
                            THEN "Conflict detected!"
                        ELSE
                            da.journal
                    END,
                    a.year =
                    CASE
                        WHEN a.year <> da.year
                            THEN "Conflict detected!"
                        ELSE
                            da.year
                    END,
                    a.month =
                    CASE
                        WHEN a.month <> da.month
                            THEN "Conflict detected!"
                        ELSE
                            da.month
                    END,
                    a.pages =
                    CASE
                        WHEN a.pages <> da.pages
                            THEN "Conflict detected!"
                        ELSE
                            da.pages
                    END,
                    a.vol =
                    CASE
                        WHEN a.vol <> da.volume
                            THEN "Conflict detected!"
                        ELSE
                            da.volume
                    END,
                    a.num =
                    CASE
                        WHEN a.num <> da.number
                            THEN "Conflict detected!"
                        ELSE
                            da.number
                    END,
                    a.loc =
                    CASE
                        WHEN a.loc <> "SK23(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK23(" + da.pid + ")"
                    END,
                    a.class =
                    CASE
                        WHEN a.class <> "SK24(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK24(" + da.pid + ")"
                    END,
                    a.note =
                    CASE
                        WHEN a.note <> "SK25(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK25(" + da.pid + ")"
                    END,
                    a.annote =
                    CASE
                        WHEN a.annote <> "SK26(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK26(" + da.pid + ")"
                    END
            """)
            rules.append(rule5)
        # rule#6 using our framework
        for i in range(scale):
            rule6 = TransformationRule(f"""
            MATCH (da:DArticle{i})
            MATCH (pa:PubAuthors{i})
            WHERE pa.pid = da.pid
            MERGE (au:_dummy {{
                _id: "(" + pa.author + ",{i})"
            }})
            ON CREATE
                SET au:Author{i},
                    au.name = pa.author
            ON MATCH
                SET au:Author{i},
                    au.name =
                    CASE
                        WHEN au.name <> pa.author
                            THEN "Conflict detected!"
                        ELSE
                            pa.author
                    END
            MERGE (a:_dummy {{ 
                _id: "(" + elementId(da) + ",{i})" 
            }})
            ON CREATE
                SET a:Article{i},
                    a.articleid = "SK22(" + da.pid + ")",
                    a.title = da.title,
                    a.journal = da.journal,
                    a.year = da.year,
                    a.month = da.month,
                    a.pages = da.pages,
                    a.vol = da.volume,
                    a.num = da.number, 
                    a.loc = "SK23(" + da.pid + ")", 
                    a.class = "SK24(" + da.pid + ")",
                    a.note = "SK25(" + da.pid + ")",
                    a.annote = "SK26(" + da.pid + ")"
            ON MATCH
                SET a:Article{i},
                    a.articleid =
                    CASE
                        WHEN a.articleid <> "SK22(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK22(" + da.pid + ")"
                    END,
                    a.title =
                    CASE
                        WHEN a.title <> da.title
                            THEN "Conflict detected!"
                        ELSE
                            da.title
                    END,
                    a.journal =
                    CASE
                        WHEN a.journal <> da.journal
                            THEN "Conflict detected!"
                        ELSE
                            da.journal
                    END,
                    a.year =
                    CASE
                        WHEN a.year <> da.year
                            THEN "Conflict detected!"
                        ELSE
                            da.year
                    END,
                    a.month =
                    CASE
                        WHEN a.month <> da.month
                            THEN "Conflict detected!"
                        ELSE
                            da.month
                    END,
                    a.pages =
                    CASE
                        WHEN a.pages <> da.pages
                            THEN "Conflict detected!"
                        ELSE
                            da.pages
                    END,
                    a.vol =
                    CASE
                        WHEN a.vol <> da.volume
                            THEN "Conflict detected!"
                        ELSE
                            da.volume
                    END,
                    a.num =
                    CASE
                        WHEN a.num <> da.number
                            THEN "Conflict detected!"
                        ELSE
                            da.number
                    END,
                    a.loc =
                    CASE
                        WHEN a.loc <> "SK23(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK23(" + da.pid + ")"
                    END,
                    a.class =
                    CASE
                        WHEN a.class <> "SK24(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK24(" + da.pid + ")"
                    END,
                    a.note =
                    CASE
                        WHEN a.note <> "SK25(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK25(" + da.pid + ")"
                    END,
                    a.annote =
                    CASE
                        WHEN a.annote <> "SK26(" + da.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK26(" + da.pid + ")"
                    END
            MERGE (a)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(a) + "," + elementId(au) + ",{i})"
            }}]-(au)
            """)
            rules.append(rule6)
        # rule#7 using our framework
        for i in range(scale):
            rule7 = TransformationRule(f"""
            MATCH (db:DBook{i})
            MERGE (b:_dummy {{ 
                _id: "(" + elementId(db) + ",{i})" 
            }})
            ON CREATE
                SET b:Book{i},
                    b.bookID = "SK27(" + db.pid + ")",
                    b.title = db.title,
                    b.publisher = db.publisher,
                    b.year = db.year,
                    b.month = "SK28(" + db.pid + ")",
                    b.pages = "SK29(" + db.pid + ")",
                    b.vol = "SK30(" + db.pid + ")",
                    b.num = "SK31(" + db.pid + ")", 
                    b.loc = "SK32(" + db.pid + ")", 
                    b.class = "SK33(" + db.pid + ")",
                    b.note = "SK34(" + db.pid + ")",
                    b.annote = "SK35(" + db.pid + ")"
            ON MATCH
                SET b:Book{i},
                    b.bookID =
                    CASE
                        WHEN b.bookID <> "SK27(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK27(" + db.pid + ")"
                    END,
                    b.title =
                    CASE
                        WHEN b.title <> db.title
                            THEN "Conflict detected!"
                        ELSE
                            db.title
                    END,
                    b.publisher =
                    CASE
                        WHEN b.publisher <> db.publisher
                            THEN "Conflict detected!"
                        ELSE
                            db.publisher
                    END,
                    b.year =
                    CASE
                        WHEN b.year <> db.year
                            THEN "Conflict detected!"
                        ELSE
                            db.year
                    END,
                    b.month =
                    CASE
                        WHEN b.month <> "SK28(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK28(" + db.pid + ")"
                    END,
                    b.pages =
                    CASE
                        WHEN b.pages <> "SK29(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK29(" + db.pid + ")"
                    END,
                    b.vol =
                    CASE
                        WHEN b.vol <> "SK30(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK30(" + db.pid + ")"
                    END,
                    b.num =
                    CASE
                        WHEN b.num <> "SK31(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK31(" + db.pid + ")"
                    END,
                    b.loc =
                    CASE
                        WHEN b.loc <> "SK32(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK32(" + db.pid + ")"
                    END,
                    b.class =
                    CASE
                        WHEN b.class <> "SK33(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK33(" + db.pid + ")"
                    END,
                    b.note =
                    CASE
                        WHEN b.note <> "SK34(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK34(" + db.pid + ")"
                    END,
                    b.annote =
                    CASE
                        WHEN b.annote <> "SK35(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK35(" + db.pid + ")"
                    END
            """)
            rules.append(rule7)
        # rule#8 using our framework
        for i in range(scale):
            rule8 = TransformationRule(f"""
            MATCH (db:DBook{i})
            MATCH (pa:PubAuthors{i})
            WHERE pa.pid = db.pid
            MERGE (au:_dummy {{
                _id: "(" + pa.author + ",{i})"
            }})
            ON CREATE
                SET au:Author{i},
                    au.name = pa.author
            ON MATCH
                SET au:Author{i},
                    au.name =
                    CASE
                        WHEN au.name <> pa.author
                            THEN "Conflict detected!"
                        ELSE
                            pa.author
                    END
            MERGE (b:_dummy {{ 
                _id: "(" + elementId(db) + ",{i})" 
            }})
            ON CREATE
                SET b:Book{i},
                    b.bookID = "SK27(" + db.pid + ")",
                    b.title = db.title,
                    b.publisher = db.publisher,
                    b.year = db.year,
                    b.month = "SK28(" + db.pid + ")",
                    b.pages = "SK29(" + db.pid + ")",
                    b.vol = "SK30(" + db.pid + ")",
                    b.num = "SK31(" + db.pid + ")", 
                    b.loc = "SK32(" + db.pid + ")", 
                    b.class = "SK33(" + db.pid + ")",
                    b.note = "SK34(" + db.pid + ")",
                    b.annote = "SK35(" + db.pid + ")"
            ON MATCH
                SET b:Book{i},
                    b.bookID =
                    CASE
                        WHEN b.bookID <> "SK27(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK27(" + db.pid + ")"
                    END,
                    b.title =
                    CASE
                        WHEN b.title <> db.title
                            THEN "Conflict detected!"
                        ELSE
                            db.title
                    END,
                    b.publisher =
                    CASE
                        WHEN b.publisher <> db.publisher
                            THEN "Conflict detected!"
                        ELSE
                            db.publisher
                    END,
                    b.year =
                    CASE
                        WHEN b.year <> db.year
                            THEN "Conflict detected!"
                        ELSE
                            db.year
                    END,
                    b.month =
                    CASE
                        WHEN b.month <> "SK28(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK28(" + db.pid + ")"
                    END,
                    b.pages =
                    CASE
                        WHEN b.pages <> "SK29(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK29(" + db.pid + ")"
                    END,
                    b.vol =
                    CASE
                        WHEN b.vol <> "SK30(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK30(" + db.pid + ")"
                    END,
                    b.num =
                    CASE
                        WHEN b.num <> "SK31(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK31(" + db.pid + ")"
                    END,
                    b.loc =
                    CASE
                        WHEN b.loc <> "SK32(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK32(" + db.pid + ")"
                    END,
                    b.class =
                    CASE
                        WHEN b.class <> "SK33(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK33(" + db.pid + ")"
                    END,
                    b.note =
                    CASE
                        WHEN b.note <> "SK34(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK34(" + db.pid + ")"
                    END,
                    b.annote =
                    CASE
                        WHEN b.annote <> "SK35(" + db.pid + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK35(" + db.pid + ")"
                    END
            MERGE (b)-[:BOOK_PUBLISHED{i} {{
                _id: "(BOOK_PUBLISHED{i}:" + elementId(b) + "," + elementId(au) + ",{i})"
            }}]-(au)
            """)
            rules.append(rule8)
        # rule#9 using our framework
        for i in range(scale):
            rule9 = TransformationRule(f"""
            MATCH (t:PhDThesis{i})
            MERGE (au:_dummy {{
                _id: "(" + t.author + ",{i})"
            }})
            ON CREATE
                SET au:Author{i},
                    au.name = t.author
            ON MATCH
                SET au:Author{i},
                    au.name =
                    CASE
                        WHEN au.name <> t.author
                            THEN "Conflict detected!"
                        ELSE
                            t.author
                    END
            MERGE (m:_dummy {{
                _id: "(" + elementId(t) + ",{i})"
            }})
            ON CREATE
                SET m:Misc{i},
                    m.miscid = "SK36(" + t.author + "," + t.title + ")",
                    m.title = t.title,
                    m.howpub = "SK37(" + t.author + "," + t.title + ")",
                    m.confloc = "SK38(" + t.author + "," + t.title + ")",
                    m.year = t.year,
                    m.month = t.month,
                    m.pages = "SK39(" + t.author + "," + t.title + ")",
                    m.vol = "SK40(" + t.author + "," + t.title + ")",
                    m.num = t.number,
                    m.loc = "SK41(" + t.author + "," + t.title + ")",
                    m.class = "SK42(" + t.author + "," + t.title + ")",
                    m.note = "SK43(" + t.author + "," + t.title + ")",
                    m.annote = t.school
            ON MATCH
                SET m:Misc{i},
                    m.miscid =
                    CASE
                        WHEN m.miscid <> "SK36(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK36(" + t.author + "," + t.title + ")"
                    END,
                    m.title =
                    CASE
                        WHEN m.title <> t.title
                            THEN "Conflict detected!"
                        ELSE
                            t.title
                    END,
                    m.howpub =
                    CASE
                        WHEN m.howpub <> "SK37(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK37(" + t.author + "," + t.title + ")"
                    END,
                    m.confloc =
                    CASE
                        WHEN m.confloc <> "SK38(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK38(" + t.author + "," + t.title + ")"
                    END,
                    m.year =
                    CASE
                        WHEN m.year <> t.year
                            THEN "Conflict detected!"
                        ELSE
                            t.year
                    END,
                    m.month =
                    CASE
                        WHEN m.month <> t.month
                            THEN "Conflict detected!"
                        ELSE
                            t.month
                    END,
                    m.pages =
                    CASE
                        WHEN m.pages <> "SK39(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK39(" + t.author + "," + t.title + ")"
                    END,
                    m.vol =
                    CASE
                        WHEN m.vol <> "SK40(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK40(" + t.author + "," + t.title + ")"
                    END,
                    m.num =
                    CASE
                        WHEN m.num <> t.number
                            THEN "Conflict detected!"
                        ELSE
                            t.number
                    END,
                    m.loc =
                    CASE
                        WHEN m.loc <> "SK41(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK41(" + t.author + "," + t.title + ")"
                    END,
                    m.class =
                    CASE
                        WHEN m.class <> "SK42(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK42(" + t.author + "," + t.title + ")"
                    END,
                    m.note =
                    CASE
                        WHEN m.note <> "SK43(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK43(" + t.author + "," + t.title + ")"
                    END,
                    m.annote =
                    CASE
                        WHEN m.annote <> t.school
                            THEN "Conflict detected!"
                        ELSE
                            t.school
                    END
            MERGE (m)-[:MISC_PUBLISHED{i} {{
                _id: "(MISC_PUBLISHED{i}:" + elementId(m) + "," + elementId(au) + ",{i})"
            }}]-(au)
            """)
            rules.append(rule9)
        # rule#10 using our framework
        for i in range(scale):
            rule10 = TransformationRule(f"""
            MATCH (t:MasterThesis{i})
            MERGE (au:_dummy {{
                _id: "(" + t.author + ",{i})"
            }})
            ON CREATE
                SET au:Author{i},
                    au.name = t.author
            ON MATCH
                SET au:Author{i},
                    au.name =
                    CASE
                        WHEN au.name <> t.author
                            THEN "Conflict detected!"
                        ELSE
                            t.author
                    END
            MERGE (m:_dummy {{
                _id: "(" + elementId(t) + ",{i})"
            }})
            ON CREATE
                SET m:Misc{i},
                    m.miscid = "SK44(" + t.author + "," + t.title + ")",
                    m.title = t.title,
                    m.howpub = "SK45(" + t.author + "," + t.title + ")",
                    m.confloc = "SK46(" + t.author + "," + t.title + ")",
                    m.year = t.year,
                    m.month = "SK47(" + t.author + "," + t.title + ")",
                    m.pages = "SK48(" + t.author + "," + t.title + ")",
                    m.vol = "SK49(" + t.author + "," + t.title + ")",
                    m.num = "SK50(" + t.author + "," + t.title + ")",
                    m.loc = "SK51(" + t.author + "," + t.title + ")",
                    m.class = "SK52(" + t.author + "," + t.title + ")",
                    m.note = "SK53(" + t.author + "," + t.title + ")",
                    m.annote = t.school
            ON MATCH
                SET m:Misc{i},
                    m.miscid =
                    CASE
                        WHEN m.miscid <> "SK44(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK44(" + t.author + "," + t.title + ")"
                    END,
                    m.title =
                    CASE
                        WHEN m.title <> t.title
                            THEN "Conflict detected!"
                        ELSE
                            t.title
                    END,
                    m.howpub =
                    CASE
                        WHEN m.howpub <> "SK45(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK45(" + t.author + "," + t.title + ")"
                    END,
                    m.confloc =
                    CASE
                        WHEN m.confloc <> "SK46(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK46(" + t.author + "," + t.title + ")"
                    END,
                    m.year =
                    CASE
                        WHEN m.year <> t.year
                            THEN "Conflict detected!"
                        ELSE
                            t.year
                    END,
                    m.month =
                    CASE
                        WHEN m.month <> "SK47(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK47(" + t.author + "," + t.title + ")"
                    END,
                    m.pages =
                    CASE
                        WHEN m.pages <> "SK48(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK48(" + t.author + "," + t.title + ")"
                    END,
                    m.vol =
                    CASE
                        WHEN m.vol <> "SK49(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK49(" + t.author + "," + t.title + ")"
                    END,
                    m.num =
                    CASE
                        WHEN m.num <> "SK50(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK50(" + t.author + "," + t.title + ")"
                    END,
                    m.loc =
                    CASE
                        WHEN m.loc <> "SK51(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK51(" + t.author + "," + t.title + ")"
                    END,
                    m.class =
                    CASE
                        WHEN m.class <> "SK52(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK52(" + t.author + "," + t.title + ")"
                    END,
                    m.note =
                    CASE
                        WHEN m.note <> "SK53(" + t.author + "," + t.title + ")"
                            THEN "Conflict detected!"
                        ELSE
                            "SK53(" + t.author + "," + t.title + ")"
                    END,
                    m.annote =
                    CASE
                        WHEN m.annote <> t.school
                            THEN "Conflict detected!"
                        ELSE
                            t.school
                    END
            MERGE (m)-[:MISC_PUBLISHED{i} {{
                _id: "(MISC_PUBLISHED{i}:" + elementId(m) + "," + elementId(au) + ",{i})"
            }}]-(au)
            """)
            rules.append(rule10)
        # transformation rules
        self.rules = rules