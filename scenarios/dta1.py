import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class DBLPToAmalgam1(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # csv#1
        rel_dinproceedings_cmd = """MERGE (n:DInProceedings {
            pid: row[1], 
            title: row[2],
            pages: row[3],
            booktitle: row[4],
            url: row[5],
            cdrom: row[6],
            month: row[7],
            year: row[8]
        })"""
        param_string = "dta1/dinproceedings"+str(size)+"-"+str(lstring)+".csv"
        rel_dinproceedings = InputRelation(os.path.join(prefix, param_string), rel_dinproceedings_cmd)
        # csv#2
        rel_darticle_cmd = """MERGE (n:DArticle {
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
        })"""
        param_string = "dta1/darticle"+str(size)+"-"+str(lstring)+".csv"
        rel_darticle = InputRelation(os.path.join(prefix, param_string), rel_darticle_cmd)
        # csv#3
        rel_pubauthors_cmd = """MERGE (n:PubAuthors {
            pid: row[1],
            author: row[2]
        })"""
        param_string = "dta1/pubauthors"+str(size)+"-"+str(lstring)+".csv"
        rel_pubauthors = InputRelation(os.path.join(prefix, param_string), rel_pubauthors_cmd)
        # csv#4
        rel_dbook_cmd = """MERGE (n:DBook {
            pid: row[1],
            editor: row[2],
            title: row[3],
            publisher: row[4],
            year: row[5],
            isbn: row[6],
            cdrom: row[7],
            citel: row[8],
            url: row[9]
        })"""
        param_string = "dta1/dbook"+str(size)+"-"+str(lstring)+".csv"
        rel_dbook = InputRelation(os.path.join(prefix, param_string), rel_dbook_cmd)
        # csv#5
        rel_masterthesis_cmd = """MERGE (n:MasterThesis {
            author: row[1],
            title: row[2],
            year: row[3],
            school: row[4]
        })"""
        param_string = "dta1/masterthesis"+str(size)+"-"+str(lstring)+".csv"
        rel_masterthesis = InputRelation(os.path.join(prefix, param_string), rel_masterthesis_cmd)
        # csv#6
        rel_phdthesis_cmd = """MERGE (n:PhDThesis {
            author: row[1],
            title: row[2],
            year: row[3],
            series: row[4],
            number: row[5],
            month: row[6],
            school: row[7],
            publisher: row[8],
            isbn: row[9]
        })"""
        param_string = "dta1/phdthesis"+str(size)+"-"+str(lstring)+".csv"
        rel_phdthesis = InputRelation(os.path.join(prefix, param_string), rel_phdthesis_cmd)
        # csv#7
        rel_www_cmd = """MERGE (n:WWW {
            pid: row[1],
            title: row[2],
            year: row[3],
            url: row[4]
        })"""
        param_string = "dta1/www"+str(size)+"-"+str(lstring)+".csv"
        rel_www = InputRelation(os.path.join(prefix, param_string), rel_www_cmd)

        # source schema
        self.schema = InputSchema([
            rel_dinproceedings, 
            rel_darticle,
            rel_pubauthors,
            rel_dbook,
            rel_masterthesis,
            rel_phdthesis,
            rel_www
        ])
    
    def addRelIndexes(self, app, stats=False):
        # index on inProcPublished
        indexInProcPublished = """
        CREATE INDEX idx_inProcPublished IF NOT EXISTS
        FOR ()-[r:IN_PROC_PUBLISHED]-()
        ON (r._id)
        """
        app.addIndex(indexInProcPublished, stats)
        # index on miscPublished
        indexMiscPublished = """
        CREATE INDEX idx_miscPublished IF NOT EXISTS
        FOR ()-[r:MISC_PUBLISHED]-()
        ON (r._id)
        """
        app.addIndex(indexMiscPublished, stats)
        # index on articlePublished
        indexArticlePublished = """
        CREATE INDEX idx_articlePublished IF NOT EXISTS
        FOR ()-[r:ARTICLE_PUBLISHED]-()
        ON (r._id)
        """
        app.addIndex(indexArticlePublished, stats)
        # index on bookPublished
        indexBookPublished = """
        CREATE INDEX idx_bookPublished IF NOT EXISTS
        FOR ()-[r:BOOK_PUBLISHED]-()
        ON (r._id)
        """
        app.addIndex(indexBookPublished, stats)
    
    def delRelIndexes(self, app, stats=False):
        # drop index on inProcPublished
        dropInProcPublished = """
        DROP INDEX idx_inProcPublished IF EXISTS
        """
        app.dropIndex(dropInProcPublished, stats)
        # drop index on miscPublished
        dropMiscPublished = """
        DROP INDEX idx_miscPublished IF EXISTS
        """
        app.dropIndex(dropMiscPublished, stats)
        # drop index on articlePublished
        dropArticlePublished = """
        DROP INDEX idx_articlePublished IF EXISTS
        """
        app.dropIndex(dropArticlePublished, stats)
        # drop index on bookPublished
        dropBookPublished = """
        DROP INDEX idx_bookPublished IF EXISTS
        """
        app.dropIndex(dropBookPublished, stats)

class DBLPToAmalgam1Plain(DBLPToAmalgam1):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (dip:DInProceedings)
        MERGE (x:_dummy { 
            _id: "(" + elementId(dip) + ")" 
        })
        SET x:InProceedings,
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
        # rule#2 using our framework
        rule2 = TransformationRule("""
        MATCH (dip:DInProceedings)
        MATCH (pa:PubAuthors)
        WHERE pa.pid = dip.pid
        MERGE (a:_dummy {
            _id: "(" + pa.author + ")"
        })
        SET a:Author,
            a.name = pa.author
        MERGE (x:_dummy { 
            _id: "(" + elementId(dip) + ")" 
        })
        SET x:InProceedings,
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
        MERGE (x)-[:IN_PROC_PUBLISHED {
            _id: "(IN_PROC_PUBLISHED:" + elementId(x) + "," + elementId(a) + ")"
        }]-(a)
        """)
        # rule#3 using our framework
        rule3 = TransformationRule("""
        MATCH (w:WWW)
        MERGE (m:_dummy {
            _id: "(" + elementId(w) + ")"
        })
        SET m:Misc,
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
        # rule#4 using our framework
        rule4 = TransformationRule("""
        MATCH (w:WWW)
        MATCH (pa:PubAuthors)
        WHERE pa.pid = w.pid
        MERGE (a:_dummy {
            _id: "(" + pa.author + ")"
        })
        SET a:Author,
            a.name = pa.author
        MERGE (m:_dummy {
            _id: "(" + elementId(w) + ")"
        })
        SET m:Misc,
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
        MERGE (m)-[:MISC_PUBLISHED {
            _id: "(MISC_PUBLISHED:" + elementId(m) + "," + elementId(a) + ")"
        }]-(a)
        """)
        # rule#5 using our framework
        rule5 = TransformationRule("""
        MATCH (da:DArticle)
        MERGE (a:_dummy { 
            _id: "(" + elementId(da) + ")" 
        })
        SET a:Article,
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
        # rule#6 using our framework
        rule6 = TransformationRule("""
        MATCH (da:DArticle)
        MATCH (pa:PubAuthors)
        WHERE pa.pid = da.pid
        MERGE (au:_dummy {
            _id: "(" + pa.author + ")"
        })
        SET au:Author,
            au.name = pa.author
        MERGE (a:_dummy { 
            _id: "(" + elementId(da) + ")" 
        })
        SET a:Article,
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
        MERGE (a)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(a) + "," + elementId(au) + ")"
        }]-(au)
        """)
        # rule#7 using our framework
        rule7 = TransformationRule("""
        MATCH (db:DBook)
        MERGE (b:_dummy { 
            _id: "(" + elementId(db) + ")" 
        })
        SET b:Book,
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
        # rule#8 using our framework
        rule8 = TransformationRule("""
        MATCH (db:DBook)
        MATCH (pa:PubAuthors)
        WHERE pa.pid = db.pid
        MERGE (au:_dummy {
            _id: "(" + pa.author + ")"
        })
        SET au:Author,
            au.name = pa.author
        MERGE (b:_dummy { 
            _id: "(" + elementId(db) + ")" 
        })
        SET b:Book,
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
        MERGE (b)-[:BOOK_PUBLISHED {
            _id: "(BOOK_PUBLISHED:" + elementId(b) + "," + elementId(au) + ")"
        }]-(au)
        """)
        # rule#9 using our framework
        rule9 = TransformationRule("""
        MATCH (t:PhDThesis)
        MERGE (au:_dummy {
            _id: "(" + t.author + ")"
        })
        SET au:Author,
            au.name = t.author
        MERGE (m:_dummy {
            _id: "(" + elementId(t) + ")"
        })
        SET m:Misc,
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
        MERGE (m)-[:MISC_PUBLISHED {
            _id: "(MISC_PUBLISHED:" + elementId(m) + "," + elementId(au) + ")"
        }]-(au)
        """)
        # rule#10 using our framework
        rule10 = TransformationRule("""
        MATCH (t:MasterThesis)
        MERGE (au:_dummy {
            _id: "(" + t.author + ")"
        })
        SET au:Author,
            au.name = t.author
        MERGE (m:_dummy {
            _id: "(" + elementId(t) + ")"
        })
        SET m:Misc,
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
        MERGE (m)-[:MISC_PUBLISHED {
            _id: "(MISC_PUBLISHED:" + elementId(m) + "," + elementId(au) + ")"
        }]-(au)
        """)

        # transformation rules
        self.rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9, rule10]

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

class DBLPToAmalgam1CDoverPlain(DBLPToAmalgam1Plain):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        TODO
        """)
        # transformation rules
        self.rules = [rule1]
