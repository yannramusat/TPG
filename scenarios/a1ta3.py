import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class Amalgam1ToAmalgam3(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5):
        # csv#1
        rel_inproceedings_cmd = """MERGE (n:InProceedings {
            inprocid: row[1], 
            title: row[2],
            bktitle: row[3],
            year: row[4],
            month: row[5],
            pages: row[6],
            vol: row[7],
            num: row[8],
            loc: row[9],
            class: row[10],
            note: row[11],
            annote: row[12]
        })"""
        param_string = "a1ta3/inproceedings"+str(size)+"-"+str(lstring)+".csv"
        rel_inproceedings = InputRelation(os.path.join(prefix, param_string), rel_inproceedings_cmd)
        # csv#2
        rel_article_cmd = """MERGE (n:Article {
            articleid: row[1], 
            title: row[2],
            journal: row[3],
            year: row[4],
            month: row[5],
            pages: row[6],
            vol: row[7],
            num: row[8],
            loc: row[9],
            class: row[10],
            note: row[11],
            annote: row[12]
        })"""
        param_string = "a1ta3/article"+str(size)+"-"+str(lstring)+".csv"
        rel_article = InputRelation(os.path.join(prefix, param_string), rel_article_cmd)
        # csv#3
        rel_techreport_cmd = """MERGE (n:TechReport {
            techid: row[1],
            title: row[2],
            inst: row[3],
            year: row[4],
            month: row[5],
            pages: row[6],
            vol: row[7],
            num: row[8],
            loc: row[9],
            class: row[10],
            note: row[11],
            annote: row[12]
        })"""
        param_string = "a1ta3/techreport"+str(size)+"-"+str(lstring)+".csv"
        rel_techreport = InputRelation(os.path.join(prefix, param_string), rel_techreport_cmd)
        # csv#4
        rel_book_cmd = """MERGE (n:Book {
            bookid: row[1],
            title: row[2],
            publisher: row[3],
            year: row[4],
            month: row[5],
            pages: row[6],
            vol: row[7],
            num: row[8],
            loc: row[9],
            class: row[10],
            note: row[11],
            annote: row[12]
        })"""
        param_string = "a1ta3/book"+str(size)+"-"+str(lstring)+".csv"
        rel_book = InputRelation(os.path.join(prefix, param_string), rel_book_cmd)
        # csv#5
        rel_incollection_cmd = """MERGE (n:InCollection {
            colid: row[1],
            title: row[2],
            bktitle: row[3],
            year: row[4],
            month: row[5],
            pages: row[6],
            vol: row[7],
            num: row[8],
            loc: row[9],
            class: row[10],
            note: row[11],
            annote: row[12]
        })"""
        param_string = "a1ta3/incollection"+str(size)+"-"+str(lstring)+".csv"
        rel_incollection = InputRelation(os.path.join(prefix, param_string), rel_incollection_cmd)
        # csv#6
        rel_misc_cmd = """MERGE (n:Misc {
            miscid: row[1],
            title: row[2],
            howpub: row[3],
            confloc: row[4],
            year: row[5],
            month: row[6],
            pages: row[7],
            vol: row[8],
            num: row[9],
            loc: row[10],
            class: row[11],
            note: row[12],
            annote: row[13]
        })"""
        param_string = "a1ta3/misc"+str(size)+"-"+str(lstring)+".csv"
        rel_misc = InputRelation(os.path.join(prefix, param_string), rel_misc_cmd)
        # csv#7
        rel_manual_cmd = """MERGE (n:Manual {
            manid: row[1],
            title: row[2],
            org: row[3],
            year: row[4],
            month: row[5],
            pages: row[6],
            vol: row[7],
            num: row[8],
            loc: row[9],
            class: row[10],
            note: row[11],
            annote: row[12]
        })"""
        param_string = "a1ta3/manual"+str(size)+"-"+str(lstring)+".csv"
        rel_manual = InputRelation(os.path.join(prefix, param_string), rel_manual_cmd)
        # csv#8
        rel_author_cmd = """MERGE (n:Author {
            authid: row[1],
            name: row[2]
        })"""
        param_string = "a1ta3/author"+str(size)+"-"+str(lstring)+".csv"
        rel_author = InputRelation(os.path.join(prefix, param_string), rel_author_cmd)
        # csv#9
        rel_inprocpublished_cmd = """MERGE (n:InProcPublished {
            inproc: row[1],
            auth: row[2]
        })"""
        param_string = "a1ta3/inprocpublished"+str(size)+"-"+str(lstring)+".csv"
        rel_inprocpublished = InputRelation(os.path.join(prefix, param_string), rel_inprocpublished_cmd)
        # csv#10
        rel_articlepublished_cmd = """MERGE (n:ArticlePublished {
            article: row[1],
            auth: row[2]
        })"""
        param_string = "a1ta3/articlepublished"+str(size)+"-"+str(lstring)+".csv"
        rel_articlepublished = InputRelation(os.path.join(prefix, param_string), rel_articlepublished_cmd)
        # csv#11
        rel_techpublished_cmd = """MERGE (n:TechPublished {
            tech: row[1],
            auth: row[2]
        })"""
        param_string = "a1ta3/techpublished"+str(size)+"-"+str(lstring)+".csv"
        rel_techpublished = InputRelation(os.path.join(prefix, param_string), rel_techpublished_cmd)
        # csv#12
        rel_bookpublished_cmd = """MERGE (n:BookPublished {
            book: row[1],
            auth: row[2]
        })"""
        param_string = "a1ta3/bookpublished"+str(size)+"-"+str(lstring)+".csv"
        rel_bookpublished = InputRelation(os.path.join(prefix, param_string), rel_bookpublished_cmd)
        # csv#13
        rel_incollpublished_cmd = """MERGE (n:InCollPublished {
            col: row[1],
            auth: row[2]
        })"""
        param_string = "a1ta3/incollpublished"+str(size)+"-"+str(lstring)+".csv"
        rel_incollpublished = InputRelation(os.path.join(prefix, param_string), rel_incollpublished_cmd)
        # csv#14
        rel_miscpublished_cmd = """MERGE (n:MiscPublished {
            misc: row[1],
            auth: row[2]
        })"""
        param_string = "a1ta3/miscpublished"+str(size)+"-"+str(lstring)+".csv"
        rel_miscpublished = InputRelation(os.path.join(prefix, param_string), rel_miscpublished_cmd)
        # csv#15
        rel_manualpublished_cmd = """MERGE (n:ManualPublished {
            manual: row[1],
            auth: row[2]
        })"""
        param_string = "a1ta3/manualpublished"+str(size)+"-"+str(lstring)+".csv"
        rel_manualpublished = InputRelation(os.path.join(prefix, param_string), rel_manualpublished_cmd)

        # source schema
        self.schema = InputSchema([
            rel_inproceedings, 
            rel_article,
            rel_techreport,
            rel_book,
            rel_incollection,
            rel_misc,
            rel_manual,
            rel_author,
            rel_inprocpublished,
            rel_articlepublished,
            rel_techpublished,
            rel_bookpublished,
            rel_incollpublished,
            rel_miscpublished,
            rel_manualpublished
        ])
    
    def addRelIndexes(self, app, stats=False):
        # index on articleAuthor
        indexArticleAuthor = """
        CREATE INDEX idx_articleAuthor IF NOT EXISTS
        FOR ()-[r:ARTICLE_AUTHOR]-()
        ON (r._id)
        """
        app.addIndex(indexArticleAuthor, stats)
    
    def delRelIndexes(self, app, stats=False):
        # drop index on articleAuthor
        dropArticleAuthor = """
        DROP INDEX idx_articleAuthor IF EXISTS
        """

    def run(self, app, launches = 5, stats=False, nodeIndex=True, relIndex=True, shuffle=False):
        ttime = 0.0
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
            ttime += self.transform(app, stats=stats)
            if(nodeIndex):
                self.delNodeIndexes(app, stats=stats)
            if(relIndex):
                self.delRelIndexes(app, stats=stats)
        avg_time = ttime / launches
        if(stats):
            print(f"The transformation: {self}  averaged {avg_time} ms over {launches} run(s).")
        return avg_time 

class Amalgam1ToAmalgam3Plain(DBLPToAmalgam1):
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
