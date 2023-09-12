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
        app.dropIndex(dropArticleAuthor, stats)

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

class Amalgam1ToAmalgam3Plain(Amalgam1ToAmalgam3):
    def __init__(self, prefix, size = 100, lstring = 5):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule("""
        MATCH (pub:InProcPublished)
        MATCH (ip:InProceedings)
        WHERE pub.inproc = ip.inprocid
        MATCH (a:Author)
        WHERE pub.auth = a.authid 
        MERGE (x:_dummy { 
            _id: "(" + elementId(ip) + ")" 
        })
        SET x:TArticle,
            x.articleid = "SK1(" + ip.inprocid + ")",
            x.title = ip.title,
            x.vol = ip.vol,
            x.num = ip.num,
            x.pages = ip.pages,
            x.month = ip.month,
            x.year = ip.year,
            x.refkey = "SK2(" + ip.inprocid + ")",
            x.note = ip.note,
            x.remarks = "SK3(" + ip.inprocid + ")",
            x.refs = "SK4(" + ip.inprocid + ")",
            x.xxxrefs = "SK5(" + ip.inprocid + ")",
            x.fullxxxrefs = "SK6(" + ip.inprocid + ")",
            x.oldkey = "SK7(" + ip.inprocid + ")",
            x.abstract = "SK8(" + ip.inprocid + ")",
            x.preliminary = "SK9(" + ip.inprocid + ")"
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        SET y:Auth,
            y.authorid = a.authid,
            y.name = a.name
        MERGE (x)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule("""
        MATCH (ap:ArticlePublished)
        MATCH (art:Article)
        WHERE ap.article = art.articleid
        MATCH (a:Author)
        WHERE ap.auth = a.authid 
        MERGE (x:_dummy { 
            _id: "(" + elementId(art) + ")" 
        })
        SET x:TArticle,
            x.articleid = "SK11(" + art.articleid + ")",
            x.title = art.title,
            x.vol = art.vol,
            x.num = art.num,
            x.pages = art.pages,
            x.month = art.month,
            x.year = art.year,
            x.refkey = "SK12(" + art.articleid + ")",
            x.note = art.note,
            x.remarks = "SK13(" + art.articleid + ")",
            x.refs = "SK14(" + art.articleid + ")",
            x.xxxrefs = "SK15(" + art.articleid + ")",
            x.fullxxxrefs = "SK16(" + art.articleid + ")",
            x.oldkey = "SK17(" + art.articleid + ")",
            x.abstract = "SK18(" + art.articleid + ")",
            x.preliminary = "SK19(" + art.articleid + ")"
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        SET y:Auth,
            y.authorid = a.authid,
            y.name = a.name
        MERGE (x)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#3 using our framework
        rule3 = TransformationRule("""
        MATCH (tp:TechPublished)
        MATCH (t:TechReport)
        WHERE tp.tech = t.techid
        MATCH (a:Author)
        WHERE tp.auth = a.authid 
        MERGE (x:_dummy { 
            _id: "(" + elementId(t) + ")" 
        })
        SET x:TArticle,
            x.articleid = "SK21(" + t.techid + ")",
            x.title = t.title,
            x.vol = t.vol,
            x.num = t.num,
            x.pages = t.pages,
            x.month = t.month,
            x.year = t.year,
            x.refkey = "SK22(" + t.techid + ")",
            x.note = t.note,
            x.remarks = "SK23(" + t.techid + ")",
            x.refs = "SK24(" + t.techid + ")",
            x.xxxrefs = "SK25(" + t.techid + ")",
            x.fullxxxrefs = "SK26(" + t.techid + ")",
            x.oldkey = "SK27(" + t.techid + ")",
            x.abstract = "SK28(" + t.techid + ")",
            x.preliminary = "SK29(" + t.techid + ")"
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        SET y:Auth,
            y.authorid = a.authid,
            y.name = a.name
        MERGE (x)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#4 using our framework
        rule4 = TransformationRule("""
        MATCH (bp:BookPublished)
        MATCH (b:Book)
        WHERE bp.book = b.bookid
        MATCH (a:Author)
        WHERE bp.auth = a.authid 
        MERGE (x:_dummy { 
            _id: "(" + elementId(b) + ")" 
        })
        SET x:TArticle,
            x.articleid = "SK31(" + b.bookid + ")",
            x.title = b.title,
            x.vol = b.vol,
            x.num = b.num,
            x.pages = b.pages,
            x.month = b.month,
            x.year = b.year,
            x.refkey = "SK32(" + b.bookid + ")",
            x.note = b.note,
            x.remarks = "SK33(" + b.bookid + ")",
            x.refs = "SK34(" + b.bookid + ")",
            x.xxxrefs = "SK35(" + b.bookid + ")",
            x.fullxxxrefs = "SK36(" + b.bookid + ")",
            x.oldkey = "SK37(" + b.bookid + ")",
            x.abstract = "SK38(" + b.bookid + ")",
            x.preliminary = "SK39(" + b.bookid + ")"
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        SET y:Auth,
            y.authorid = a.authid,
            y.name = a.name
        MERGE (x)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#5 using our framework
        rule5 = TransformationRule("""
        MATCH (icp:InCollPublished)
        MATCH (i:InCollection)
        WHERE icp.col = i.colid
        MATCH (a:Author)
        WHERE icp.auth = a.authid 
        MERGE (x:_dummy { 
            _id: "(" + elementId(i) + ")" 
        })
        SET x:TArticle,
            x.articleid = "SK41(" + i.colid + ")",
            x.title = i.title,
            x.vol = i.vol,
            x.num = i.num,
            x.pages = i.pages,
            x.month = i.month,
            x.year = i.year,
            x.refkey = "SK42(" + i.colid + ")",
            x.note = i.note,
            x.remarks = "SK43(" + i.colid + ")",
            x.refs = "SK44(" + i.colid + ")",
            x.xxxrefs = "SK45(" + i.colid + ")",
            x.fullxxxrefs = "SK46(" + i.colid + ")",
            x.oldkey = "SK47(" + i.colid + ")",
            x.abstract = "SK48(" + i.colid + ")",
            x.preliminary = "SK49(" + i.colid + ")"
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        SET y:Auth,
            y.authorid = a.authid,
            y.name = a.name
        MERGE (x)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#6 using our framework
        rule6 = TransformationRule("""
        MATCH (mp:MiscPublished)
        MATCH (m:Misc)
        WHERE mp.misc = m.miscid
        MATCH (a:Author)
        WHERE mp.auth = a.authid 
        MERGE (x:_dummy { 
            _id: "(" + elementId(m) + ")" 
        })
        SET x:TArticle,
            x.articleid = "SK51(" + m.miscid + ")",
            x.title = m.title,
            x.vol = m.vol,
            x.num = m.num,
            x.pages = m.pages,
            x.month = m.month,
            x.year = m.year,
            x.refkey = "SK52(" + m.miscid + ")",
            x.note = m.note,
            x.remarks = "SK53(" + m.miscid + ")",
            x.refs = "SK54(" + m.miscid + ")",
            x.xxxrefs = "SK55(" + m.miscid + ")",
            x.fullxxxrefs = "SK56(" + m.miscid + ")",
            x.oldkey = "SK57(" + m.miscid + ")",
            x.abstract = "SK58(" + m.miscid + ")",
            x.preliminary = "SK59(" + m.miscid + ")"
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        SET y:Auth,
            y.authorid = a.authid,
            y.name = a.name
        MERGE (x)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#7 using our framework
        rule7 = TransformationRule("""
        MATCH (mp:ManualPublished)
        MATCH (m:Manual)
        WHERE mp.manual = m.manid
        MATCH (a:Author)
        WHERE mp.auth = a.authid 
        MERGE (x:_dummy { 
            _id: "(" + elementId(m) + ")" 
        })
        SET x:TArticle,
            x.articleid = "SK61(" + m.manid + ")",
            x.title = m.title,
            x.vol = m.vol,
            x.num = m.num,
            x.pages = m.pages,
            x.month = m.month,
            x.year = m.year,
            x.refkey = "SK62(" + m.manid + ")",
            x.note = m.note,
            x.remarks = "SK63(" + m.manid + ")",
            x.refs = "SK64(" + m.manid + ")",
            x.xxxrefs = "SK65(" + m.manid + ")",
            x.fullxxxrefs = "SK66(" + m.manid + ")",
            x.oldkey = "SK67(" + m.manid + ")",
            x.abstract = "SK68(" + m.manid + ")",
            x.preliminary = "SK69(" + m.manid + ")"
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        SET y:Auth,
            y.authorid = a.authid,
            y.name = a.name
        MERGE (x)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#8 using our framework
        rule8 = TransformationRule("""
        MATCH (a:Author)
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        SET y:Auth,
            y.authorid = a.authid,
            y.name = a.name
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
