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

class Amalgam1ToAmalgam3SeparateIndexes(Amalgam1ToAmalgam3):
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(ip) + ")" 
        })
        SET x.articleid = "SK1(" + ip.inprocid + ")",
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
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        SET y.authorid = a.authid,
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(art) + ")" 
        })
        SET x.articleid = "SK11(" + art.articleid + ")",
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
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        SET y.authorid = a.authid,
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(t) + ")" 
        })
        SET x.articleid = "SK21(" + t.techid + ")",
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
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        SET y.authorid = a.authid,
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(b) + ")" 
        })
        SET x.articleid = "SK31(" + b.bookid + ")",
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
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        SET y.authorid = a.authid,
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(i) + ")" 
        })
        SET x.articleid = "SK41(" + i.colid + ")",
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
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        SET y.authorid = a.authid,
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(m) + ")" 
        })
        SET x.articleid = "SK51(" + m.miscid + ")",
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
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        SET y.authorid = a.authid,
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(m) + ")" 
        })
        SET x.articleid = "SK61(" + m.manid + ")",
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
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        SET y.authorid = a.authid,
            y.name = a.name
        MERGE (x)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#8 using our framework
        rule8 = TransformationRule("""
        MATCH (a:Author)
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        SET y.authorid = a.authid,
            y.name = a.name
        """)

        # transformation rules
        self.rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8]

    def addNodeIndexes(self, app, stats=False):
        # index on TArticle
        indexTArticle = """
        CREATE INDEX idx_TArticle IF NOT EXISTS
        FOR (n:TArticle)
        ON (n._id)
        """
        app.addIndex(indexTArticle, stats)

        # index on Auth
        indexAuth = """
        CREATE INDEX idx_Auth IF NOT EXISTS
        FOR (n:Auth)
        ON (n._id)
        """
        app.addIndex(indexAuth, stats)
    
    def delNodeIndexes(self, app, stats=False):
        # drop index on TArticle
        dropTArticle = """
        DROP INDEX idx_TArticle IF EXISTS
        """
        app.dropIndex(dropTArticle, stats)

        # drop index on Auth
        dropAuth = """
        DROP INDEX idx_Auth IF EXISTS
        """
        app.dropIndex(dropAuth, stats)

class Amalgam1ToAmalgam3CDoverPlain(Amalgam1ToAmalgam3Plain):
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
        ON CREATE
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
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK1(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK1(" + ip.inprocid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> ip.title
                        THEN "Conflict detected!"
                    ELSE
                        ip.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> ip.vol
                        THEN "Conflict detected!"
                    ELSE
                        ip.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> ip.num
                        THEN "Conflict detected!"
                    ELSE
                        ip.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> ip.pages
                        THEN "Conflict detected!"
                    ELSE
                        ip.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> ip.month
                        THEN "Conflict detected!"
                    ELSE
                        ip.month
                END,
                x.year =
                CASE
                    WHEN x.year <> ip.year
                        THEN "Conflict detected!"
                    ELSE
                        ip.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK2(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK2(" + ip.inprocid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> ip.note
                        THEN "Conflict detected!"
                    ELSE
                        ip.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK3(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK3(" + ip.inprocid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK4(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK4(" + ip.inprocid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK5(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK5(" + ip.inprocid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK6(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK6(" + ip.inprocid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK7(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK7(" + ip.inprocid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK8(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK8(" + ip.inprocid + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK9(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK9(" + ip.inprocid + ")"
                END
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        ON CREATE
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
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK11(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK11(" + art.articleid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> art.title
                        THEN "Conflict detected!"
                    ELSE
                        art.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> art.vol
                        THEN "Conflict detected!"
                    ELSE
                        art.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> art.num
                        THEN "Conflict detected!"
                    ELSE
                        art.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> art.pages
                        THEN "Conflict detected!"
                    ELSE
                        art.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> art.month
                        THEN "Conflict detected!"
                    ELSE
                        art.month
                END,
                x.year =
                CASE
                    WHEN x.year <> art.year
                        THEN "Conflict detected!"
                    ELSE
                        art.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK12(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK12(" + art.articleid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> art.note
                        THEN "Conflict detected!"
                    ELSE
                        art.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK13(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK13(" + art.articleid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK14(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK14(" + art.articleid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK15(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + art.articleid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK16(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK16(" + art.articleid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK17(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK17(" + art.articleid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK18(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK18(" + art.articleid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK19(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK19(" + art.articleid + ")"
                END
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        ON CREATE
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
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK21(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK21(" + t.techid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> t.title
                        THEN "Conflict detected!"
                    ELSE
                        t.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> t.vol
                        THEN "Conflict detected!"
                    ELSE
                        t.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> t.num
                        THEN "Conflict detected!"
                    ELSE
                        t.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> t.pages
                        THEN "Conflict detected!"
                    ELSE
                        t.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> t.month
                        THEN "Conflict detected!"
                    ELSE
                        t.month
                END,
                x.year =
                CASE
                    WHEN x.year <> t.year
                        THEN "Conflict detected!"
                    ELSE
                        t.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK22(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK22(" + t.techid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> t.note
                        THEN "Conflict detected!"
                    ELSE
                        t.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK23(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK23(" + t.techid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK24(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK24(" + t.techid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK25(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK25(" + t.techid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK26(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK26(" + t.techid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK27(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK27(" + t.techid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK28(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK28(" + t.techid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK29(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK29(" + t.techid + ")"
                END
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        ON CREATE
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
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK31(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK31(" + b.bookid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> b.title
                        THEN "Conflict detected!"
                    ELSE
                        b.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> b.vol
                        THEN "Conflict detected!"
                    ELSE
                        b.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> b.num
                        THEN "Conflict detected!"
                    ELSE
                        b.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> b.pages
                        THEN "Conflict detected!"
                    ELSE
                        b.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> b.month
                        THEN "Conflict detected!"
                    ELSE
                        b.month
                END,
                x.year =
                CASE
                    WHEN x.year <> b.year
                        THEN "Conflict detected!"
                    ELSE
                        b.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK32(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK32(" + b.bookid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> b.note
                        THEN "Conflict detected!"
                    ELSE
                        b.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK33(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK33(" + b.bookid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK34(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK34(" + b.bookid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK35(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK35(" + b.bookid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK36(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK36(" + b.bookid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK37(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK37(" + b.bookid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK38(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK38(" + b.bookid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK39(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK39(" + b.bookid + ")"
                END
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        ON CREATE
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
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK41(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK41(" + i.colid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> i.title
                        THEN "Conflict detected!"
                    ELSE
                        i.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> i.vol
                        THEN "Conflict detected!"
                    ELSE
                        i.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> i.num
                        THEN "Conflict detected!"
                    ELSE
                        i.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> i.pages
                        THEN "Conflict detected!"
                    ELSE
                        i.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> i.month
                        THEN "Conflict detected!"
                    ELSE
                        i.month
                END,
                x.year =
                CASE
                    WHEN x.year <> i.year
                        THEN "Conflict detected!"
                    ELSE
                        i.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK42(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK42(" + i.colid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> i.note
                        THEN "Conflict detected!"
                    ELSE
                        i.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK43(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK43(" + i.colid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK44(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK44(" + i.colid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK45(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK45(" + i.colid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK46(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK46(" + i.colid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK47(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK47(" + i.colid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK48(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK48(" + i.colid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK49(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK49(" + i.colid + ")"
                END
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        ON CREATE
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
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK51(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK51(" + m.miscid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> m.title
                        THEN "Conflict detected!"
                    ELSE
                        m.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> m.vol
                        THEN "Conflict detected!"
                    ELSE
                        m.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> m.num
                        THEN "Conflict detected!"
                    ELSE
                        m.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> m.pages
                        THEN "Conflict detected!"
                    ELSE
                        m.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> m.month
                        THEN "Conflict detected!"
                    ELSE
                        m.month
                END,
                x.year =
                CASE
                    WHEN x.year <> m.year
                        THEN "Conflict detected!"
                    ELSE
                        m.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK52(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK52(" + m.miscid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> m.note
                        THEN "Conflict detected!"
                    ELSE
                        m.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK53(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK53(" + m.miscid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK54(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK54(" + m.miscid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK55(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK55(" + m.miscid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK56(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK56(" + m.miscid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK57(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK57(" + m.miscid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK58(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK58(" + m.miscid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK59(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK59(" + m.miscid + ")"
                END 
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        ON CREATE
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
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK61(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK61(" + m.manid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> m.title
                        THEN "Conflict detected!"
                    ELSE
                        m.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> m.vol
                        THEN "Conflict detected!"
                    ELSE
                        m.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> m.num
                        THEN "Conflict detected!"
                    ELSE
                        m.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> m.pages
                        THEN "Conflict detected!"
                    ELSE
                        m.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> m.month
                        THEN "Conflict detected!"
                    ELSE
                        m.month
                END,
                x.year =
                CASE
                    WHEN x.year <> m.year
                        THEN "Conflict detected!"
                    ELSE
                        m.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK62(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK62(" + m.manid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> m.note
                        THEN "Conflict detected!"
                    ELSE
                        m.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK63(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK63(" + m.manid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK64(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK64(" + m.manid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK65(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK65(" + m.manid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK66(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK66(" + m.manid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK67(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK67(" + m.manid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK68(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK68(" + m.manid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK69(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK69(" + m.manid + ")"
                END 
        MERGE (y:_dummy {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
        """)

        # transformation rules
        self.rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8]

class Amalgam1ToAmalgam3CDoverSI(Amalgam1ToAmalgam3SeparateIndexes):
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(ip) + ")" 
        })
        ON CREATE
            SET x.articleid = "SK1(" + ip.inprocid + ")",
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
        ON MATCH
            SET x.articleid =
                CASE
                    WHEN x.articleid <> "SK1(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK1(" + ip.inprocid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> ip.title
                        THEN "Conflict detected!"
                    ELSE
                        ip.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> ip.vol
                        THEN "Conflict detected!"
                    ELSE
                        ip.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> ip.num
                        THEN "Conflict detected!"
                    ELSE
                        ip.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> ip.pages
                        THEN "Conflict detected!"
                    ELSE
                        ip.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> ip.month
                        THEN "Conflict detected!"
                    ELSE
                        ip.month
                END,
                x.year =
                CASE
                    WHEN x.year <> ip.year
                        THEN "Conflict detected!"
                    ELSE
                        ip.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK2(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK2(" + ip.inprocid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> ip.note
                        THEN "Conflict detected!"
                    ELSE
                        ip.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK3(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK3(" + ip.inprocid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK4(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK4(" + ip.inprocid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK5(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK5(" + ip.inprocid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK6(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK6(" + ip.inprocid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK7(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK7(" + ip.inprocid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK8(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK8(" + ip.inprocid + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK9(" + ip.inprocid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK9(" + ip.inprocid + ")"
                END
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(art) + ")" 
        })
        ON CREATE
            SET x.articleid = "SK11(" + art.articleid + ")",
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
        ON MATCH
            SET x.articleid =
                CASE
                    WHEN x.articleid <> "SK11(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK11(" + art.articleid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> art.title
                        THEN "Conflict detected!"
                    ELSE
                        art.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> art.vol
                        THEN "Conflict detected!"
                    ELSE
                        art.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> art.num
                        THEN "Conflict detected!"
                    ELSE
                        art.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> art.pages
                        THEN "Conflict detected!"
                    ELSE
                        art.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> art.month
                        THEN "Conflict detected!"
                    ELSE
                        art.month
                END,
                x.year =
                CASE
                    WHEN x.year <> art.year
                        THEN "Conflict detected!"
                    ELSE
                        art.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK12(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK12(" + art.articleid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> art.note
                        THEN "Conflict detected!"
                    ELSE
                        art.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK13(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK13(" + art.articleid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK14(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK14(" + art.articleid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK15(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + art.articleid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK16(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK16(" + art.articleid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK17(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK17(" + art.articleid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK18(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK18(" + art.articleid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK19(" + art.articleid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK19(" + art.articleid + ")"
                END
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(t) + ")" 
        })
        ON CREATE
            SET x.articleid = "SK21(" + t.techid + ")",
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
        ON MATCH
            SET x.articleid =
                CASE
                    WHEN x.articleid <> "SK21(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK21(" + t.techid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> t.title
                        THEN "Conflict detected!"
                    ELSE
                        t.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> t.vol
                        THEN "Conflict detected!"
                    ELSE
                        t.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> t.num
                        THEN "Conflict detected!"
                    ELSE
                        t.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> t.pages
                        THEN "Conflict detected!"
                    ELSE
                        t.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> t.month
                        THEN "Conflict detected!"
                    ELSE
                        t.month
                END,
                x.year =
                CASE
                    WHEN x.year <> t.year
                        THEN "Conflict detected!"
                    ELSE
                        t.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK22(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK22(" + t.techid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> t.note
                        THEN "Conflict detected!"
                    ELSE
                        t.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK23(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK23(" + t.techid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK24(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK24(" + t.techid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK25(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK25(" + t.techid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK26(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK26(" + t.techid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK27(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK27(" + t.techid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK28(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK28(" + t.techid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK29(" + t.techid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK29(" + t.techid + ")"
                END
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(b) + ")" 
        })
        ON CREATE
            SET x.articleid = "SK31(" + b.bookid + ")",
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
        ON MATCH
            SET x.articleid =
                CASE
                    WHEN x.articleid <> "SK31(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK31(" + b.bookid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> b.title
                        THEN "Conflict detected!"
                    ELSE
                        b.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> b.vol
                        THEN "Conflict detected!"
                    ELSE
                        b.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> b.num
                        THEN "Conflict detected!"
                    ELSE
                        b.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> b.pages
                        THEN "Conflict detected!"
                    ELSE
                        b.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> b.month
                        THEN "Conflict detected!"
                    ELSE
                        b.month
                END,
                x.year =
                CASE
                    WHEN x.year <> b.year
                        THEN "Conflict detected!"
                    ELSE
                        b.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK32(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK32(" + b.bookid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> b.note
                        THEN "Conflict detected!"
                    ELSE
                        b.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK33(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK33(" + b.bookid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK34(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK34(" + b.bookid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK35(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK35(" + b.bookid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK36(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK36(" + b.bookid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK37(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK37(" + b.bookid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK38(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK38(" + b.bookid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK39(" + b.bookid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK39(" + b.bookid + ")"
                END
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(i) + ")" 
        })
        ON CREATE
            SET x.articleid = "SK41(" + i.colid + ")",
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
        ON MATCH
            SET x.articleid =
                CASE
                    WHEN x.articleid <> "SK41(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK41(" + i.colid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> i.title
                        THEN "Conflict detected!"
                    ELSE
                        i.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> i.vol
                        THEN "Conflict detected!"
                    ELSE
                        i.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> i.num
                        THEN "Conflict detected!"
                    ELSE
                        i.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> i.pages
                        THEN "Conflict detected!"
                    ELSE
                        i.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> i.month
                        THEN "Conflict detected!"
                    ELSE
                        i.month
                END,
                x.year =
                CASE
                    WHEN x.year <> i.year
                        THEN "Conflict detected!"
                    ELSE
                        i.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK42(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK42(" + i.colid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> i.note
                        THEN "Conflict detected!"
                    ELSE
                        i.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK43(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK43(" + i.colid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK44(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK44(" + i.colid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK45(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK45(" + i.colid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK46(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK46(" + i.colid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK47(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK47(" + i.colid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK48(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK48(" + i.colid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK49(" + i.colid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK49(" + i.colid + ")"
                END
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(m) + ")" 
        })
        ON CREATE
            SET x.articleid = "SK51(" + m.miscid + ")",
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
        ON MATCH
            SET x.articleid =
                CASE
                    WHEN x.articleid <> "SK51(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK51(" + m.miscid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> m.title
                        THEN "Conflict detected!"
                    ELSE
                        m.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> m.vol
                        THEN "Conflict detected!"
                    ELSE
                        m.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> m.num
                        THEN "Conflict detected!"
                    ELSE
                        m.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> m.pages
                        THEN "Conflict detected!"
                    ELSE
                        m.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> m.month
                        THEN "Conflict detected!"
                    ELSE
                        m.month
                END,
                x.year =
                CASE
                    WHEN x.year <> m.year
                        THEN "Conflict detected!"
                    ELSE
                        m.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK52(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK52(" + m.miscid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> m.note
                        THEN "Conflict detected!"
                    ELSE
                        m.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK53(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK53(" + m.miscid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK54(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK54(" + m.miscid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK55(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK55(" + m.miscid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK56(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK56(" + m.miscid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK57(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK57(" + m.miscid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK58(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK58(" + m.miscid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK59(" + m.miscid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK59(" + m.miscid + ")"
                END 
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
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
        MERGE (x:TArticle { 
            _id: "(" + elementId(m) + ")" 
        })
        ON CREATE
            SET x.articleid = "SK61(" + m.manid + ")",
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
        ON MATCH
            SET x.articleid =
                CASE
                    WHEN x.articleid <> "SK61(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK61(" + m.manid + ")"
                END,
                x.title =
                CASE
                    WHEN x.title <> m.title
                        THEN "Conflict detected!"
                    ELSE
                        m.title
                END,
                x.vol =
                CASE
                    WHEN x.vol <> m.vol
                        THEN "Conflict detected!"
                    ELSE
                        m.vol
                END,
                x.num =
                CASE
                    WHEN x.num <> m.num
                        THEN "Conflict detected!"
                    ELSE
                        m.num
                END,
                x.pages =
                CASE
                    WHEN x.pages <> m.pages
                        THEN "Conflict detected!"
                    ELSE
                        m.pages
                END,
                x.month =
                CASE
                    WHEN x.month <> m.month
                        THEN "Conflict detected!"
                    ELSE
                        m.month
                END,
                x.year =
                CASE
                    WHEN x.year <> m.year
                        THEN "Conflict detected!"
                    ELSE
                        m.year
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK62(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK62(" + m.manid + ")"
                END,
                x.note =
                CASE
                    WHEN x.note <> m.note
                        THEN "Conflict detected!"
                    ELSE
                        m.note
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK63(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK63(" + m.manid + ")"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK64(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK64(" + m.manid + ")"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK65(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK65(" + m.manid + ")"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK66(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK66(" + m.manid + ")"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK67(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK67(" + m.manid + ")"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK68(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK68(" + m.manid  + ")"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK69(" + m.manid + ")"
                        THEN "Conflict detected!"
                    ELSE
                        "SK69(" + m.manid + ")"
                END 
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
        MERGE (x)-[:ARTICLE_PUBLISHED {
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }]-(y)
        """)
        # rule#8 using our framework
        rule8 = TransformationRule("""
        MATCH (a:Author)
        MERGE (y:Auth {
            _id: "(" + a.authid + ")"
        })
        ON CREATE
            SET y.authorid = a.authid,
                y.name = a.name
        ON MATCH
            SET y.authorid =
                CASE
                    WHEN y.authorid <> a.authid
                        THEN "Conflict detected!"
                    ELSE
                        a.authid
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name
                        THEN "Conflict detected!"
                    ELSE
                        a.name
                END 
        """)

        # transformation rules
        self.rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8]

class Amalgam1ToAmalgam3RandomConflicts(Amalgam1ToAmalgam3CDoverPlain):
    def __init__(self, prefix, size = 100, lstring = 5, prob_conflict = 50):
        # input schema
        super().__init__(prefix, size, lstring)

        # rule#1 using our framework
        rule1 = TransformationRule(f"""
        MATCH (pub:InProcPublished)
        MATCH (ip:InProceedings)
        WHERE pub.inproc = ip.inprocid
        MATCH (a:Author)
        WHERE pub.auth = a.authid 
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(ip) + ")" 
        }})
        ON CREATE
            SET x:TArticle,
                x.articleid = "SK1(" + ip.inprocid + ")" + "1",
                x.title = ip.title + "1",
                x.vol = ip.vol + "1",
                x.num = ip.num + "1",
                x.pages = ip.pages + "1",
                x.month = ip.month + "1",
                x.year = ip.year + "1",
                x.refkey = "SK2(" + ip.inprocid + ")" + "1",
                x.note = ip.note + "1",
                x.remarks = "SK3(" + ip.inprocid + ")" + "1",
                x.refs = "SK4(" + ip.inprocid + ")" + "1",
                x.xxxrefs = "SK5(" + ip.inprocid + ")" + "1",
                x.fullxxxrefs = "SK6(" + ip.inprocid + ")" + "1",
                x.oldkey = "SK7(" + ip.inprocid + ")" + "1",
                x.abstract = "SK8(" + ip.inprocid + ")" + "1",
                x.preliminary = "SK9(" + ip.inprocid + ")" + "1"
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK1(" + ip.inprocid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK1(" + ip.inprocid + ")" + "1"
                END,
                x.title =
                CASE
                    WHEN x.title <> ip.title + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ip.title + "1"
                END,
                x.vol =
                CASE
                    WHEN x.vol <> ip.vol + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ip.vol + "1"
                END,
                x.num =
                CASE
                    WHEN x.num <> ip.num + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ip.num + "1"
                END,
                x.pages =
                CASE
                    WHEN x.pages <> ip.pages + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ip.pages + "1"
                END,
                x.month =
                CASE
                    WHEN x.month <> ip.month + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ip.month + "1"
                END,
                x.year =
                CASE
                    WHEN x.year <> ip.year + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ip.year + "1"
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK2(" + ip.inprocid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK2(" + ip.inprocid + ")" + "1"
                END,
                x.note =
                CASE
                    WHEN x.note <> ip.note + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        ip.note + "1"
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK3(" + ip.inprocid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK3(" + ip.inprocid + ")" + "1"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK4(" + ip.inprocid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK4(" + ip.inprocid + ")" + "1"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK5(" + ip.inprocid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK5(" + ip.inprocid + ")" + "1"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK6(" + ip.inprocid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK6(" + ip.inprocid + ")" + "1"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK7(" + ip.inprocid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK7(" + ip.inprocid + ")" + "1"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK8(" + ip.inprocid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK8(" + ip.inprocid + ")" + "1"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK9(" + ip.inprocid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK9(" + ip.inprocid + ")" + "1"
                END
        MERGE (y:_dummy {{
            _id: "(" + a.authid + ")"
        }})
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid + "1",
                y.name = a.name + "1"
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.authid + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.name + "1"
                END 
        MERGE (x)-[:ARTICLE_PUBLISHED {{
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#2 using our framework
        rule2 = TransformationRule(f"""
        MATCH (ap:ArticlePublished)
        MATCH (art:Article)
        WHERE ap.article = art.articleid
        MATCH (a:Author)
        WHERE ap.auth = a.authid 
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(art) + ")" 
        }})
        ON CREATE
            SET x:TArticle,
                x.articleid = "SK11(" + art.articleid + ")" + "1",
                x.title = art.title + "1",
                x.vol = art.vol + "1",
                x.num = art.num + "1",
                x.pages = art.pages + "1",
                x.month = art.month + "1",
                x.year = art.year + "1",
                x.refkey = "SK12(" + art.articleid + ")" + "1",
                x.note = art.note + "1",
                x.remarks = "SK13(" + art.articleid + ")" + "1",
                x.refs = "SK14(" + art.articleid + ")" + "1",
                x.xxxrefs = "SK15(" + art.articleid + ")" + "1",
                x.fullxxxrefs = "SK16(" + art.articleid + ")" + "1",
                x.oldkey = "SK17(" + art.articleid + ")" + "1",
                x.abstract = "SK18(" + art.articleid + ")" + "1",
                x.preliminary = "SK19(" + art.articleid + ")" + "1"
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK11(" + art.articleid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK11(" + art.articleid + ")" + "1"
                END,
                x.title =
                CASE
                    WHEN x.title <> art.title + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        art.title + "1"
                END,
                x.vol =
                CASE
                    WHEN x.vol <> art.vol + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        art.vol + "1"
                END,
                x.num =
                CASE
                    WHEN x.num <> art.num + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        art.num + "1"
                END,
                x.pages =
                CASE
                    WHEN x.pages <> art.pages + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        art.pages + "1"
                END,
                x.month =
                CASE
                    WHEN x.month <> art.month + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        art.month + "1"
                END,
                x.year =
                CASE
                    WHEN x.year <> art.year + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        art.year + "1"
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK12(" + art.articleid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK12(" + art.articleid + ")" + "1"
                END,
                x.note =
                CASE
                    WHEN x.note <> art.note + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        art.note + "1"
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK13(" + art.articleid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK13(" + art.articleid + ")" + "1"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK14(" + art.articleid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK14(" + art.articleid + ")" + "1"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK15(" + art.articleid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK15(" + art.articleid + ")" + "1"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK16(" + art.articleid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK16(" + art.articleid + ")" + "1"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK17(" + art.articleid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK17(" + art.articleid + ")" + "1"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK18(" + art.articleid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK18(" + art.articleid  + ")" + "1"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK19(" + art.articleid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK19(" + art.articleid + ")" + "1"
                END
        MERGE (y:_dummy {{
            _id: "(" + a.authid + ")"
        }})
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid + "1",
                y.name = a.name + "1"
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.authid + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.name + "1"
                END 
        MERGE (x)-[:ARTICLE_PUBLISHED {{
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#3 using our framework
        rule3 = TransformationRule(f"""
        MATCH (tp:TechPublished)
        MATCH (t:TechReport)
        WHERE tp.tech = t.techid
        MATCH (a:Author)
        WHERE tp.auth = a.authid 
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(t) + ")" 
        }})
        ON CREATE
            SET x:TArticle,
                x.articleid = "SK21(" + t.techid + ")" + "1",
                x.title = t.title + "1",
                x.vol = t.vol + "1",
                x.num = t.num + "1",
                x.pages = t.pages + "1",
                x.month = t.month + "1",
                x.year = t.year + "1",
                x.refkey = "SK22(" + t.techid + ")" + "1",
                x.note = t.note + "1",
                x.remarks = "SK23(" + t.techid + ")" + "1",
                x.refs = "SK24(" + t.techid + ")" + "1",
                x.xxxrefs = "SK25(" + t.techid + ")" + "1",
                x.fullxxxrefs = "SK26(" + t.techid + ")" + "1",
                x.oldkey = "SK27(" + t.techid + ")" + "1",
                x.abstract = "SK28(" + t.techid + ")" + "1",
                x.preliminary = "SK29(" + t.techid + ")" + "1"
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK21(" + t.techid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK21(" + t.techid + ")" + "1"
                END,
                x.title =
                CASE
                    WHEN x.title <> t.title + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        t.title + "1"
                END,
                x.vol =
                CASE
                    WHEN x.vol <> t.vol + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        t.vol + "1"
                END,
                x.num =
                CASE
                    WHEN x.num <> t.num + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        t.num + "1"
                END,
                x.pages =
                CASE
                    WHEN x.pages <> t.pages + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        t.pages + "1"
                END,
                x.month =
                CASE
                    WHEN x.month <> t.month + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        t.month + "1"
                END,
                x.year =
                CASE
                    WHEN x.year <> t.year + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        t.year + "1"
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK22(" + t.techid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK22(" + t.techid + ")" + "1"
                END,
                x.note =
                CASE
                    WHEN x.note <> t.note + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        t.note + "1"
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK23(" + t.techid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK23(" + t.techid + ")" + "1"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK24(" + t.techid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK24(" + t.techid + ")" + "1"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK25(" + t.techid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK25(" + t.techid + ")" + "1"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK26(" + t.techid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK26(" + t.techid + ")" + "1"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK27(" + t.techid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK27(" + t.techid + ")" + "1"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK28(" + t.techid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK28(" + t.techid  + ")" + "1"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK29(" + t.techid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK29(" + t.techid + ")" + "1"
                END
        MERGE (y:_dummy {{
            _id: "(" + a.authid + ")"
        }})
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid + "1",
                y.name = a.name + "1"
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.authid + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.name + "1"
                END 
        MERGE (x)-[:ARTICLE_PUBLISHED {{
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#4 using our framework
        rule4 = TransformationRule(f"""
        MATCH (bp:BookPublished)
        MATCH (b:Book)
        WHERE bp.book = b.bookid
        MATCH (a:Author)
        WHERE bp.auth = a.authid 
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(b) + ")" 
        }})
        ON CREATE
            SET x:TArticle,
                x.articleid = "SK31(" + b.bookid + ")" + "1",
                x.title = b.title + "1",
                x.vol = b.vol + "1",
                x.num = b.num + "1",
                x.pages = b.pages + "1",
                x.month = b.month + "1",
                x.year = b.year + "1",
                x.refkey = "SK32(" + b.bookid + ")" + "1",
                x.note = b.note + "1",
                x.remarks = "SK33(" + b.bookid + ")" + "1",
                x.refs = "SK34(" + b.bookid + ")" + "1",
                x.xxxrefs = "SK35(" + b.bookid + ")" + "1",
                x.fullxxxrefs = "SK36(" + b.bookid + ")" + "1",
                x.oldkey = "SK37(" + b.bookid + ")" + "1",
                x.abstract = "SK38(" + b.bookid + ")" + "1",
                x.preliminary = "SK39(" + b.bookid + ")" + "1"
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK31(" + b.bookid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK31(" + b.bookid + ")" + "1"
                END,
                x.title =
                CASE
                    WHEN x.title <> b.title + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        b.title + "1"
                END,
                x.vol =
                CASE
                    WHEN x.vol <> b.vol + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        b.vol + "1"
                END,
                x.num =
                CASE
                    WHEN x.num <> b.num + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        b.num + "1"
                END,
                x.pages =
                CASE
                    WHEN x.pages <> b.pages + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        b.pages + "1"
                END,
                x.month =
                CASE
                    WHEN x.month <> b.month + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        b.month + "1"
                END,
                x.year =
                CASE
                    WHEN x.year <> b.year + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        b.year + "1"
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK32(" + b.bookid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK32(" + b.bookid + ")" + "1"
                END,
                x.note =
                CASE
                    WHEN x.note <> b.note + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        b.note + "1"
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK33(" + b.bookid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK33(" + b.bookid + ")" + "1"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK34(" + b.bookid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK34(" + b.bookid + ")" + "1"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK35(" + b.bookid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK35(" + b.bookid + ")" + "1"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK36(" + b.bookid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK36(" + b.bookid + ")" + "1"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK37(" + b.bookid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK37(" + b.bookid + ")" + "1"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK38(" + b.bookid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK38(" + b.bookid  + ")" + "1"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK39(" + b.bookid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK39(" + b.bookid + ")" + "1"
                END
        MERGE (y:_dummy {{
            _id: "(" + a.authid + ")"
        }})
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid + "1",
                y.name = a.name + "1"
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.authid + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.name + "1"
                END 
        MERGE (x)-[:ARTICLE_PUBLISHED {{
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#5 using our framework
        rule5 = TransformationRule(f"""
        MATCH (icp:InCollPublished)
        MATCH (i:InCollection)
        WHERE icp.col = i.colid
        MATCH (a:Author)
        WHERE icp.auth = a.authid 
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(i) + ")" 
        }})
        ON CREATE
            SET x:TArticle,
                x.articleid = "SK41(" + i.colid + ")" + "1",
                x.title = i.title + "1",
                x.vol = i.vol + "1",
                x.num = i.num + "1",
                x.pages = i.pages + "1",
                x.month = i.month + "1",
                x.year = i.year + "1",
                x.refkey = "SK42(" + i.colid + ")" + "1",
                x.note = i.note + "1",
                x.remarks = "SK43(" + i.colid + ")" + "1",
                x.refs = "SK44(" + i.colid + ")" + "1",
                x.xxxrefs = "SK45(" + i.colid + ")" + "1",
                x.fullxxxrefs = "SK46(" + i.colid + ")" + "1",
                x.oldkey = "SK47(" + i.colid + ")" + "1",
                x.abstract = "SK48(" + i.colid + ")" + "1",
                x.preliminary = "SK49(" + i.colid + ")" + "1"
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK41(" + i.colid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK41(" + i.colid + ")" + "1"
                END,
                x.title =
                CASE
                    WHEN x.title <> i.title + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        i.title + "1"
                END,
                x.vol =
                CASE
                    WHEN x.vol <> i.vol + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        i.vol + "1"
                END,
                x.num =
                CASE
                    WHEN x.num <> i.num + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        i.num + "1"
                END,
                x.pages =
                CASE
                    WHEN x.pages <> i.pages + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        i.pages + "1"
                END,
                x.month =
                CASE
                    WHEN x.month <> i.month + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        i.month + "1"
                END,
                x.year =
                CASE
                    WHEN x.year <> i.year + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        i.year + "1"
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK42(" + i.colid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK42(" + i.colid + ")" + "1"
                END,
                x.note =
                CASE
                    WHEN x.note <> i.note + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        i.note + "1"
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK43(" + i.colid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK43(" + i.colid + ")" + "1"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK44(" + i.colid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK44(" + i.colid + ")" + "1"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK45(" + i.colid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK45(" + i.colid + ")" + "1"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK46(" + i.colid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK46(" + i.colid + ")" + "1"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK47(" + i.colid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK47(" + i.colid + ")" + "1"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK48(" + i.colid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK48(" + i.colid  + ")" + "1"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK49(" + i.colid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK49(" + i.colid + ")" + "1"
                END
        MERGE (y:_dummy {{
            _id: "(" + a.authid + ")"
        }})
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid + "1",
                y.name = a.name + "1"
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.authid + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.name + "1"
                END 
        MERGE (x)-[:ARTICLE_PUBLISHED {{
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#6 using our framework
        rule6 = TransformationRule(f"""
        MATCH (mp:MiscPublished)
        MATCH (m:Misc)
        WHERE mp.misc = m.miscid
        MATCH (a:Author)
        WHERE mp.auth = a.authid 
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(m) + ")" 
        }})
        ON CREATE
            SET x:TArticle,
                x.articleid = "SK51(" + m.miscid + ")" + "1",
                x.title = m.title + "1",
                x.vol = m.vol + "1",
                x.num = m.num + "1",
                x.pages = m.pages + "1",
                x.month = m.month + "1",
                x.year = m.year + "1",
                x.refkey = "SK52(" + m.miscid + ")" + "1",
                x.note = m.note + "1",
                x.remarks = "SK53(" + m.miscid + ")" + "1",
                x.refs = "SK54(" + m.miscid + ")" + "1",
                x.xxxrefs = "SK55(" + m.miscid + ")" + "1",
                x.fullxxxrefs = "SK56(" + m.miscid + ")" + "1",
                x.oldkey = "SK57(" + m.miscid + ")" + "1",
                x.abstract = "SK58(" + m.miscid + ")" + "1",
                x.preliminary = "SK59(" + m.miscid + ")" + "1"
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK51(" + m.miscid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK51(" + m.miscid + ")" + "1"
                END,
                x.title =
                CASE
                    WHEN x.title <> m.title + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.title + "1"
                END,
                x.vol =
                CASE
                    WHEN x.vol <> m.vol + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.vol + "1"
                END,
                x.num =
                CASE
                    WHEN x.num <> m.num + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.num + "1"
                END,
                x.pages =
                CASE
                    WHEN x.pages <> m.pages + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.pages + "1"
                END,
                x.month =
                CASE
                    WHEN x.month <> m.month + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.month + "1"
                END,
                x.year =
                CASE
                    WHEN x.year <> m.year + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.year + "1"
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK52(" + m.miscid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK52(" + m.miscid + ")" + "1"
                END,
                x.note =
                CASE
                    WHEN x.note <> m.note + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.note + "1"
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK53(" + m.miscid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK53(" + m.miscid + ")" + "1"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK54(" + m.miscid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK54(" + m.miscid + ")" + "1"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK55(" + m.miscid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK55(" + m.miscid + ")" + "1"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK56(" + m.miscid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK56(" + m.miscid + ")" + "1"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK57(" + m.miscid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK57(" + m.miscid + ")" + "1"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK58(" + m.miscid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK58(" + m.miscid  + ")" + "1"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK59(" + m.miscid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK59(" + m.miscid + ")" + "1"
                END 
        MERGE (y:_dummy {{
            _id: "(" + a.authid + ")"
        }})
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid + "1",
                y.name = a.name + "1"
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.authid + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.name + "1"
                END 
        MERGE (x)-[:ARTICLE_PUBLISHED {{
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#7 using our framework
        rule7 = TransformationRule(f"""
        MATCH (mp:ManualPublished)
        MATCH (m:Manual)
        WHERE mp.manual = m.manid
        MATCH (a:Author)
        WHERE mp.auth = a.authid 
        MERGE (x:_dummy {{ 
            _id: "(" + elementId(m) + ")" 
        }})
        ON CREATE
            SET x:TArticle,
                x.articleid = "SK61(" + m.manid + ")" + "1",
                x.title = m.title + "1",
                x.vol = m.vol + "1",
                x.num = m.num + "1",
                x.pages = m.pages + "1",
                x.month = m.month + "1",
                x.year = m.year + "1",
                x.refkey = "SK62(" + m.manid + ")" + "1",
                x.note = m.note + "1",
                x.remarks = "SK63(" + m.manid + ")" + "1",
                x.refs = "SK64(" + m.manid + ")" + "1",
                x.xxxrefs = "SK65(" + m.manid + ")" + "1",
                x.fullxxxrefs = "SK66(" + m.manid + ")" + "1",
                x.oldkey = "SK67(" + m.manid + ")" + "1",
                x.abstract = "SK68(" + m.manid + ")" + "1",
                x.preliminary = "SK69(" + m.manid + ")" + "1"
        ON MATCH
            SET x:TArticle,
                x.articleid =
                CASE
                    WHEN x.articleid <> "SK61(" + m.manid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK61(" + m.manid + ")" + "1"
                END,
                x.title =
                CASE
                    WHEN x.title <> m.title + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.title + "1"
                END,
                x.vol =
                CASE
                    WHEN x.vol <> m.vol + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.vol + "1"
                END,
                x.num =
                CASE
                    WHEN x.num <> m.num + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.num + "1"
                END,
                x.pages =
                CASE
                    WHEN x.pages <> m.pages + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.pages + "1"
                END,
                x.month =
                CASE
                    WHEN x.month <> m.month + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.month + "1"
                END,
                x.year =
                CASE
                    WHEN x.year <> m.year + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.year + "1"
                END,
                x.refkey =
                CASE
                    WHEN x.refkey <> "SK62(" + m.manid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK62(" + m.manid + ")" + "1"
                END,
                x.note =
                CASE
                    WHEN x.note <> m.note + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        m.note + "1"
                END,
                x.remarks =
                CASE
                    WHEN x.remarks <> "SK63(" + m.manid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK63(" + m.manid + ")" + "1"
                END,
                x.refs =
                CASE
                    WHEN x.refs <> "SK64(" + m.manid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK64(" + m.manid + ")" + "1"
                END,
                x.xxxrefs =
                CASE
                    WHEN x.xxxrefs <> "SK65(" + m.manid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK65(" + m.manid + ")" + "1"
                END,
                x.fullxxxrefs =
                CASE
                    WHEN x.fullxxxrefs <> "SK66(" + m.manid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK66(" + m.manid + ")" + "1"
                END,
                x.oldkey =
                CASE
                    WHEN x.oldkey <> "SK67(" + m.manid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK67(" + m.manid + ")" + "1"
                END,
                x.abstract =
                CASE
                    WHEN x.abstract <> "SK68(" + m.manid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK68(" + m.manid  + ")" + "1"
                END,
                x.preliminary =
                CASE
                    WHEN x.preliminary <> "SK69(" + m.manid + ")" + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        "SK69(" + m.manid + ")" + "1"
                END 
        MERGE (y:_dummy {{
            _id: "(" + a.authid + ")"
        }})
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid + "1",
                y.name = a.name + "1"
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.authid + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.name + "1"
                END 
        MERGE (x)-[:ARTICLE_PUBLISHED {{
            _id: "(ARTICLE_PUBLISHED:" + elementId(x) + "," + elementId(y) + ")"
        }}]-(y)
        """)
        # rule#8 using our framework
        rule8 = TransformationRule(f"""
        MATCH (a:Author)
        MERGE (y:_dummy {{
            _id: "(" + a.authid + ")"
        }})
        ON CREATE
            SET y:Auth,
                y.authorid = a.authid + "1",
                y.name = a.name + "1"
        ON MATCH
            SET y:Auth,
                y.authorid =
                CASE
                    WHEN y.authorid <> a.authid + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.authid + "1"
                END,
                y.name =
                CASE
                    WHEN y.name <> a.name + toInteger(sign((rand() * 100) - {prob_conflict}))
                        THEN "Conflict detected!"
                    ELSE
                        a.name + "1"
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