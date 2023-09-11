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
            x.refkey = "SK2(" + ip.inprocid = ")",
            x.note = ip.note,
            x.remarks = "SK3(" + ip.inprocid = ")",
            x.refs = "SK4(" + ip.inprocid = ")",
            x.xxxrefs = "SK5(" + ip.inprocid = ")",
            x.fullxxxrefs = "SK6(" + ip.inprocid = ")",
            x.oldkey = "SK7(" + ip.inprocid = ")",
            x.abstract = "SK8(" + ip.inprocid = ")",
            x.preliminary = "SK9(" + ip.inprocid = ")"
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

        # transformation rules
        self.rules = [rule1]

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
