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
    
    def delRelIndexes(self, app, stats=False):
        # drop index on inProcPublished
        dropInProcPublished = """
        DROP INDEX idx_inProcPulished IF EXISTS
        """
        app.dropIndex(dropInProcPublished, stats)

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

        # transformation rules
        self.rules = [rule1, rule2]

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
