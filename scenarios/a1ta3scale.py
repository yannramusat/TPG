import os
from app import App
from scenarios.scenario import InputRelation, InputSchema, TransformationRule, Scenario

class Amalgam1ToAmalgam3Scale(Scenario):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        relations = []
        # csv#1
        for i in range(scale):
            rel_inproceedings_cmd = f"""MERGE (n:InProceedings{i} {{
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
            }})"""
            param_string = "a1ta3/inproceedings"+str(size)+"-"+str(lstring)+".csv"
            rel_inproceedings = InputRelation(os.path.join(prefix, param_string), rel_inproceedings_cmd)
            relations.append(rel_inproceedings)
        # csv#2
        for i in range(scale):
            rel_article_cmd = f"""MERGE (n:Article{i} {{
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
            }})"""
            param_string = "a1ta3/article"+str(size)+"-"+str(lstring)+".csv"
            rel_article = InputRelation(os.path.join(prefix, param_string), rel_article_cmd)
            relations.append(rel_article)
        # csv#3
        for i in range(scale):
            rel_techreport_cmd = f"""MERGE (n:TechReport{i} {{
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
            }})"""
            param_string = "a1ta3/techreport"+str(size)+"-"+str(lstring)+".csv"
            rel_techreport = InputRelation(os.path.join(prefix, param_string), rel_techreport_cmd)
            relations.append(rel_techreport)
        # csv#4
        for i in range(scale):
            rel_book_cmd = f"""MERGE (n:Book{i} {{
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
            }})"""
            param_string = "a1ta3/book"+str(size)+"-"+str(lstring)+".csv"
            rel_book = InputRelation(os.path.join(prefix, param_string), rel_book_cmd)
            relations.append(rel_book)
        # csv#5
        for i in range(scale):
            rel_incollection_cmd = f"""MERGE (n:InCollection{i} {{
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
            }})"""
            param_string = "a1ta3/incollection"+str(size)+"-"+str(lstring)+".csv"
            rel_incollection = InputRelation(os.path.join(prefix, param_string), rel_incollection_cmd)
            relations.append(rel_incollection)
        # csv#6
        for i in range(scale):
            rel_misc_cmd = f"""MERGE (n:Misc{i} {{
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
            }})"""
            param_string = "a1ta3/misc"+str(size)+"-"+str(lstring)+".csv"
            rel_misc = InputRelation(os.path.join(prefix, param_string), rel_misc_cmd)
            relations.append(rel_misc)
        # csv#7
        for i in range(scale):
            rel_manual_cmd = f"""MERGE (n:Manual{i} {{
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
            }})"""
            param_string = "a1ta3/manual"+str(size)+"-"+str(lstring)+".csv"
            rel_manual = InputRelation(os.path.join(prefix, param_string), rel_manual_cmd)
            relations.append(rel_manual)
        # csv#8
        for i in range(scale):
            rel_author_cmd = f"""MERGE (n:Author{i} {{
                authid: row[1],
                name: row[2]
            }})"""
            param_string = "a1ta3/author"+str(size)+"-"+str(lstring)+".csv"
            rel_author = InputRelation(os.path.join(prefix, param_string), rel_author_cmd)
            relations.append(rel_author)
        # csv#9
        for i in range(scale):
            rel_inprocpublished_cmd = f"""MERGE (n:InProcPublished{i} {{
                inproc: row[1],
                auth: row[2]
            }})"""
            param_string = "a1ta3/inprocpublished"+str(size)+"-"+str(lstring)+".csv"
            rel_inprocpublished = InputRelation(os.path.join(prefix, param_string), rel_inprocpublished_cmd)
            relations.append(rel_inprocpublished)
        # csv#10
        for i in range(scale):
            rel_articlepublished_cmd = f"""MERGE (n:ArticlePublished{i} {{
                article: row[1],
                auth: row[2]
            }})"""
            param_string = "a1ta3/articlepublished"+str(size)+"-"+str(lstring)+".csv"
            rel_articlepublished = InputRelation(os.path.join(prefix, param_string), rel_articlepublished_cmd)
            relations.append(rel_articlepublished)
        # csv#11
        for i in range(scale):
            rel_techpublished_cmd = f"""MERGE (n:TechPublished{i} {{
                tech: row[1],
                auth: row[2]
            }})"""
            param_string = "a1ta3/techpublished"+str(size)+"-"+str(lstring)+".csv"
            rel_techpublished = InputRelation(os.path.join(prefix, param_string), rel_techpublished_cmd)
            relations.append(rel_techpublished)
        # csv#12
        for i in range(scale):
            rel_bookpublished_cmd = f"""MERGE (n:BookPublished{i} {{
                book: row[1],
                auth: row[2]
            }})"""
            param_string = "a1ta3/bookpublished"+str(size)+"-"+str(lstring)+".csv"
            rel_bookpublished = InputRelation(os.path.join(prefix, param_string), rel_bookpublished_cmd)
            relations.append(rel_bookpublished)
        # csv#13
        for i in range(scale):
            rel_incollpublished_cmd = f"""MERGE (n:InCollPublished{i} {{
                col: row[1],
                auth: row[2]
            }})"""
            param_string = "a1ta3/incollpublished"+str(size)+"-"+str(lstring)+".csv"
            rel_incollpublished = InputRelation(os.path.join(prefix, param_string), rel_incollpublished_cmd)
            relations.append(rel_incollpublished)
        # csv#14
        for i in range(scale):
            rel_miscpublished_cmd = f"""MERGE (n:MiscPublished{i} {{
                misc: row[1],
                auth: row[2]
            }})"""
            param_string = "a1ta3/miscpublished"+str(size)+"-"+str(lstring)+".csv"
            rel_miscpublished = InputRelation(os.path.join(prefix, param_string), rel_miscpublished_cmd)
            relations.append(rel_miscpublished)
        # csv#15
        for i in range(scale):
            rel_manualpublished_cmd = f"""MERGE (n:ManualPublished{i} {{
                manual: row[1],
                auth: row[2]
            }})"""
            param_string = "a1ta3/manualpublished"+str(size)+"-"+str(lstring)+".csv"
            rel_manualpublished = InputRelation(os.path.join(prefix, param_string), rel_manualpublished_cmd)
            relations.append(rel_manualpublished)
        # source schema
        self.schema = InputSchema(relations)

class Amalgam1ToAmalgam3PlainScale(Amalgam1ToAmalgam3Scale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (pub:InProcPublished{i})
            MATCH (ip:InProceedings{i})
            WHERE pub.inproc = ip.inprocid
            MATCH (a:Author{i})
            WHERE pub.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(ip) + ",{i})" 
            }})
            SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            SET y:Auth{i},
                y.authorid = a.authid,
                y.name = a.name
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule1)
        # rule#2 using our framework
        for i in range(scale):
            rule2 = TransformationRule(f"""
            MATCH (ap:ArticlePublished{i})
            MATCH (art:Article{i})
            WHERE ap.article = art.articleid
            MATCH (a:Author{i})
            WHERE ap.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(art) + ",{i})" 
            }})
            SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            SET y:Auth{i},
                y.authorid = a.authid,
                y.name = a.name
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule2)
        # rule#3 using our framework
        for i in range(scale):
            rule3 = TransformationRule(f"""
            MATCH (tp:TechPublished{i})
            MATCH (t:TechReport{i})
            WHERE tp.tech = t.techid
            MATCH (a:Author{i})
            WHERE tp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(t) + ",{i})" 
            }})
            SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            SET y:Auth{i},
                y.authorid = a.authid,
                y.name = a.name
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule3)
        # rule#4 using our framework
        for i in range(scale):
            rule4 = TransformationRule(f"""
            MATCH (bp:BookPublished{i})
            MATCH (b:Book{i})
            WHERE bp.book = b.bookid
            MATCH (a:Author{i})
            WHERE bp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(b) + ",{i})" 
            }})
            SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            SET y:Auth{i},
                y.authorid = a.authid,
                y.name = a.name
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule4)
        # rule#5 using our framework
        for i in range(scale):
            rule5 = TransformationRule(f"""
            MATCH (icp:InCollPublished{i})
            MATCH (i:InCollection{i})
            WHERE icp.col = i.colid
            MATCH (a:Author{i})
            WHERE icp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(i) + ",{i})" 
            }})
            SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            SET y:Auth{i},
                y.authorid = a.authid,
                y.name = a.name
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule5)
        # rule#6 using our framework
        for i in range(scale):
            rule6 = TransformationRule(f"""
            MATCH (mp:MiscPublished{i})
            MATCH (m:Misc{i})
            WHERE mp.misc = m.miscid
            MATCH (a:Author{i})
            WHERE mp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(m) + ",{i})" 
            }})
            SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            SET y:Auth{i},
                y.authorid = a.authid,
                y.name = a.name
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule6)
        # rule#7 using our framework
        for i in range(scale):
            rule7 = TransformationRule(f"""
            MATCH (mp:ManualPublished{i})
            MATCH (m:Manual{i})
            WHERE mp.manual = m.manid
            MATCH (a:Author{i})
            WHERE mp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(m) + ",{i})" 
            }})
            SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            SET y:Auth{i},
                y.authorid = a.authid,
                y.name = a.name
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule7)
        # rule#8 using our framework
        for i in range(scale):
            rule8 = TransformationRule(f"""
            MATCH (a:Author{i})
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            SET y:Auth{i},
                y.authorid = a.authid,
                y.name = a.name
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

class Amalgam1ToAmalgam3CDoverPlainScale(Amalgam1ToAmalgam3PlainScale):
    def __init__(self, prefix, size = 100, lstring = 5, scale = 2):
        # input schema
        super().__init__(prefix, size, lstring, scale)

        rules = []
        # rule#1 using our framework
        for i in range(scale):
            rule1 = TransformationRule(f"""
            MATCH (pub:InProcPublished{i})
            MATCH (ip:InProceedings{i})
            WHERE pub.inproc = ip.inprocid
            MATCH (a:Author{i})
            WHERE pub.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(ip) + ",{i})" 
            }})
            ON CREATE
                SET x:TArticle{i},
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
                SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            ON CREATE
                SET y:Auth{i},
                    y.authorid = a.authid,
                    y.name = a.name
            ON MATCH
                SET y:Auth{i},
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
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule1)
        # rule#2 using our framework
        for i in range(scale):
            rule2 = TransformationRule(f"""
            MATCH (ap:ArticlePublished{i})
            MATCH (art:Article{i})
            WHERE ap.article = art.articleid
            MATCH (a:Author{i})
            WHERE ap.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(art) + ",{i})" 
            }})
            ON CREATE
                SET x:TArticle{i},
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
                SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            ON CREATE
                SET y:Auth{i},
                    y.authorid = a.authid,
                    y.name = a.name
            ON MATCH
                SET y:Auth{i},
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
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule2)
        # rule#3 using our framework
        for i in range(scale):
            rule3 = TransformationRule(f"""
            MATCH (tp:TechPublished{i})
            MATCH (t:TechReport{i})
            WHERE tp.tech = t.techid
            MATCH (a:Author{i})
            WHERE tp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(t) + ",{i})"
            }})
            ON CREATE
                SET x:TArticle{i},
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
                SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            ON CREATE
                SET y:Auth{i},
                    y.authorid = a.authid,
                    y.name = a.name
            ON MATCH
                SET y:Auth{i},
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
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule3)
        # rule#4 using our framework
        for i in range(scale):
            rule4 = TransformationRule(f"""
            MATCH (bp:BookPublished{i})
            MATCH (b:Book{i})
            WHERE bp.book = b.bookid
            MATCH (a:Author{i})
            WHERE bp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(b) + ",{i})" 
            }})
            ON CREATE
                SET x:TArticle{i},
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
                SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            ON CREATE
                SET y:Auth{i},
                    y.authorid = a.authid,
                    y.name = a.name
            ON MATCH
                SET y:Auth{i},
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
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule4)
        # rule#5 using our framework
        for i in range(scale):
            rule5 = TransformationRule(f"""
            MATCH (icp:InCollPublished{i})
            MATCH (i:InCollection{i})
            WHERE icp.col = i.colid
            MATCH (a:Author{i})
            WHERE icp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(i) + ",{i})" 
            }})
            ON CREATE
                SET x:TArticle{i},
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
                SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            ON CREATE
                SET y:Auth{i},
                    y.authorid = a.authid,
                    y.name = a.name
            ON MATCH
                SET y:Auth{i},
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
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule5)
        # rule#6 using our framework
        for i in range(scale):
            rule6 = TransformationRule(f"""
            MATCH (mp:MiscPublished{i})
            MATCH (m:Misc{i})
            WHERE mp.misc = m.miscid
            MATCH (a:Author{i})
            WHERE mp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(m) + ",{i})"
            }})
            ON CREATE
                SET x:TArticle{i},
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
                SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            ON CREATE
                SET y:Auth{i},
                    y.authorid = a.authid,
                    y.name = a.name
            ON MATCH
                SET y:Auth{i},
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
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule6)
        # rule#7 using our framework
        for i in range(scale):
            rule7 = TransformationRule(f"""
            MATCH (mp:ManualPublished{i})
            MATCH (m:Manual{i})
            WHERE mp.manual = m.manid
            MATCH (a:Author{i})
            WHERE mp.auth = a.authid 
            MERGE (x:_dummy {{ 
                _id: "(" + elementId(m) + ",{i})" 
            }})
            ON CREATE
                SET x:TArticle{i},
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
                SET x:TArticle{i},
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
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            ON CREATE
                SET y:Auth{i},
                    y.authorid = a.authid,
                    y.name = a.name
            ON MATCH
                SET y:Auth{i},
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
            MERGE (x)-[:ARTICLE_PUBLISHED{i} {{
                _id: "(ARTICLE_PUBLISHED{i}:" + elementId(x) + "," + elementId(y) + ",{i})"
            }}]-(y)
            """)
            rules.append(rule7)
        # rule#8 using our framework
        for i in range(scale):
            rule8 = TransformationRule(f"""
            MATCH (a:Author{i})
            MERGE (y:_dummy {{
                _id: "(" + a.authid + ",{i})"
            }})
            ON CREATE
                SET y:Auth{i},
                    y.authorid = a.authid,
                    y.name = a.name
            ON MATCH
                SET y:Auth{i},
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
            rules.append(rule8)
        # transformation rules
        self.rules = rules