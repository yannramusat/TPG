from app import App

class InputRelation(object):
    def __init__(self, path_to_csv_file, mergeCMD):
        self.file = path_to_csv_file
        self.mergeCMD = mergeCMD

    def populate(self, app):
        app.populate_with_csv(self.file, self.mergeCMD)

class InputSchema(object):
    def __init__(self, input_relations):
        self.relations = input_relations

    def instanciate(self, app):
        for rel in self.relations:
            rel.populate(app)

class TransformationRule(object):
    def __init__(self, query_str):
        self.query_str = query_str

    def apply(self, app):
        return app.query(self.query_str) 

class Scenario(object):
    def __init__(self, schema, rules):
        self.schema = schema
        self.rules = rules

    def prepare(self, app, stats=False):
        app.flush_database()
        self.schema.instanciate(app)
        if(stats):
            app.output_all_nodes(stats=True)

    def transform(self, app, stats=False):
        elapsed = 0
        for rule in self.rules:
            elapsed += rule.apply(app)
        if(stats):
            print(f"The transformation has been executed in {elapsed} ms.")
            app.output_all_nodes(stats=True)
        return elapsed
