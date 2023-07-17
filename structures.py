from app import App

class InputRelation(object):
    def __init__(self, path_to_csv_file, mergeCMD):
        self.file = path_to_csv_file
        self.mergeCMD = mergeCMD

    def populate(self, app):
        app.populate_with_csv(self.file, self.mergeCMD)

    def __str__(self):
        return f"    {self.mergeCMD} from {self.file}\n" 

class InputSchema(object):
    def __init__(self, input_relations):
        self.relations = input_relations

    def instanciate(self, app):
        for rel in self.relations:
            rel.populate(app)

    def __str__(self):
        desc = "Input schema:\n"
        for rel in self.relations:
            desc += str(rel)
        return desc

class TransformationRule(object):
    def __init__(self, query_str):
        self.query_str = query_str

    def apply(self, app):
        return app.query(self.query_str) 

    def __str__(self):
        return f"    {self.query_str}"

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

    def __str__(self):
        desc = f"\n*****\n"
        desc += str(self.schema)
        desc += "Transformation rules:"
        for rule in self.rules:
            desc += str(rule)
        desc += f"\n*****\n"
        return desc

    def addIndexes(self, app, stats=False):
        pass

    def destroyIndexes(self, app, stats=False):
        pass
    
    def run(self, app, launches = 5, stats=False, index=True):
        ttime = 0.0
        for i in range(launches):
            self.prepare(app, stats=stats)
            if(index):
                self.addIndexes(app, stats=stats)
            ttime += self.transform(app, stats=stats)
            if(index):
                self.destroyIndexes(app, stats=stats)
        avg_time = ttime / launches
        if(stats):
            print(f"The transformation: {self} averaged {avg_time} ms over {launches} run(s).")
        return avg_time 
