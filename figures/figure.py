class Figure(object):
    def __init__(self, app, prefix, values=[], nbLaunches=1, showStats=True):
        self.app = app
        self.prefix = prefix
        self.x = values
        self.nbLaunches = nbLaunches
        self.showStats = showStats

    def compute(self):
        pass

    def plot(self):
        pass

    def print_cmd(self):
        pass
