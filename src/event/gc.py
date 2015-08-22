class GC:
    def __init__(self, args):
        assert len(args) == 2
        self.count = long(args[0])  # number of GC occurred until current.
        self.time = long(args[1])  # amount of time spent on GC until current.
