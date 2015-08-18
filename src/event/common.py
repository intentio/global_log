class Common:
    def __init__(self, args):
        self.time = long(args[0])  # vm up time (ms)
        self.heap = float(args[1])  # jvm heap used (MB)
        self.nonheap = float(args[2])  # jvm nonheap used (MB)
        self.total = float(args[3])  # heap + nonheap (MB)
