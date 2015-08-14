
##
# Persist class comprises information from btracelog.
##
class Persist:
    # time,heap,persist,<memory|disk|offheap>,size
    def __init__(self, args):
        self.time = long(args[0])
        self.heap = float(args[1])
        self.storage = args[3]
        self.size = long(args[4])

    def __repr__(self):
        return "[Persist " + str(self.storage) + "] " + self._adjust_size(self.size)

    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

    def get_driver_text(self):
        return str(self.time) + "(ms), " + str(self.heap) + "(MB) -- " + str(self)

    def get_executor_text(self):
        return str(self.time) + "(ms), " + str(self.heap) + "(MB) -- " + str(self)
