##
# Shuffle class comprises information from btracelog.
##
class Shuffle:
    # time,heap,shuffle,<start|end>,<map|sorter>
    # time,heap,shuffle,spill,size
    def __init__(self, args):
        self.time = long(args[0])
        self.heap = float(args[1])
        assert args[2] == "shuffle"
        self.status = args[3]
        if self.status == "spill":
            self.size = long(args[4])
            self.type = None
        else:
            self.size = None
            self.type = args[4]

    def __repr__(self):
        result =  "[Shuffle " + self.status + "]"
        if self.status == "spill":
            return result + " " + self._adjust_size(self.size)
        return result

    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

    def get_executor_text(self):
        return str(self.time) + "(ms), " + str(self.heap) + "(MB) -- " + str(self)
