##
# Shuffle class comprises information from btracelog.
##
class Shuffle:
    # time,heap,shuffle,<map|sorter>,<start|end>
    # time,heap,shuffle,<map|sorter>,spill,size
    # time,heap,shuffle,manager,release,size
    def __init__(self, args):
        self.time = long(args[0])
        self.heap = float(args[1])
        assert args[2] == "shuffle"
        self.cls = args[3]
        self.method = args[4]
        self.size = None
        if self.method == "spill" or self.method == "release":
            self.size = long(args[5])

    def __repr__(self):
        result =  "[Shuffle " + self.cls + " " + self.method + "]"
        if self.method == "spill" or self.method == "release":
            return result + " " + _adjust_size(self.size)
        return result

    def get_executor_text(self):
        return str(self.time) + "(ms), " + str(self.heap) + "(MB) -- " + str(self)

##
# ShuffleInfo class combines shuffle events and divide them into shuffle
# phases. And each of shuffles phases has start_time, end_time, and
# memory_bytes_spilled for that phase.
# ##
class ShuffleInfo:
    def __init__(self, shuffle_memory, shuffle_events):
        self.shuffle_memory = shuffle_memory
        self.shuffles = None
        self.combine_shuffle_events(shuffle_events)

    def __repr__(self):
        result  = "[ Shuffle Memory Fraction ]"
        result += "\n- Shuffle Memory = " + _adjust_size(self.shuffle_memory)
        result += "\n- Max Memory Bytes Spilled  = " + _adjust_size(self.get_max_spill())
        #result += "\n- Total Memory Bytes Spilled  = " + _adjust_size(self.get_total_spill())
        result += "\n- Max Memory Bytes Released = " + _adjust_size(self.get_max_release())
        #result += "\n- Total Memory Bytes Released = " + _adjust_size(self.get_total_release())
        for shuffle in self.shuffles:
            #result += "\nclass: " + shuffle["class"]
            #if shuffle["class"] == "map": result += "   "
            #result += ", start_time: " + str(shuffle["start_time"])
            result += "\n  start_time: " + str(shuffle["start_time"])
            result += ", end_time: " + str(shuffle["end_time"])
            result += ", memory_bytes_spilled: " + _adjust_size(shuffle["total_spill"])
            result += ", memory_bytes_released: " + _adjust_size(shuffle["total_release"])
        return result

    def combine_shuffle_events(self, shuffle_events):
        self.shuffles = []
        shuffle = None

        for event in shuffle_events:
            if event.method == "start":
                shuffle = {"class":event.cls, "start_time":event.time, "end_time":None, "total_spill":0, "total_release":0}
            elif event.method == "spill":
                shuffle["total_spill"] += event.size
            elif event.method == "release":
                shuffle["total_release"] += event.size
            elif event.method == "end":
                shuffle["end_time"] = event.time
                self.shuffles.append(shuffle)

    def get_max_spill(self):
        l = [shuffle["total_spill"] for shuffle in self.shuffles]
        if len(l) == 0:
            return 0
        return max(l)

    def get_total_spill(self):
        l = [shuffle["total_spill"] for shuffle in self.shuffles]
        if len(l) == 0:
            return 0
        return sum(l)

    def get_max_release(self):
        l = [shuffle["total_release"] for shuffle in self.shuffles]
        if len(l) == 0:
            return 0
        return max(l)

    def get_total_release(self):
        l = [shuffle["total_release"] for shuffle in self.shuffles]
        if len(l) == 0:
            return 0
        return sum(l)

def _adjust_size(size):
    l = len(str(size))
    if l <= 3: return str(size) + "(B)"
    elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
    else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"
