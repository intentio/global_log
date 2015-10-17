from common import *

##
# Persist class comprises information from btracelog.
##
class Persist:
    # common,persist,<memory|disk|offheap>,size
    def __init__(self, common, args):
        self.common = common
        assert args[0] == "persist"
        self.storage = args[1]
        self.size = long(args[2])

    def __repr__(self):
        return "[Persist " + str(self.storage) + "] " + _adjust_size(self.size)

    def get_driver_text(self):
        return str(self.common.time) + "(ms), " + \
                str(self.common.total) + "(MB), " + \
                str(self.common.pcpu) + \
                " -- " + str(self)

    def get_executor_text(self):
        return self.get_driver_text()

class CacheInfo:
    def __init__(self, storage_memory, persist_events):
        self.storage_memory = storage_memory
        self.caches = []
        self.get_caches(persist_events)

    def __repr__(self):
        result  = "[ Storage Memory Fraction ]"
        result += "\n- Storage Memory = " + _adjust_size(self.storage_memory)
        result += "\n- Total Memory Bytes Cached = " + _adjust_size(self.get_total_cached())
        return result

    def get_caches(self, persist_events):
        for persist in persist_events:
            if persist.storage == "memory":
                self.caches.append(persist)

    def get_total_cached(self):
        l = [cache.size for cache in self.caches]
        if len(l) == 0:
            return 0
        return sum(l)

def _adjust_size(size):
    l = len(str(size))
    if l <= 3: return str(size) + "(B)"
    elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
    else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

