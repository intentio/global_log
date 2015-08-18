import json
import re

from common import *

##
# Stage class comprises information from both Spark's eventlog and btracelog.
##
class Stage:
    def __init__(self, start, end):
        # Eventlog Information
        assert start["Stage Info"]["Stage ID"] == end["Stage Info"]["Stage ID"]
        assert start["Stage Info"]["Stage Attempt ID"] == end["Stage Info"]["Stage Attempt ID"]
        self.stage_id = end["Stage Info"]["Stage ID"]
        self.stage_attempt_id = end["Stage Info"]["Stage Attempt ID"]
        self.parent_ids = end["Stage Info"]["Parent IDs"]
        self.submission_time = end["Stage Info"]["Submission Time"]
        self.completion_time = end["Stage Info"]["Completion Time"]
        self.rdds = []
        for r in end["Stage Info"]["RDD Info"]:
            rdd = Stage.RDD(r)
            self.rdds.append(rdd)
        self.tasks = []

        self.total_bytes_cached = 0L
        self.total_records_read = 0L
        self.total_bytes_read = 0L

        # BTrace Information
        self.start_common = None
        self.end_common = None

    def __repr__(self):
        result = "[Stage " + str(self.stage_id) + "] "
        result += "total_bytes_read: " + self._adjust_size(self.total_bytes_read)
        result += ", total_records_read: " + str(self.total_records_read)
        result += ", totoal_bytes_cached: " + self._adjust_size(self.total_bytes_cached)
        return result

    def _new_repr(self, status):
        match = re.search("]", str(self))
        if match is None:
            return str(self)
        index = match.start()
        return str(self)[:index] + " " + status + str(self)[index:]

    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

    def get_driver_text(self, status):
        if status == "start":
            return str(self.start_common.time) + "(ms), " + str(self.start_common.total) + "(MB) -- " + self._new_repr(status)
        elif status == "end":
            return str(self.end_common.time) + "(ms), " + str(self.end_common.total) + "(MB) -- " + self._new_repr(status)

    def compute_total_data_read_cached(self):
        for task in self.tasks:
            if task.input_metrics != None:
                self.total_records_read += task.input_metrics.records_read
                self.total_bytes_read += task.input_metrics.bytes_read
            if len(task.updated_blocks) != 0:
                for block in task.updated_blocks:
                    self.total_bytes_cached += block.memory_size

    class RDD:
        def __init__(self, j):
            self.rdd_id = j["RDD ID"]
            self.name = j["Name"]
            self.storage_level = Stage.StorageLevel(j["Storage Level"])
            self.num_partitions = j["Number of Partitions"]
            self.num_cached_partitions = j["Number of Cached Partitions"]
            self.disk_size = j["Disk Size"]
            self.memory_size = j["Memory Size"]
            self.offheap_size = j["ExternalBlockStore Size"]

    class StorageLevel:
        def __init__(self, j):
            self.use_disk = j["Use Disk"]
            self.use_memory = j["Use Memory"]
            self.use_offheap = j["Use ExternalBlockStore"]
