import json
import re

from common import *
from gc import *

##
# Task class comprises information from both Spark's eventlog and btracelog.
##
class Task:
    def __init__(self, start):
        # Eventlog Information
        self.stage_id = start["Stage ID"]
        self.stage_attempt_id = start["Stage Attempt ID"]
        self.task_id = start["Task Info"]["Task ID"]
        self.task_attempt_id = start["Task Info"]["Attempt"]
        self.partition_index = start["Task Info"]["Index"]
        self.executor_id = start["Task Info"]["Executor ID"]
        self.host = start["Task Info"]["Host"]
        self.input_metrics = None
        self.output_metrics = None
        self.shuffle_write_metrics = None
        self.shuffle_read_metrics = None
        self.updated_blocks = None
        self.launch_time = None
        self.finish_time = None

        # BTrace Information
        self.start_common = None
        self.start_gc = None
        self.end_common = None
        self.end_gc = None
        self.gc = None

    def add_end(self, end):
        assert self.stage_id == end["Stage ID"]
        assert self.stage_attempt_id == end["Stage Attempt ID"]
        assert self.task_id == end["Task Info"]["Task ID"]
        assert self.task_attempt_id == end["Task Info"]["Attempt"]
        if "Input Metrics" in end["Task Metrics"]:
            self.input_metrics = self.InputMetrics(end["Task Metrics"]["Input Metrics"])
        if "Output Metrics" in end["Task Metrics"]:
            self.output_metrics = self.OutputMetrics(end["Task Metrics"]["Output Metrics"])
        if "Shuffle Write Metrics" in end["Task Metrics"]:
            self.shuffle_write_metrics = self.ShuffleWriteMetrics(end["Task Metrics"]["Shuffle Write Metrics"])
        if "Shuffle Read Metrics" in end["Task Metrics"]:
            self.shuffle_read_metrics = self.ShuffleReadMetrics(end["Task Metrics"]["Shuffle Read Metrics"])
        self.updated_blocks = []
        if "Updated Blocks" in end["Task Metrics"]:
            for b in end["Task Metrics"]["Updated Blocks"]:
                block = self.Block(b)
                self.updated_blocks.append(block)
        self.launch_time = end["Task Info"]["Launch Time"]
        self.finish_time = end["Task Info"]["Finish Time"]

    def __repr__(self):
        return "[Task " + str(self.task_id) + "]"

    def _new_repr(self, status):
        match = re.search("]", str(self))
        if match is None:
            return str(self)
        index = match.start()
        return str(self)[:index] + " " + status + str(self)[index:]

    def get_driver_text(self, status):
        result = ""
        if status == "start":
            result += str(self.start_common.time) + "(ms), " + str(self.start_common.total) + "(MB), " + str(self.start_common.pcpu) + " -- " + self._new_repr(status)
            flag = False
            if self.input_metrics != None:
                result += " " + str(self.input_metrics)
                flag = True
            if self.shuffle_read_metrics != None:
                if flag: result += ","
                result += " " + str(self.shuffle_read_metrics)
        elif status == "end":
            result += str(self.start_common.time) + "(ms), " + str(self.start_common.total) + "(MB), " + str(self.start_common.pcpu) + " -- " + self._new_repr(status)
            if self.shuffle_write_metrics != None:
                result += " " + str(self.shuffle_write_metrics)
        return result


    def get_executor_text(self, status):
        result = ""
        if status == "start":
            result += str(self.start_common.time) + "(ms), " + str(self.start_common.total) + "(MB), " + str(self.start_common.pcpu) + " -- " + self._new_repr(status)
            flag = False
            if self.input_metrics != None:
                result += " " + str(self.input_metrics)
                flag = True
            if self.shuffle_read_metrics != None:
                if flag: result += ","
                result += " " + str(self.shuffle_read_metrics)

        elif status == "end":
            result += str(self.start_common.time) + "(ms), " + str(self.start_common.total) + "(MB), " + str(self.start_common.pcpu) + " -- " + self._new_repr(status)
            flag = False
            if self.shuffle_write_metrics != None:
                result += " " + str(self.shuffle_write_metrics)
                flag = True

            gc_count = self.end_gc.count - self.start_gc.count
            gc_time  = self.end_gc.time  - self.start_gc.time
            if flag: result += ","
            result += " gc_count: " + str(gc_count) + ", gc_time: " + str(gc_time)
        return result

    class InputMetrics:
        def __init__(self, j):
            self.method = j["Data Read Method"]
            self.bytes_read = j["Bytes Read"]
            self.records_read = j["Records Read"]
        def __repr__(self):
            result  = "bytes_read: " + _adjust_size(self.bytes_read)
            result += ", records_read: " + str(self.records_read)
            return result

    class OutputMetrics:
        def __init__(self, j):
            self.method = j["Data Write Method"]
            self.bytes_written = j["Bytes Written"]
            self.records_written = j["Records Written"]
        def __repr__(self):
            result  = "bytes_written: " + _adjust_size(self.bytes_written)
            result += ", records_written: " + str(self.records_written)
            return result

    class ShuffleWriteMetrics:
        def __init__(self, j):
            self.shuffle_bytes_written = j["Shuffle Bytes Written"]
            self.shuffle_write_time = j["Shuffle Write Time"]
            self.shuffle_records_written = j["Shuffle Records Written"]
        def __repr__(self):
            result = "shuffle_bytes_written: " + _adjust_size(self.shuffle_bytes_written)
            result += ", shuffle_records_written: " + str(self.shuffle_records_written)
            return result

    class ShuffleReadMetrics:
        def __init__(self, j):
            self.remote_blocks_fetched = j["Remote Blocks Fetched"]
            self.local_blocks_fetched = j["Local Blocks Fetched"]
            self.fetch_wait_time = j["Fetch Wait Time"]
            self.remote_bytes_read = j["Remote Bytes Read"]
            self.local_bytes_read = j["Local Bytes Read"]
            self.total_records_read = j["Total Records Read"]
        def __repr__(self):
            result = "remote_bytes_read: " + _adjust_size(self.remote_bytes_read)
            result += ", local_bytes_read: " + _adjust_size(self.local_bytes_read)
            result += ", total_records_read: " + str(self.total_records_read)
            return result

    class Block:
        def __init__(self, j):
            self.block_id = j["Block ID"]  # "rdd_2_0" or "broadcast_1"
            self.storage_level = Task.StorageLevel(j["Status"]["Storage Level"])
            self.disk_size = j["Status"]["Disk Size"]
            self.memory_size = j["Status"]["Memory Size"]
            self.offheap_size = j["Status"]["ExternalBlockStore Size"]

    class StorageLevel:
        def __init__(self, j):
            self.use_disk = j["Use Disk"]
            self.use_memory = j["Use Memory"]
            self.use_offheap = j["Use ExternalBlockStore"]

def _adjust_size(size):
    l = len(str(size))
    if l <= 3: return str(size) + "(B)"
    elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
    else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"
