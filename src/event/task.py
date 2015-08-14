import json
import re

##
# Task class comprises information from both Spark's eventlog and btracelog.
##
class Task:
    def __init__(self, start, end):
        # Eventlog Information
        assert start["Stage ID"] == end["Stage ID"]
        assert start["Stage Attempt ID"] == end["Stage Attempt ID"]
        assert start["Task Info"]["Task ID"] == end["Task Info"]["Task ID"]
        assert start["Task Info"]["Attempt"] == end["Task Info"]["Attempt"]
        self.stage_id = end["Stage ID"]
        self.stage_attempt_id = end["Stage Attempt ID"]
        self.task_id = end["Task Info"]["Task ID"]
        self.task_attempt_id = end["Task Info"]["Attempt"]
        self.partition_index = end["Task Info"]["Index"]
        self.executor_id = end["Task Info"]["Executor ID"]
        self.host = end["Task Info"]["Host"]
        self.input_metrics = None
        if "Input Metrics" in end["Task Metrics"]:
            self.input_metrics = self.InputMetrics(end["Task Metrics"]["Input Metrics"])
        self.output_metrics = None
        if "Output Metrics" in end["Task Metrics"]:
            self.output_metrics = self.OutputMetrics(end["Task Metrics"]["Output Metrics"])
        self.shuffle_write_metrics = None
        if "Shuffle Write Metrics" in end["Task Metrics"]:
            self.shuffle_write_metrics = self.ShuffleWriteMetrics(end["Task Metrics"]["Shuffle Write Metrics"])
        self.shuffle_read_metrics = None
        if "Shuffle Read Metrics" in end["Task Metrics"]:
            self.shuffle_read_metrics = self.ShuffleReadMetrics(end["Task Metrics"]["Shuffle Read Metrics"])
        self.updated_blocks = []
        if "Updated Blocks" in end["Task Metrics"]:
            for b in end["Task Metrics"]["Updated Blocks"]:
                block = self.Block(b)
                self.updated_blocks.append(block)
        self.launch_time = end["Task Info"]["Launch Time"]
        self.finish_time = end["Task Info"]["Finish Time"]

        # BTrace Information
        self.start_time = None
        self.start_heap = None
        self.start_minor_gc_count = None
        self.start_minor_gc_time = None
        self.start_major_gc_count = None
        self.start_major_gc_time = None
        self.end_time = None
        self.end_heap = None
        self.end_minor_gc_count = None
        self.end_minor_gc_time = None
        self.end_major_gc_count = None
        self.end_major_gc_time = None
        self.minor_gc_count = None
        self.minor_gc_time = None
        self.major_gc_count = None
        self.major_gc_time = None


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
            result += str(self.start_time) + "(ms), " + str(self.start_heap) + "(MB) -- " + self._new_repr(status)
            flag = False
            if self.input_metrics != None:
                result += " " + str(self.input_metrics)
                flag = True
            if self.shuffle_read_metrics != None:
                if flag: result += ", " + str(self.shuffle_read_metrics)
                else: result += " " + str(self.shuffle_read_metrics)
        elif status == "end":
            result += str(self.end_time) + "(ms), " + str(self.end_heap) + "(MB) -- " + self._new_repr(status)
            if self.shuffle_write_metrics != None:
                result += " " + str(self.shuffle_write_metrics)
        return result

    def get_executor_text(self, status):
        result = ""
        if status == "start":
            result += str(self.start_time) + "(ms), " + str(self.start_heap) + "(MB) -- " + self._new_repr(status)
            flag = False
            if self.input_metrics != None:
                result += " " + str(self.input_metrics)
                flag = True
            if self.shuffle_read_metrics != None:
                if flag: result += ", " + str(self.shuffle_read_metrics)
                else: result += " " + str(self.shuffle_read_metrics)

        elif status == "end":
            result += str(self.end_time) + "(ms), " + str(self.end_heap) + "(MB) -- " + self._new_repr(status)
            flag = False
            if self.shuffle_write_metrics != None:
                result += " " + str(self.shuffle_write_metrics)
                flag = True
            if flag: result += ", minor_gc_count: " + str(self.minor_gc_count) + ", minor_gc_time: " + str(self.minor_gc_time)
            else: result += " minor_gc_count: " + str(self.minor_gc_count) + ", minor_gc_time: " + str(self.minor_gc_time)
            result += ", major_gc_count: " + str(self.major_gc_count) + ", major_gc_time: " + str(self.major_gc_time)
        return result

    def compute_gc_count_time(self):
        self.minor_gc_count = self.end_minor_gc_count - self.start_minor_gc_count
        self.minor_gc_time = self.end_minor_gc_time - self.start_minor_gc_time
        self.major_gc_count = self.end_major_gc_count - self.start_major_gc_count
        self.major_gc_time = self.end_major_gc_time - self.start_major_gc_time

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
            self.block_id = j["Block ID"]  # "rdd_2_0"
            self.rdd_id = self.block_id.split("_")[1]
            self.partition_index = self.block_id.split("_")[2]
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
