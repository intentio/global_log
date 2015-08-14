import json
import re

class Job:
    def __init__(self, start, end):
        # Eventlog Information
        assert start["Job ID"] == end["Job ID"]
        self.job_id = start["Job ID"]
        self.submission_time = start["Submission Time"]
        self.completion_time = end["Completion Time"]
        self.job_result = end["Job Result"]["Result"]
        self.stages = []

        # BTrace Information
        self.start_time = None
        self.start_heap = None
        self.end_time = None
        self.end_heap = None

    def __repr__(self):
        return "[Job " + str(self.job_id) + "]"

    def _new_repr(self, status):
        match = re.search("]", str(self))
        if match is None:
            return str(self)
        index = match.start()
        return str(self)[:index] + " " + status + str(self)[index:]

    def get_driver_text(self, status):
        if status == "start":
            return str(self.start_time) + "(ms), " + str(self.start_heap) + "(MB) -- " + self._new_repr(status)
        elif status == "end":
            return str(self.end_time) + "(ms), " + str(self.end_heap) + "(MB) -- " + self._new_repr(status)


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
            rdd = RDD(r)
            self.rdds.append(rdd)
        self.tasks = []

        self.total_bytes_cached = 0L
        self.total_records_read = 0L
        self.total_bytes_read = 0L

        # BTrace Information
        self.start_time = None
        self.start_heap = None
        self.end_time = None
        self.end_heap = None

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
            return str(self.start_time) + "(ms), " + str(self.start_heap) + "(MB) -- " + self._new_repr(status)
        elif status == "end":
            return str(self.end_time) + "(ms), " + str(self.end_heap) + "(MB) -- " + self._new_repr(status)

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
        self.storage_level = StorageLevel(j["Storage Level"])
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
            self.input_metrics = InputMetrics(end["Task Metrics"]["Input Metrics"])
        self.output_metrics = None
        if "Output Metrics" in end["Task Metrics"]:
            self.output_metrics = OutputMetrics(end["Task Metrics"]["Output Metrics"])
        self.shuffle_write_metrics = None
        if "Shuffle Write Metrics" in end["Task Metrics"]:
            self.shuffle_write_metrics = ShuffleWriteMetrics(end["Task Metrics"]["Shuffle Write Metrics"])
        self.shuffle_read_metrics = None
        if "Shuffle Read Metrics" in end["Task Metrics"]:
            self.shuffle_read_metrics = ShuffleReadMetrics(end["Task Metrics"]["Shuffle Read Metrics"])
        self.updated_blocks = []
        if "Updated Blocks" in end["Task Metrics"]:
            for b in end["Task Metrics"]["Updated Blocks"]:
                block = Block(b)
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
        result  = "bytes_read: " + self._adjust_size(self.bytes_read)
        result += ", records_read: " + str(self.records_read)
        return result
    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

class OutputMetrics:
    def __init__(self, j):
        self.method = j["Data Write Method"]
        self.bytes_written = j["Bytes Written"]
        self.records_written = j["Records Written"]
    def __repr__(self):
        result  = "bytes_written: " + self._adjust_size(self.bytes_written)
        result += ", records_written: " + str(self.records_written)
        return result
    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

class ShuffleWriteMetrics:
    def __init__(self, j):
        self.shuffle_bytes_written = j["Shuffle Bytes Written"]
        self.shuffle_write_time = j["Shuffle Write Time"]
        self.shuffle_records_written = j["Shuffle Records Written"]
    def __repr__(self):
        result = "shuffle_bytes_written: " + self._adjust_size(self.shuffle_bytes_written)
        result += ", shuffle_records_written: " + str(self.shuffle_records_written)
        return result
    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

class ShuffleReadMetrics:
    def __init__(self, j):
        self.remote_blocks_fetched = j["Remote Blocks Fetched"]
        self.local_blocks_fetched = j["Local Blocks Fetched"]
        self.fetch_wait_time = j["Fetch Wait Time"]
        self.remote_bytes_read = j["Remote Bytes Read"]
        self.local_bytes_read = j["Local Bytes Read"]
        self.total_records_read = j["Total Records Read"]
    def __repr__(self):
        result = "remote_bytes_read: " + self._adjust_size(self.remote_bytes_read)
        result += ", local_bytes_read: " + self._adjust_size(self.local_bytes_read)
        result += ", total_records_read: " + str(self.total_records_read)
        return result
    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

class Block:
    def __init__(self, j):
        self.block_id = j["Block ID"]  # "rdd_2_0"
        self.rdd_id = self.block_id.split("_")[1]
        self.partition_index = self.block_id.split("_")[2]
        self.storage_level = StorageLevel(j["Status"]["Storage Level"])
        self.disk_size = j["Status"]["Disk Size"]
        self.memory_size = j["Status"]["Memory Size"]
        self.offheap_size = j["Status"]["ExternalBlockStore Size"]

####
# job_start / job end
#   dictionary, where
#   - key1: job_id
#
# stage_submitted / stage_completed
#   dictionary of dictionaries, where
#   - key1: job_id
#   - key2: (stage_id, stage_attempt_id)
#
# task_start / task_end
#   dictionary of dictionaries, where
#   - key2: (stage_id, stage_attempt_id)
#   - key3: (task_id, task_attempt_id)
# 
# Returns the list of jobs in which each of jobs contains all the
# stages and tasks that have ran during the job.
def parse_eventlog(fname):
    jobs = []

    job_start = {}
    job_end = {}
    stage_submitted = {}
    stage_completed = {}
    task_start = {}
    task_end = {}
    last_job = None

    # Parse eventlogs and put them in separate dictionaries.
    f = open(fname, "r")
    for line in f.readlines():
        j = json.loads(line)
        if j["Event"] == "SparkListenerJobStart":
            key1 = j["Job ID"]
            job_start[key1] = j
            last_job = key1
            stage_submitted[key1] = {}
            stage_completed[key1] = {}
            for s in j["Stage Infos"]:
                stage_id = s["Stage ID"]
                stage_attempt_id = s["Stage Attempt ID"]
                key2 = (stage_id, stage_attempt_id)
                stage_submitted[key1][key2] = None
                stage_completed[key1][key2] = None
        elif j["Event"] == "SparkListenerJobEnd":
            key1 = j["Job ID"]
            job_end[key1] = j
        elif j["Event"] == "SparkListenerStageSubmitted":
            key1 = last_job
            assert stage_submitted.has_key(key1)
            stage_id = j["Stage Info"]["Stage ID"]
            stage_attempt_id = j["Stage Info"]["Stage Attempt ID"]
            key2 = (stage_id, stage_attempt_id)
            assert stage_submitted[key1].has_key(key2)
            stage_submitted[key1][key2] = j
        elif j["Event"] == "SparkListenerStageCompleted":
            key1 = last_job
            assert stage_completed.has_key(key1)
            stage_id = j["Stage Info"]["Stage ID"]
            stage_attempt_id = j["Stage Info"]["Stage Attempt ID"]
            key2 = (stage_id, stage_attempt_id)
            assert stage_completed[key1].has_key(key2)
            stage_completed[key1][key2] = j
        elif j["Event"] == "SparkListenerTaskStart":
            stage_id = j["Stage ID"]
            stage_attempt_id = j["Stage Attempt ID"]
            key2 = (stage_id, stage_attempt_id)
            task_id = j["Task Info"]["Task ID"]
            task_attempt_id = j["Task Info"]["Attempt"]
            key3 = (task_id, task_attempt_id)
            if not task_start.has_key(key2):
                task_start[key2] = {}
            task_start[key2][key3] = j
        elif j["Event"] == "SparkListenerTaskEnd":
            stage_id = j["Stage ID"]
            stage_attempt_id = j["Stage Attempt ID"]
            key2 = (stage_id, stage_attempt_id)
            task_id = j["Task Info"]["Task ID"]
            task_attempt_id = j["Task Info"]["Attempt"]
            key3 = (task_id, task_attempt_id)
            if not task_end.has_key(key2):
                task_end[key2] = {}
            task_end[key2][key3] = j
        # Add more "Event" cases here
    f.close()

    # Combine the dictionaries into list of Jobs, in which each of
    # Jobs contains according Stages and Tasks.
    key1s = job_start.keys()
    key1s.sort()
    for key1 in key1s:
        job = Job(job_start[key1], job_end[key1])
        key2s = stage_submitted[key1].keys()
        key2s.sort()
        for key2 in key2s:
            stage = Stage(stage_submitted[key1][key2], stage_completed[key1][key2])
            key3s = task_start[key2].keys()
            key3s.sort()
            for key3 in key3s:
                task = Task(task_start[key2][key3], task_end[key2][key3])
                stage.tasks.append(task)
            job.stages.append(stage)
        jobs.append(job)

    # Compute additional stuff for each of Stages.
    for job in jobs:
        for stage in job.stages:
            stage.compute_total_data_read_cached()
    return jobs
