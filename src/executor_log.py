from matplotlib import pyplot as plt
import numpy as np
from termcolor import colored

from event.job import *
from event.stage import *
from event.task import *
from event.persist import *
from event.shuffle import *

class ExecutorLog:
    def __init__(self, eventlog_fname, btracelog_fname):
        self.eventlog_fname = eventlog_fname
        self.btracelog_fname = btracelog_fname

        self.jobs = []
        self.persists = []
        self.shuffles = []
        self.max_heap = None

    def parse(self):
        self.parse_eventlog()
        self.parse_btracelog()

    def parse_btracelog(self):
        if len(self.jobs) == 0:
            return

        j = 0
        s = 0

        tasks = {}
        for job in self.jobs:
            for stage in job.stages:
                for task in stage.tasks:
                    tasks[task.task_id] = task

        f = open(self.btracelog_fname, "r")
        for line in f.readlines():
            lst = line[:-1].split(",")
            if len(lst) <= 2:
                continue

            time = long(lst[0])  # vm up time (ms)
            heap = float(lst[1])  # jvm heap used (MB)
            event = lst[2]

            if event == "task":
                task_id = int(lst[4])
                assert tasks.has_key(task_id)
                task = tasks[task_id]
                if lst[3] == "start":
                    task.start_time = time
                    task.start_heap = heap
                    task.start_minor_gc_count = long(lst[5])
                    task.start_minor_gc_time = long(lst[6])
                    task.start_major_gc_count = long(lst[7])
                    task.start_major_gc_time = long(lst[8])
                elif lst[3] == "end":
                    task.end_time = time
                    task.end_heap = heap
                    task.end_minor_gc_count = long(lst[5])
                    task.end_minor_gc_time = long(lst[6])
                    task.end_major_gc_count = long(lst[7])
                    task.end_major_gc_time = long(lst[8])
            elif event == "persist":
                self.persists.append(Persist(lst))
            elif event == "shuffle":
                self.shuffles.append(Shuffle(lst))
        f.close()

    def plot(self):
        # Plot all the collected (time,heap) points.
        x, y = np.loadtxt(self.btracelog_fname, unpack=True, delimiter=",")
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(x, y)
        ax.set_xlabel("Time (ms)")
        ax.set_ylabel("JVM Heap Used (MB)")
        self.max_heap = max(y)
        ax.text(0.05, 0.95, "Max JVM Heap Used = " + str(self.max_heap) + " (MB)", transform=ax.transAxes, verticalalignment='top')

        # Collect all the btrace events except periodic alarm function.
        d = []
        for job in self.jobs:
            for stage in job.stages:
                for task in stage.tasks:
                    if task.start_time != None and task.end_time != None:
                        d.append( (task.start_time, task.start_heap, task, "start") )
                        d.append( (task.end_time, task.end_heap, task, "end") )
                        task.compute_gc_count_time()
        for persist in self.persists:
            d.append( (persist.time, persist.heap, persist) )

        for shuffle in self.shuffles:
            d.append( (shuffle.time, shuffle.heap, shuffle) )

        def comp(x,y):
            c = x[0] - y[0]
            if c < 0: return -1
            elif c > 0: return 1
            else: return 0

        d.sort(cmp=comp)

        # Print out collected btrace events before plotting them.
        for tup in d:
            if isinstance(tup[2], Job):
                print tup[2].get_executor_text(tup[3])
            elif isinstance(tup[2], Stage):
                print tup[2].get_executor_text(tup[3])
            elif isinstance(tup[2], Task):
                print tup[2].get_executor_text(tup[3])
            else:  # Persist, Shuffle
                print tup[2].get_executor_text()
        print "\n"

        # Plot collected btrace events with different colors and size.
        x = [tup[0] for tup in d]  # time list
        y = [tup[1] for tup in d]  # heap list
        c = []  # color list
        s = 50
        for tup in d:
            if isinstance(tup[2], Task):
                c.append('r')
            elif isinstance(tup[2], Persist):
                c.append('b')
            elif isinstance(tup[2], Shuffle):
                if tup[2].method == "start" or tup[2].method == "end":
                    c.append('g')
                else:
                    c.append('y')

        print colored("Red   ", 'red') + ": Task"
        print colored("Blue  ", 'blue') + ": Persist"
        print colored("Green ", 'green') + ": Shuffle start/end"
        print colored("Yellow", 'yellow') + ": Shuffle spill/release\n"

        print "[ Executor Memory ]"
        print "- Max JVM Heap Used = " + str(self.max_heap) + "(MB)\n"

        sinfo = ShuffleInfo(self.shuffles)
        print sinfo
        print "\n"

        # By clicking each of collected btrace events on the graph,
        # you can print out text associated with the event.
        def onclick(event):
            i = event.ind[0]
            if isinstance(d[i][2], Job):
                print d[i][2].get_executor_text(d[i][3])
            elif isinstance(d[i][2], Stage):
                print d[i][2].get_executor_text(d[i][3])
            elif isinstance(d[i][2], Task):
                print d[i][2].get_executor_text(d[i][3])
            else:  # Persist, Shuffle
                print d[i][2].get_executor_text()

        ax.scatter(x, y, 50, c, picker=True)
        fig.canvas.mpl_connect('pick_event', onclick)
        plt.show()


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
    def parse_eventlog(self):
        job_start = {}
        job_end = {}
        stage_submitted = {}
        stage_completed = {}
        task_start = {}
        task_end = {}
        last_job = None

        # Parse eventlogs and put them in separate dictionaries.
        f = open(self.eventlog_fname, "r")
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
            self.jobs.append(job)

        # Compute additional stuff for each of Stages.
        for job in self.jobs:
            for stage in job.stages:
                stage.compute_total_data_read_cached()

    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"


import sys
if __name__ == "__main__":
    el = ExecutorLog(sys.argv[1], sys.argv[2])
    el.parse()
    el.plot()
