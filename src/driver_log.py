from matplotlib import pyplot as plt
import numpy as np
from termcolor import colored

from events import *

class DriverLog:
    def __init__(self, eventlog_fname, btracelog_fname):
        self.eventlog_fname = eventlog_fname
        self.btracelog_fname = btracelog_fname
        
        self.jobs = []
        self.persists = []
        self.max_heap = None

    def parse(self):
        self.parse_eventlog()
        self.parse_btracelog()

    def parse_btracelog(self):
        if len(self.jobs) == 0:
            return
        
        j = 0
        s = 0

        f = open(self.btracelog_fname, "r")
        for line in f.readlines():
            lst = line[:-1].split(",")
            if len(lst) <= 2:
                continue

            time = long(lst[0])  # vm up time (ms)
            heap = float(lst[1])  # jvm heap used (MB)
            event = lst[2]

            if event == "job":
                job = self.jobs[j]
                job_id = int(lst[4])
                assert job.job_id == job_id
                if lst[3] == "start":
                    if job.job_id == job_id:
                        job.start_time = time
                        job.start_heap = heap
                        s = 0
                elif lst[3] == "end":
                    if job.job_id == job_id:
                        job.end_time = time
                        job.end_heap = heap
                        j += 1
            elif event == "stage":
                job = self.jobs[j]
                stage = job.stages[s]
                if lst[3] == "start":
                    stage.start_time = time
                    stage.start_heap = heap
                elif lst[3] == "end":
                    stage.end_time = time
                    stage.end_heap = heap
                    s += 1
            elif event == "task":
                job = self.jobs[j]
                stage = job.stages[s]
                stage_id = int(lst[4])
                stage_attempt_id = int(lst[5])
                partition_index = int(lst[6].split(".")[0])
                task_attempt_id = int(lst[6].split(".")[1])
                assert stage.stage_id == stage_id
                assert stage.stage_attempt_id == stage_attempt_id
                if lst[3] == "start":
                    for task in stage.tasks:
                        if task.task_attempt_id == task_attempt_id and task.partition_index == partition_index:
                            task.start_time = time
                            task.start_heap = heap
                elif lst[3] == "end":
                    for task in stage.tasks:
                        if task.task_attempt_id == task_attempt_id and task.partition_index == partition_index:
                            task.end_time = time
                            task.end_heap = heap
            elif event == "persist":
                self.persists.append(Persist(lst))
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
            d.append( (job.start_time, job.start_heap, job, "start") )
            d.append( (job.end_time, job.end_heap, job, "end") )
            for stage in job.stages:
                d.append( (stage.start_time, stage.start_heap, stage, "start") )
                d.append( (stage.end_time, stage.end_heap, stage, "end") )
                for task in stage.tasks:
                    d.append( (task.start_time, task.start_heap, task, "start") )
                    d.append( (task.end_time, task.end_heap, task, "end") )
        for persist in self.persists:
            d.append( (persist.time, persist.heap, persist) )

        def comp(x,y):
            c = x[0] - y[0]
            if c < 0: return -1
            elif c > 0: return 1
            else:
                if isinstance(x[2], Persist) and isinstance(y[2], Task): return -1
                if isinstance(x[2], Persist) and isinstance(y[2], Stage): return -1
                if isinstance(x[2], Persist) and isinstance(y[2], Job): return -1
                if isinstance(x[2], Task) and isinstance(y[2], Stage): return -1
                if isinstance(x[2], Task) and isinstance(y[2], Job): return -1
                if isinstance(x[2], Stage) and isinstance(y[2], Job): return -1
                if isinstance(y[2], Persist) and isinstance(x[2], Task): return 1
                if isinstance(y[2], Persist) and isinstance(x[2], Stage): return 1
                if isinstance(y[2], Persist) and isinstance(x[2], Job): return 1
                if isinstance(y[2], Task) and isinstance(x[2], Stage): return 1
                if isinstance(y[2], Task) and isinstance(x[2], Job): return 1
                if isinstance(y[2], Stage) and isinstance(x[2], Job): return 1
                return 0

        d.sort(cmp=comp)

        # Print out collected btrace events before plotting them.
        for tup in d:
            if isinstance(tup[2], Job):
                print tup[2].get_driver_text(tup[3])
            elif isinstance(tup[2], Stage):
                print tup[2].get_driver_text(tup[3])
            elif isinstance(tup[2], Task):
                print tup[2].get_driver_text(tup[3])
            elif isinstance(tup[2], Persist):
                print tup[2].get_driver_text()
        print "\n"

        # Plot collected btrace events with different colors and size.
        x = [tup[0] for tup in d] 
        y = [tup[1] for tup in d]
        c = []
        s = 50
        for tup in d:
            if isinstance(tup[2], Job):
                c.append('y')
            elif isinstance(tup[2], Stage):
                c.append('g')
            elif isinstance(tup[2], Task):
                c.append('r')
            elif isinstance(tup[2], Persist):
                c.append('b')

        print colored("Yellow", 'yellow') + ": Job"
        print colored("Green ", 'green') + ": Stage"
        print colored("Red   ", 'red') + ": Task"
        print colored("Blue  ", 'blue') + ": Persist\n"

        print "[ Executor Memory ]"
        print "- Max JVM Heap Used = " + str(self.max_heap) + "(MB)"

        print "\n"

        # By clicking each of collected btrace events on the graph,
        # you can print out text associated with the event.
        def onclick(event):
            i = event.ind[0]
            if isinstance(d[i][2], Job):
                print d[i][2].get_driver_text(d[i][3])
            elif isinstance(d[i][2], Stage):
                print d[i][2].get_driver_text(d[i][3])
            elif isinstance(d[i][2], Task):
                print d[i][2].get_driver_text(d[i][3])
            elif isinstance(d[i][2], Persist):
                print d[i][2].get_driver_text()

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


