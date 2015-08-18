from matplotlib import pyplot as plt
import numpy as np
from termcolor import colored

from event.common import *
from event.environment import *
from event.job import *
from event.stage import *
from event.task import *
from event.persist import *

class DriverLog:
    def __init__(self, eventlog_fname, btracelog_fname):
        self.eventlog_fname = eventlog_fname
        self.btracelog_fname = btracelog_fname
        
        self.environment = None
        self.jobs = []
        self.persists = []
        self.time_sorted_combined_events = []

        self.max_memory = None

    def parse(self):
        self.parse_eventlog()
        self.parse_btracelog()

    def parse_btracelog(self):
        common_length = 4
        if len(self.jobs) == 0: return
        j = 0
        s = 0
        f = open(self.btracelog_fname, "r")
        for line in f.readlines():
            lst = line[:-1].split(",")
            if len(lst) <= 4:
                continue
            common = Common(lst[:common_length])
            event = lst[common_length]
            if event == "job":
                job = self.jobs[j]
                job_id = int(lst[common_length+2])
                assert job.job_id == job_id
                if lst[common_length+1] == "start":
                    if job.job_id == job_id:
                        job.start_common = common
                        s = 0
                elif lst[common_length+1] == "end":
                    if job.job_id == job_id:
                        job.end_common = common
                        j += 1
            elif event == "stage":
                job = self.jobs[j]
                stage = job.stages[s]
                if lst[common_length+1] == "start":
                    stage.start_common = common
                elif lst[common_length+1] == "end":
                    stage.end_common = common
                    s += 1
            elif event == "task":
                job = self.jobs[j]
                stage = job.stages[s]
                stage_id = int(lst[common_length+2])
                stage_attempt_id = int(lst[common_length+3])
                partition_index = int(lst[common_length+4].split(".")[0])
                task_attempt_id = int(lst[common_length+4].split(".")[1])
                assert stage.stage_id == stage_id
                assert stage.stage_attempt_id == stage_attempt_id
                if lst[common_length+1] == "start":
                    for task in stage.tasks:
                        if task.task_attempt_id == task_attempt_id and task.partition_index == partition_index:
                            task.start_common = common
                elif lst[common_length+1] == "end":
                    for task in stage.tasks:
                        if task.task_attempt_id == task_attempt_id and task.partition_index == partition_index:
                            task.end_common = common
            elif event == "persist":
                self.persists.append(Persist(common, lst[common_length:]))
        f.close()

    def combine(self):
        # Collect all the btrace events except periodic alarm function.
        self.time_sorted_combined_events = []
        for job in self.jobs:
            self.time_sorted_combined_events.append( (job.start_common.time, job, "start") )
            self.time_sorted_combined_events.append( (job.end_common.time, job, "end") )
            for stage in job.stages:
                self.time_sorted_combined_events.append( (stage.start_common.time, stage, "start") )
                self.time_sorted_combined_events.append( (stage.end_common.time, stage, "end") )
                for task in stage.tasks:
                    self.time_sorted_combined_events.append( (task.start_common.time, task, "start") )
                    self.time_sorted_combined_events.append( (task.end_common.time, task, "end") )
        for persist in self.persists:
            self.time_sorted_combined_events.append( (persist.common.time, persist) )

        def _comp(x,y):
            c = x[0] - y[0]
            if c < 0: return -1
            elif c > 0: return 1
            else:
                if isinstance(x[1], Persist) and isinstance(y[1], Task): return -1
                if isinstance(x[1], Persist) and isinstance(y[1], Stage): return -1
                if isinstance(x[1], Persist) and isinstance(y[1], Job): return -1
                if isinstance(x[1], Task) and isinstance(y[1], Stage): return -1
                if isinstance(x[1], Task) and isinstance(y[1], Job): return -1
                if isinstance(x[1], Stage) and isinstance(y[1], Job): return -1
                if isinstance(y[1], Persist) and isinstance(x[1], Task): return 1
                if isinstance(y[1], Persist) and isinstance(x[1], Stage): return 1
                if isinstance(y[1], Persist) and isinstance(x[1], Job): return 1
                if isinstance(y[1], Task) and isinstance(x[1], Stage): return 1
                if isinstance(y[1], Task) and isinstance(x[1], Job): return 1
                if isinstance(y[1], Stage) and isinstance(x[1], Job): return 1
                return 0

        self.time_sorted_combined_events.sort(cmp=_comp)


    def plot(self):
        # Plot all the collected (time,heap) points.
        x, y = np.loadtxt(self.btracelog_fname, unpack=True, delimiter=",", usecols=(0,3))
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(x, y)
        ax.set_xlabel("Time (ms)")
        ax.set_ylabel("JVM Memory Used (MB)")
        self.max_memory = max(y)
        ax.text(0.05, 0.95, "Max JVM Memory Used = " + str(self.max_memory) + " (MB)", transform=ax.transAxes, verticalalignment='top')

        # Plot collected btrace events with different colors and size.
        x = [tup[0] for tup in self.time_sorted_combined_events]  # time list
        y = []  # memory list
        for tup in self.time_sorted_combined_events:
            if isinstance(tup[1], Job) or isinstance(tup[1], Stage) or isinstance(tup[1], Task):
                if tup[2] == "start": y.append(tup[1].start_common.total)
                elif tup[2] == "end": y.append(tup[1].end_common.total)
            else:
                y.append(tup[1].common.total)
        c = []
        s = 50
        for tup in self.time_sorted_combined_events:
            if isinstance(tup[1], Job):
                c.append('y')
            elif isinstance(tup[1], Stage):
                c.append('g')
            elif isinstance(tup[1], Task):
                c.append('r')
            elif isinstance(tup[1], Persist):
                c.append('b')

        print colored("Yellow", 'yellow') + ": Job"
        print colored("Green ", 'green') + ": Stage"
        print colored("Red   ", 'red') + ": Task"
        print colored("Blue  ", 'blue') + ": Persist\n"

        self.print_driver_info()

        # By clicking each of collected btrace events on the graph,
        # you can print out text associated with the event.
        def onclick(event):
            i = event.ind[0]
            cls = self.time_sorted_combined_events[i][1]
            if isinstance(cls, Job):
                print cls.get_driver_text(self.time_sorted_combined_events[i][2])
            elif isinstance(cls, Stage):
                print cls.get_driver_text(self.time_sorted_combined_events[i][2])
            elif isinstance(cls, Task):
                print cls.get_driver_text(self.time_sorted_combined_events[i][2])
            elif isinstance(cls, Persist):
                print cls.get_driver_text()

        ax.scatter(x, y, 50, c, picker=True)
        fig.canvas.mpl_connect('pick_event', onclick)
        plt.show()

    def print_time_sorted_combined_events(self):
        for tup in self.time_sorted_combined_events:
            if isinstance(tup[1], Job):
                print tup[1].get_driver_text(tup[2])
            elif isinstance(tup[1], Stage):
                print tup[1].get_driver_text(tup[2])
            elif isinstance(tup[1], Task):
                print tup[1].get_driver_text(tup[2])
            elif isinstance(tup[1], Persist):
                print tup[1].get_driver_text()

    def print_driver_info(self):
        temp = ""
        for job in self.jobs:
            temp += "Job " + str(job.job_id)
            for stage in job.stages:
                temp += "\n  Stage " + str(stage.stage_id) + "\n    Task [ "
                for task in stage.tasks:
                    if task.start_common != None and task.end_common != None:
                        temp += str(task.task_id) + ","
                temp = temp[:-1]
                temp += " ]"
        print temp
        print ""

        if self.environment is not None:
            print "[ Driver Information ]"
            print "- Driver Memory = " + self._adjust_size(self.environment.driver_memory)
            print "- Max Driver Memory Used = " + str(self.max_memory) + "(MB)"
            print "\n"


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
            if j["Event"] == "SparkListenerEnvironmentUpdate":
                self.environment = Environment(j)
            elif j["Event"] == "SparkListenerJobStart":
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
    dl = DriverLog(sys.argv[1], sys.argv[2])
    dl.parse()
    dl.combine()

    dl.print_time_sorted_combined_events()
    print "\n"

    dl.plot()
