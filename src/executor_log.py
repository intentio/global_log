from matplotlib import pyplot as plt
import numpy as np
from termcolor import colored

from event.common import *
from event.gc import *
from event.environment import *
from event.job import *
from event.stage import *
from event.task import *
from event.persist import *
from event.shuffle import *

class ExecutorLog:
    def __init__(self, eventlog_fname, btracelog_fname):
        self.eventlog_fname = eventlog_fname
        self.btracelog_fname = btracelog_fname

        self.environment = None
        self.jobs = []
        self.persists = []
        self.shuffles = []
        self.time_sorted_combined_events = []

        self.executor_id = None
        self.max_memory = None

    def parse(self):
        self.parse_eventlog()
        self.parse_btracelog()

    def parse_btracelog(self):
        common_length = 4
        if len(self.jobs) == 0: return
        j = 0
        s = 0
        tasks = {}
        for job in self.jobs:
            for stage in job.id_sorted_stages:
                for task in stage.id_sorted_tasks:
                    tasks[task.task_id] = task
        temp_shuffles = []
        f = open(self.btracelog_fname, "r")
        for line in f.readlines():
            lst = line[:-1].split(",")
            if len(lst) <= 4:
                continue
            common = Common(lst[:common_length])
            event = lst[common_length]
            if event == "task":
                task_id = int(lst[common_length+2])
                assert tasks.has_key(task_id)
                task = tasks[task_id]
                self.executor_id = task.executor_id
                if lst[common_length+1] == "start":
                    task.start_common = common
                    task.start_gc = GC(lst[common_length+3:])
                elif lst[common_length+1] == "end":
                    task.end_common = common
                    task.end_gc = GC(lst[common_length+3:])
            elif event == "persist":
                self.persists.append(Persist(common, lst[common_length:]))
            elif event == "shuffle":
                temp_shuffles.append(Shuffle(common, lst[common_length:]))
        f.close()
        self._truncate_duplicate_shuffle_events(temp_shuffles)

    def _truncate_duplicate_shuffle_events(self, temp_shuffles):
        num_dup = 2
        depth = 0
        spill_count = 0
        prev_spill = None
        for shuffle in temp_shuffles:
            if shuffle.method == "start":
                depth += 1
                if depth == 1:
                    self.shuffles.append(shuffle)
            elif shuffle.method == "spill":
                spill_count += 1
                if spill_count > 1:
                    assert prev_spill.size == shuffle.size
                if spill_count == num_dup:
                    self.shuffles.append(shuffle)
                    spill_count = 0
                prev_spill = shuffle
            elif shuffle.method == "release":
                self.shuffles.append(shuffle)
            elif shuffle.method == "end":
                depth -= 1
                if depth == 0:
                    self.shuffles.append(shuffle)

    def combine(self):
        # Collect all the btrace events except periodic alarm function.
        self.time_sorted_combined_events = []
        for job in self.jobs:
            for stage in job.id_sorted_stages:
                for task in stage.id_sorted_tasks:
                    if task.start_common != None and task.end_common != None:
                        self.time_sorted_combined_events.append( (task.start_common.time, task, "start") )
                        self.time_sorted_combined_events.append( (task.end_common.time, task, "end") )

        for persist in self.persists:
            self.time_sorted_combined_events.append( (persist.common.time, persist) )

        for shuffle in self.shuffles:
            self.time_sorted_combined_events.append( (shuffle.common.time, shuffle) )

        def _comp(x,y):
            c = x[0] - y[0]
            if c < 0: return -1
            elif c > 0: return 1
            else: return 0

        self.time_sorted_combined_events.sort(cmp=_comp)


    def plot(self):
        # Plot all the collected (time,memory) points.
        x, y = np.loadtxt(self.btracelog_fname, unpack=True, delimiter=",", usecols=(0,3))
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(x, y)
        ax.set_xlabel("Time (ms)")
        ax.set_ylabel("JVM Memory Used (MB)")
        self.max_memory = max(y)
        ax.text(0.05, 0.95, "Max JVM Memory Used = " + str(self.max_memory) + " (MB)", transform=ax.transAxes, verticalalignment='top')

        # Plot collected events with different colors.
        x = [tup[0] for tup in self.time_sorted_combined_events]  # time list
        y = []  # memory list
        for tup in self.time_sorted_combined_events:
            if isinstance(tup[1], Job) or isinstance(tup[1], Stage) or isinstance(tup[1], Task):
                if tup[2] == "start": y.append(tup[1].start_common.total)
                elif tup[2] == "end": y.append(tup[1].end_common.total)
            else:
                y.append(tup[1].common.total)
        c = []  # color list
        s = 50
        for tup in self.time_sorted_combined_events:
            if isinstance(tup[1], Task):
                c.append('r')
            elif isinstance(tup[1], Persist):
                c.append('b')
            elif isinstance(tup[1], Shuffle):
                if tup[1].method == "start" or tup[1].method == "end":
                    c.append('g')
                else:
                    c.append('y')

        print colored("Red   ", 'red') + ": Task"
        print colored("Blue  ", 'blue') + ": Persist"
        print colored("Green ", 'green') + ": Shuffle start/end"
        print colored("Yellow", 'yellow') + ": Shuffle spill/release\n"

        self.print_executor_info()

        # By clicking each of collected btrace events on the graph,
        # you can print out text associated with the event.
        def onclick(event):
            i = event.ind[0]
            cls = self.time_sorted_combined_events[i][1]
            if isinstance(cls, Task):
                print cls.get_executor_text(self.time_sorted_combined_events[i][2])
            elif isinstance(cls, Persist):
                print cls.get_executor_text()
            elif isinstance(cls, Shuffle):
                print cls.get_executor_text()

        ax.scatter(x, y, 50, c, picker=True)
        fig.canvas.mpl_connect('pick_event', onclick)
        plt.show()

    def print_time_sorted_combined_events(self):
        for tup in self.time_sorted_combined_events:
            if isinstance(tup[1], Task):
                print tup[1].get_executor_text(tup[2])
            else:  # Persist, Shuffle
                print tup[1].get_executor_text()

    def print_executor_info(self):
        temp = ""
        for job in self.jobs:
            temp += "\nJob " + str(job.job_id)
            for stage in job.id_sorted_stages:
                temp += "\n  Stage " + str(stage.stage_id) + "\n    Task [ "
                for task in stage.id_sorted_tasks:
                    if task.start_common != None and task.end_common != None:
                        temp += str(task.task_id) + ","
                temp = temp[:-1]
                temp += " ]"
        print temp
        print ""

        if self.environment is not None:
            print "[ Executor Information ]"
            print "- Executor ID: " + str(self.executor_id)
            print "- Executor Memory = " + self._adjust_size(self.environment.executor_memory)
            print "- Max JVM Memory Used = " + str(self.max_memory) + "(MB)"
            print ""

            cinfo = CacheInfo(self.environment.storage_memory, self.persists)
            print cinfo
            print ""
            sinfo = ShuffleInfo(self.environment.shuffle_memory, self.shuffles)
            print sinfo
            print ""

        for i in range(len(self.time_sorted_combined_events)):
            j = len(self.time_sorted_combined_events) - i - 1
            tup = self.time_sorted_combined_events[j]
            if isinstance(tup[1], Task):
                task = tup[1]
                break
        print "[ Garbage Collector Information ]"
        print "- GC Count = " + str(task.end_gc.count)
        print "- GC Time  = " + str(task.end_gc.time) + "(ms)"
        print "\n"


    ####
    # Returns the list of jobs in which each of jobs contains all the
    # stages and tasks that have ran during the job.
    def parse_eventlog(self):
        last_job = None
        f = open(self.eventlog_fname, "r")
        for line in f.readlines():
            j = json.loads(line)
            if j["Event"] == "SparkListenerEnvironmentUpdate":
                self.environment = Environment(j)
            elif j["Event"] == "SparkListenerJobStart":
                job = Job(j)
                last_job = job
            elif j["Event"] == "SparkListenerJobEnd":
                assert last_job.job_id == j["Job ID"]
                last_job.add_end(j)
                self.jobs.append(last_job)
            elif j["Event"] == "SparkListenerStageSubmitted":
                stage_id = j["Stage Info"]["Stage ID"]
                stage_attempt_id = j["Stage Info"]["Stage Attempt ID"]
                last_job.stages[(stage_id, stage_attempt_id)] = Stage(j)
            elif j["Event"] == "SparkListenerStageCompleted":
                stage_id = j["Stage Info"]["Stage ID"]
                stage_attempt_id = j["Stage Info"]["Stage Attempt ID"]
                assert last_job.stages.has_key((stage_id, stage_attempt_id))
                last_job.stages[(stage_id, stage_attempt_id)].add_end(j)
            elif j["Event"] == "SparkListenerTaskStart":
                stage_id = j["Stage ID"]
                stage_attempt_id = j["Stage Attempt ID"]
                assert last_job.stages.has_key((stage_id, stage_attempt_id))
                stage = last_job.stages[(stage_id, stage_attempt_id)]
                task_id = j["Task Info"]["Task ID"]
                task_attempt_id = j["Task Info"]["Attempt"]
                stage.tasks[(task_id, task_attempt_id)] = Task(j)
            elif j["Event"] == "SparkListenerTaskEnd":
                stage_id = j["Stage ID"]
                stage_attempt_id = j["Stage Attempt ID"]
                assert last_job.stages.has_key((stage_id, stage_attempt_id))
                stage = last_job.stages[(stage_id, stage_attempt_id)]
                task_id = j["Task Info"]["Task ID"]
                task_attempt_id = j["Task Info"]["Attempt"]
                assert stage.tasks.has_key((task_id, task_attempt_id))
                stage.tasks[(task_id, task_attempt_id)].add_end(j)
        f.close()
        # Compute additional stuff for each of Stages.
        for job in self.jobs:
            for stage in job.stages.values():
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
    el.combine()

    el.print_time_sorted_combined_events()
    print "\n"

    el.plot()
