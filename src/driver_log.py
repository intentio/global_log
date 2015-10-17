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
        self.time_xmax = None
        self.avg_cpu_load = None


    def run(self):
        # parse logs
        self.parse()
        # combine parsed logs
        self.combine()
        self.print_time_sorted_combined_events()
        print "\n"
        print colored("Yellow", 'yellow') + ": Job"
        print colored("Green ", 'green') + ": Stage"
        print colored("Red   ", 'red') + ": Task"
        print colored("Blue  ", 'blue') + ": Persist\n"
        self.print_driver_info()
        self.plot()


    def parse(self):
        self.parse_eventlog()
        self.parse_btracelog()


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


    def parse_btracelog(self):
        common_length = 6
        if len(self.jobs) == 0: return
        j = 0
        s = 0
        f = open(self.btracelog_fname, "r")
        for line in f.readlines():
            lst = line[:-1].split(",")
            if len(lst) <= common_length:
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
                stage = job.id_sorted_stages[s]
                if lst[common_length+1] == "start":
                    stage.start_common = common
                elif lst[common_length+1] == "end":
                    stage.end_common = common
                    s += 1
            elif event == "task":
                job = self.jobs[j]
                stage = job.id_sorted_stages[s]
                stage_id = int(lst[common_length+2])
                stage_attempt_id = int(lst[common_length+3])
                partition_index = int(lst[common_length+4].split(".")[0])
                task_attempt_id = int(lst[common_length+4].split(".")[1])
                assert stage.stage_id == stage_id
                assert stage.stage_attempt_id == stage_attempt_id
                if lst[common_length+1] == "start":
                    for task in stage.id_sorted_tasks:
                        if task.task_attempt_id == task_attempt_id and task.partition_index == partition_index:
                            task.start_common = common
                elif lst[common_length+1] == "end":
                    for task in stage.id_sorted_tasks:
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
            for stage in job.id_sorted_stages:
                self.time_sorted_combined_events.append( (stage.start_common.time, stage, "start") )
                self.time_sorted_combined_events.append( (stage.end_common.time, stage, "end") )
                for task in stage.id_sorted_tasks:
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
        self.time_xmax = self.time_sorted_combined_events[-1][0]


    def plot(self):
        time, memory, cpu = np.loadtxt(self.btracelog_fname, unpack=True, delimiter=",", usecols=(0,3,4))

        fig = plt.figure()
        time_memory = fig.add_subplot(211, xlim=(0,self.time_xmax + time[-1]/20))
        time_memory.plot(time, memory)
        time_memory.set_xlabel("Time (ms)")
        time_memory.set_ylabel("JVM Memory Used (MB)")
        self.max_memory = max(memory)
        time_memory.text(0.05, 0.95, "Max JVM Memory Used = " + str(self.max_memory) + " (MB)", transform=time_memory.transAxes, verticalalignment='top')

        time_cpu = fig.add_subplot(212, xlim=(0,self.time_xmax + time[-1]/20))
        time_cpu.plot(time, cpu, 'k')
        time_cpu.set_xlabel("Time (ms)")
        time_cpu.set_ylabel("JVM CPU Load")
        self.avg_cpu_load = sum(cpu) / len(cpu)
        time_cpu.text(0.05, 0.95, "Avg JVM CPU Load = " + str(self.avg_cpu_load), transform=time_cpu.transAxes, verticalalignment='top')

        x = [tup[0] for tup in self.time_sorted_combined_events]  # event time list
        y = []  # event memory list
        z = []  # event cpu list
        for tup in self.time_sorted_combined_events:
            if isinstance(tup[1], Job) or isinstance(tup[1], Stage) or isinstance(tup[1], Task):
                if tup[2] == "start":
                    y.append(tup[1].start_common.total)
                    z.append(tup[1].start_common.pcpu)
                elif tup[2] == "end":
                    y.append(tup[1].end_common.total)
                    z.append(tup[1].end_common.pcpu)
            else:
                y.append(tup[1].common.total)
                z.append(tup[1].common.pcpu)
        c = []
        for tup in self.time_sorted_combined_events:
            if isinstance(tup[1], Job):
                c.append('y')
            elif isinstance(tup[1], Stage):
                c.append('g')
            elif isinstance(tup[1], Task):
                c.append('r')
            elif isinstance(tup[1], Persist):
                c.append('b')

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

        time_memory.scatter(x, y, 50, c, picker=True)
        time_cpu.scatter(x, z, 50, c, picker=True)
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
            print "[ Driver Information ]"
            print "- Driver Memory = " + self._adjust_size(self.environment.driver_memory)
            print "- Max Driver Memory Used = " + str(self.max_memory) + "(MB)"
            print "\n"

    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"


import sys
if __name__ == "__main__":
    dl = DriverLog(sys.argv[1], sys.argv[2])
    dl.run()
