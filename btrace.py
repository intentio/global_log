from matplotlib import pyplot as plt
import numpy as np

from eventlog import *

class DriverEvents:
    def __init__(self):
        self.jobs = None
        self.persists = []
        
        # Additional Information
        self.max_heap = None

class ExecutorEvents:
    def __init__(self):
        self.jobs = None
        self.persists = []
        self.shuffles = []
        
        # Additional Information
        self.max_heap = None

# time,heap,persist,<memory|disk|offheap>,size
class Persist:
    def __init__(self, args):
        self.time = long(args[0])
        self.heap = float(args[1])
        self.storage = args[3]
        self.size = long(args[4])

    def __repr__(self):
        return "[Persist " + str(self.storage) + "] " + self._adjust_size(self.size)

    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

    def get_driver_text(self):
        return str(self.time) + "(ms), " + str(self.heap) + "(MB) -- " + str(self)

    def get_executor_text(self):
        return str(self.time) + "(ms), " + str(self.heap) + "(MB) -- " + str(self)

# time,heap,shuffle,start,<map|sorter>
class ShuffleStart:
    def __init__(self, args):
        self.time = long(args[0])
        self.heap = float(args[1])
        self.type = args[4]
    def __repr__(self):
        return "[Shuffle start]"
    def get_executor_text(self):
        return str(self.time) + "(ms), " + str(self.heap) + "(MB) -- " + str(self)
# time,heap,shuffle,end,<map|sorter>
class ShuffleEnd:
    def __init__(self, args):
        self.time = long(args[0])
        self.heap = float(args[1])
        self.type = args[4]
    def __repr__(self):
        return "[Shuffle end]"
    def get_executor_text(self):
        return str(self.time) + "(ms), " + str(self.heap) + "(MB) -- " + str(self)
# time,heap,shuffle,spill,size
class ShuffleSpill:
    def __init__(self, args):
        self.time = long(args[0])
        self.heap = float(args[1])
        self.size = long(args[4])
    def __repr__(self):
        return "[Shuffle spill] " + self._adjust_size(self.size)
    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"
    def get_executor_text(self):
        return str(self.time) + "(ms), " + str(self.heap) + "(MB) -- " + str(self)


def parse_driver_btrace(fname, jobs):
    if len(jobs) == 0:
        return
    
    result = DriverEvents()

    j = 0
    s = 0

    f = open(fname, "r")
    for line in f.readlines():
        lst = line[:-1].split(",")
        if len(lst) <= 2:
            continue

        time = long(lst[0])  # vm up time (ms)
        heap = float(lst[1])  # jvm heap used (MB)
        event = lst[2]

        if event == "job":
            job = jobs[j]
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
            job = jobs[j]
            stage = job.stages[s]
            if lst[3] == "start":
                stage.start_time = time
                stage.start_heap = heap
            elif lst[3] == "end":
                stage.end_time = time
                stage.end_heap = heap
                s += 1
        elif event == "task":
            job = jobs[j]
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
            result.persists.append(Persist(lst))
    f.close()
    result.jobs = jobs
    return result


def plot_driver_btrace(fname, result):
    # Plot all the collected (time,heap) points.
    x, y = np.loadtxt(fname, unpack=True, delimiter=",")
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(x, y)
    ax.set_xlabel("Time (ms)")
    ax.set_ylabel("JVM Heap Used (MB)")
    result.max_heap = max(y)
    ax.text(0.05, 0.95, "Max JVM Heap Used = " + str(result_max_heap) + " (MB)", transform=ax.transAxes, verticalalignment='top')

    # Collect all the btrace events except periodic alarm function.
    d = []
    for job in result.jobs:
        d.append( (job.start_time, job.start_heap, job, "start") )
        d.append( (job.end_time, job.end_heap, job, "end") )
        for stage in job.stages:
            d.append( (stage.start_time, stage.start_heap, stage, "start") )
            d.append( (stage.end_time, stage.end_heap, stage, "end") )
            for task in stage.tasks:
                d.append( (task.start_time, task.start_heap, task, "start") )
                d.append( (task.end_time, task.end_heap, task, "end") )
    for persist in result.persists:
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

def parse_executor_btrace(fname, jobs):
    if len(jobs) == 0:
        return
    
    result = ExecutorEvents()

    j = 0
    s = 0

    tasks = {}
    for job in jobs:
        for stage in job.stages:
            for task in stage.tasks:
                tasks[task.task_id] = task

    f = open(fname, "r")
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
            result.persists.append(Persist(lst))
        elif event == "shuffle":
            if lst[3] == "start":
                result.shuffles.append(ShuffleStart(lst))
            elif lst[3] == "end":
                result.shuffles.append(ShuffleEnd(lst))
            elif lst[3] == "spill":
                result.shuffles.append(ShuffleSpill(lst))
    f.close()
    result.jobs = jobs
    return result

def plot_executor_btrace(fname, result):
    # Plot all the collected (time,heap) points.
    x, y = np.loadtxt(fname, unpack=True, delimiter=",")
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(x, y)
    ax.set_xlabel("Time (ms)")
    ax.set_ylabel("JVM Heap Used (MB)")
    result.max_heap = max(y)
    ax.text(0.05, 0.95, "Max JVM Heap Used = " + str(result.max_heap) + " (MB)", transform=ax.transAxes, verticalalignment='top')

    # Collect all the btrace events except periodic alarm function.
    d = []
    for job in result.jobs:
        for stage in job.stages:
            for task in stage.tasks:
                if task.start_time != None and task.end_time != None:
                    d.append( (task.start_time, task.start_heap, task, "start") )
                    d.append( (task.end_time, task.end_heap, task, "end") )
                    task.compute_gc_count_time()
    for persist in result.persists:
        d.append( (persist.time, persist.heap, persist) )

    for shuffle in result.shuffles:
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
        else:  # Persist, Shuffle*
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
        elif isinstance(tup[2], ShuffleStart) or isinstance(tup[2], ShuffleEnd):
            c.append('g')
        elif isinstance(tup[2], ShuffleSpill):
            c.append('y')

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
        else:
            print d[i][2].get_executor_text()

    ax.scatter(x, y, 50, c, picker=True)
    fig.canvas.mpl_connect('pick_event', onclick)
    plt.show()
