import sys
sys.path.insert(0, 'src')

from driver_log import *
from executor_log import *

def plot_driver(eventlog, btracelog):
    dl = DriverLog(eventlog, btracelog)
    dl.parse()
    dl.plot()
    #print dl.jobs[0].stages[1].tasks[6].shuffle_metrics

def plot_executor(eventlog, btracelog):
    el = ExecutorLog(eventlog, btracelog)
    el.parse()
    el.plot()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Usage: parse type eventlog_file btracelog_file"
        print "type:"
        print "    driver      combine driver's eventlog file and btracelog file to plot a graph."
        print "    executor    combine executor's eventlog file and btracelog file to plot a graph."
        exit()
    if sys.argv[1] != "driver" and sys.argv[1] != "executor":
        print "Usage: parse type eventlog_file btracelog_file"
        print "Type:"
        print "    driver      combine driver's eventlog file and btracelog file to plot a graph."
        print "    executor    combine executor's eventlog file and btracelog file to plot a graph."
        exit()
    if sys.argv[1] == "driver":
        plot_driver(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "executor":
        plot_executor(sys.argv[2], sys.argv[3])
