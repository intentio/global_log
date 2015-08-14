import sys

from eventlog import *
from btrace import *

def plot_driver(eventlog, btracelog):
    jobs = parse_eventlog(eventlog)
    print jobs[0].stages[1].tasks[6].shuffle_metrics
    result = parse_driver_btrace(btracelog, jobs)
    plot_driver_btrace(btracelog, result)

def plot_executor(eventlog, btracelog):
    jobs = parse_eventlog(eventlog)
    result = parse_executor_btrace(btracelog, jobs)
    plot_executor_btrace(btracelog, result)

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
