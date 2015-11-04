from os import listdir
from os.path import isfile, isdir, join

from driver_log import *
from executor_log import *

def main(p):
    eventlog = ""
    master_btrace = ""
    for f in listdir(p):
        if f.split("-")[0] == "app":
            eventlog = join(p,f)
        if f.split(".")[-1] == "btrace":
            master_btrace = join(p,f)

    execdirs = sorted([ d for d in listdir(p) if isdir(join(p,d)) ])

    slave_btraces = []
    for d in execdirs:
        for f in listdir(join(p,d)):
            fname = join(p,d,f)
            if isfile(fname) and fname.split(".")[-1] == "btrace":
                slave_btraces.append(fname)

    dl = DriverLog(eventlog, master_btrace)
    dl.run(p)

    for s in slave_btraces:
        el = ExecutorLog(eventlog, s)
        el.run(s)

import sys
if __name__ == "__main__":
    main(sys.argv[1])
