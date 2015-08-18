import json
import re

from common import *

##
# Job class comprises information from both Spark's eventlog and btracelog.
##
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
        self.start_common = None
        self.end_common = None

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
            return str(self.start_common.time) + "(ms), " + str(self.start_common.total) + "(MB) -- " + self._new_repr(status)
        elif status == "end":
            return str(self.end_common.time) + "(ms), " + str(self.end_common.total) + "(MB) -- " + self._new_repr(status)
