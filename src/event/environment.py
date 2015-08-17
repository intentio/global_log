import json
import re

##
# Environment class comprises information from Spark's eventlog.
##
class Environment:
    def __init__(self, j):
        self.driver_memory = None  # in Byte
        if "spark.driver.memory" in j["Spark Properties"]:
            self.driver_memory = self.convert_memory_string(j["Spark Properties"]["spark.driver.memory"])
        else:
            self.driver_memory = 512 * 1024 * 1024  # default

        self.executor_memory = None  # in Byte
        if "spark.executor.memory" in j["Spark Properties"]:
            self.executor_memory = self.convert_memory_string(j["Spark Properties"]["spark.executor.memory"])
        else:
            self.executor_memory = 512 * 1024 * 1024  # default

        self.storage_memory_fraction = None
        if "spark.storage.memoryFraction" in j["Spark Properties"]:
            self.storage_memory_fraction = float(j["Spark Properties"]["spark.storage.memoryFraction"])
        else:
            self.storage_memory_fraction = 0.6  # default

        self.shuffle_memory_fraction = None
        if "spark.shuffle.memoryFraction" in j["Spark Properties"]:
            self.shuffle_memory_fraction = float(j["Spark Properties"]["spark.shuffle.memoryFraction"])
        else:
            self.shuffle_memory_fraction = 0.2  # default

        self.storage_safety_fraction = 0.9
        self.shuffle_safety_fraction = 0.8

        self.storage_memory = self.executor_memory * self.storage_memory_fraction * self.storage_safety_fraction
        self.shuffle_memory = self.executor_memory * self.shuffle_memory_fraction * self.shuffle_safety_fraction

    def convert_memory_string(self, s):
        if "m" in s or "M" in s:
            return float(s[:-1]) * 1024 * 1024
        if "g" in s or "G" in s:
            return float(s[:-1]) * 1024 * 1024 * 1024
