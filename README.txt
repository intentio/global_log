Usage: spark-glog <drvier | executor> eventlog_file btracelog_file


Example:

# Combine the Spark eventlog and driver's btracelog to see combined driver information.
$ bin/spark-glog driver sample_input/app-eventlog sample_input/driver.btrace

# Combine the Spark eventlog and executor's btracelog to see combined executor information.
$ bin/spark-glog executor sample_input/app-eventlog sample_input/slave1/slave1.btrace

$ bin/spark-glog dir sample_input
