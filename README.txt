Usage: spark-glog <drvier | executor> eventlog_file btracelog_file


Example:

# Combine the Spark eventlog and driver's btracelog to see combined driver information.
$ bin/spark-glog driver sample_input/master/eventlog sample_input/master/btracelog

# Combine the Spark eventlog and executor's btracelog to see combined executor information.
$ bin/spark-glog executor sample_input/master/eventlog sample_input/slave1/btracelog
