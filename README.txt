Usage: parse <drvier | executor> eventlog_file btracelog_file


Example:

# Combine the Spark eventlog and driver's btracelog to see combined driver information.
$ bin/spark-glog driver sample_input/shuffle/master/eventlog sample_input/shuffle/master/btracelog

# Combine the Spark eventlog and executor's btracelog to see combined executor information.
$ bin/spark-glog executor sample_input/shuffle/master/eventlog sample_input/shuffle/slave1/btracelog
