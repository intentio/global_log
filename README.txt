Usage: parse <drvier | executor> eventlog_file btracelog_file

If you want to combine the sample driver's logs and plot a graph, type:
    $ ./parse driver sample_input/simple/master/eventlog sample_input/simple/master/btracelog
or
    $ ./parse driver sample_input/shuffle/master/eventlog sample_input/shuffle/master/btracelog

If you want to combine one of executors' sample logs and plot a graph, type:
    $ ./parse executor sample_input/simple/master/eventlog sample_input/simple/slave3/btracelog
or
    $ ./parse executor sample_input/shuffle/master/eventlog sample_input/shuffle/slave1/btracelog
