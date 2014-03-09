from tpcc_parameters import *
import os
import shutil

groupId = "tpcc_checkpoint_throughput_tmp"

for checkpoint_interval in xrange(1, 60002, 6000):

    runId = "checkpoint_throughput_%s" % checkpoint_interval
    kwargs["numUsers"] = args["clients"]
    kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"

    # no persistency
    kwargs["checkpointInterval"] = 0
    b1 = benchmark.TPCCBenchmark(groupId, runId, s1, **kwargs)

    # buffered logger 50ms group commit
    kwargs["checkpointInterval"] = checkpoint_interval
    b4 = benchmark.TPCCBenchmark(groupId, runId, s4, **kwargs)

    # clear persistency directory
    if os.path.exists(kwargs["hyriseDBPath"]) and not args["manual"]:
        print "Deleting directory:", kwargs["hyriseDBPath"]
        shutil.rmtree(kwargs["hyriseDBPath"])

    # run no persistency
    b1.run()

    # clear persistency directory
    if os.path.exists(kwargs["hyriseDBPath"]) and not args["manual"]:
        print "Deleting directory:", kwargs["hyriseDBPath"]
        shutil.rmtree(kwargs["hyriseDBPath"])

    # run buffered logger 50ms
    b4.run()
