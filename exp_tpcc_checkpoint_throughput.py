from tpcc_parameters import *
import os
import shutil

groupId = "tpcc_checkpoint_throughput_tmp"

for checkpoint_interval in xrange(1, 240002, 6000):
    parameters = {"checkpoint_interval":checkpoint_interval}
    kwargs["numUsers"] = args["clients"]
    kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"
    
    kwargs["checkpointInterval"] = 0
    create_benchmark_none("None", groupId, parameters, kwargs).run()
    
    kwargs["checkpointInterval"] = checkpoint_interval
    create_benchmark_logger("Logger", groupId, parameters, kwargs, windowsize_ms=50000).run()
    
    kwargs["checkpointInterval"] = 0
    create_benchmark_nvram("NVRAM", groupId, parameters, kwargs).run()
