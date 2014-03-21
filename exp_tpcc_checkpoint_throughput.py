from tpcc_parameters import *
import os
import shutil

groupId = "tpcc_checkpoint_throughput_tmp"

for checkpoint_interval_ms in xrange(1, 180002, 10000):
    parameters = {"checkpoint_interval":checkpoint_interval_ms}
    kwargs["numUsers"] = args["clients"]
    kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"
    
    create_benchmark_none("None", groupId, parameters, kwargs).run()
    create_benchmark_logger("Logger", groupId, parameters, kwargs, windowsize_ms=30, checkpoint_interval_ms=checkpoint_interval_ms).run()
    create_benchmark_nvram("NVRAM", groupId, parameters, kwargs).run()
