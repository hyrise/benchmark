from tpcc_parameters import *
import os
import shutil

groupId = "tpcc_checkpoint_throughput_tmp"


kwargs["numUsers"] = args["clients"]
kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"

parameters = {"default":"yes"}

create_benchmark_nvram("NVRAM", groupId, parameters, kwargs).run()
create_benchmark_none("None", groupId, parameters, kwargs).run()

for checkpoint_interval_ms in xrange(1, 180002, 30000):
    parameters = {"checkpoint_interval":checkpoint_interval_ms}    
    create_benchmark_logger("Logger", groupId, parameters, kwargs, windowsize_ms=30, checkpoint_interval_ms=checkpoint_interval_ms).run()

    # hack to copy persistency dir
    # persistency_dir = os.path.join(kwargs["hyriseDBPath"], "persistency")
    # shutil.copytree(persistency_dir, "persistency-copy-"+str(checkpoint_interval_ms))
    