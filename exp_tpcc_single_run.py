from tpcc_parameters import *
import shutil

groupId = "tpcc_single_run"
num_clients = args["clients"]
parameters = {}
kwargs["numUsers"] = args["clients"]
kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"

create_benchmark_nvram("NVRAM", groupId, parameters, kwargs).run()
create_benchmark_logger("Logger", groupId, parameters, kwargs, windowsize_ms=30, checkpoint_interval_ms=0).run()
create_benchmark_none("None", groupId, parameters, kwargs).run()

