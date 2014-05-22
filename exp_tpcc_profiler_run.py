from tpcc_parameters import *
import shutil

groupId = "tpcc_single_run"
num_clients = args["clients"]
parameters = {}
kwargs["numUsers"] = args["clients"]
kwargs["profiler"] = "sampler"
kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"

# create_benchmark_nvram("NVRAM", groupId, parameters, kwargs).run()
create_benchmark_none("None", groupId, parameters, kwargs).run()

