from tpcc_parameters import *
import shutil

groupId = "tpcc_logger_windowsize"

num_clients = args["clients"]

for windowsize_ms in xrange(1, 52, 10):
  parameters = {"windowsize_ms":windowsize_ms}
  kwargs["numUsers"] = args["clients"]
  kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"
  
  create_benchmark_logger("Logger", groupId, parameters, kwargs, windowsize_ms=windowsize_ms).run()
  create_benchmark_none("None", groupId, parameters, kwargs).run()
