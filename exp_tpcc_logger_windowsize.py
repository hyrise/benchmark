from tpcc_parameters import *
import shutil

groupId = "tpcc_logger_windowsize"

num_clients = args["clients"]

for windowsize_ms in xrange(1, 27, 4):
  parameters = {"windowsize_ms":windowsize_ms}
  kwargs["numUsers"] = args["clients"]
  kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"
  
  create_benchmark_logger("Logger", groupId, parameters, kwargs, windowsize_ms=windowsize_ms, checkpoint_interval_ms=0).run()
  create_benchmark_none("None", groupId, parameters, kwargs).run()
