import os
import threading
import subprocess
from tpcc_parameters import *

if args["genCount"] == None:
  print "Please specify number of queries to generate."
  exit(0)
if args["genFile"] == None:
  print "Please specify file to save the generated queries."
  exit(0)

args["genFile"] = os.path.abspath(args["genFile"])
print "Creating file with generated queries:", args["genFile"]
print "Create queries:", args["genCount"]

groupId = "tpcc_" + args["genFile"]
num_clients = 1
runId = "numClients_%s" % num_clients
kwargs["numUsers"] = num_clients
kwargs["write_to_file"] = args["genFile"]
kwargs["write_to_file_count"] = args["genCount"]
b1 = benchmark.TPCCBenchmark(groupId, runId, s1, **kwargs)
b1.run()