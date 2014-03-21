from tpcc_parameters import *
import shutil

groupId = "tpcc_clients_tmp"
num_clients = args["clients"]
minClients = args["clients_min"]
maxClients = args["clients_max"]
stepClients = args["clients_step"]


if args["clients"] > 0:
    minClients = args["clients"]
    maxClients = args["clients"]

for num_clients in xrange(minClients, maxClients+1, stepClients):

    parameters = {"numUsers":num_clients}
    kwargs["numUsers"] = num_clients
    kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"

    create_benchmark_none("None", groupId, parameters, kwargs).run()
    create_benchmark_logger("Logger-30ms", groupId, parameters, kwargs, windowsize_ms=30000, checkpoint_interval_ms=0).run()
    create_benchmark_nvram("NVRAM", groupId, parameters, kwargs).run()
