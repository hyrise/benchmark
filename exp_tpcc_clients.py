from tpcc_parameters import *
import shutil


groupId = "tpcc_clients_tmp"

minClients = 20
maxClients = 301
stepClients = 20
runs = 5

clients = [x for x in xrange(minClients, maxClients+1, stepClients)]
clients.append(10)
clients.append(5)
clients.append(1)

for num_clients in clients:
    for run in range(runs):
        parameters = {"numUsers":num_clients, "run":run}
        kwargs["numUsers"] = num_clients
        kwargs["hyriseDBPath"] = "/mnt/fusion/david/hyrise_persistency/"
    
        create_benchmark_none("None", groupId, parameters, kwargs).run()
        create_benchmark_logger("Logger-10ms", groupId, parameters, kwargs, windowsize_ms=10, checkpoint_interval_ms=0).run()
        # create_benchmark_logger("Logger-5ms", groupId, parameters, kwargs, windowsize_ms=5, checkpoint_interval_ms=0).run()
        create_benchmark_nvram("NVRAM", groupId, parameters, kwargs).run()
