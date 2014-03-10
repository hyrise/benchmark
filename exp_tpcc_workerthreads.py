from tpcc_parameters import *

groupId = "tpcc_threads_tmp"
num_clients = args["clients"]
minClients = args["clients_min"]
maxClients = args["clients_max"]

if args["clients"] > 0:
    minClients = args["clients"]
    maxClients = args["clients"]

for threads in xrange(1, 19):

    print "benchmarking threads: ", threads
    kwargs["serverThreads"] = threads
    kwargs["numUsers"] = num_clients
    parameters = {"numClients":num_clients}    
    create_benchmark_none("None", groupId, parameters, kwargs).run()
