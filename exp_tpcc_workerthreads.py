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

    runId = "numClients_%s" % num_clients
    kwargs["numUsers"] = num_clients

    b1 = benchmark.TPCCBenchmark(groupId, runId, s1, **kwargs)
    
    b1.run()
    
    if os.path.exists("/mnt/pmfs/hyrise_tpcc"):
        os.remove("/mnt/pmfs/hyrise_tpcc")