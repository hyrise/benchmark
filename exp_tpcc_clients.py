from tpcc_parameters import *

groupId = "tpcc_clients_tmp"
num_clients = args["clients"]
minClients = args["clients_min"]
maxClients = args["clients_max"]
stepClients = args["clients_step"]

if args["clients"] > 0:
    minClients = args["clients"]
    maxClients = args["clients"]

for num_clients in xrange(minClients, maxClients+1, stepClients):

    runId = "numClients_%s" % num_clients
    kwargs["numUsers"] = num_clients

    b1 = benchmark.TPCCBenchmark(groupId, runId, s1, **kwargs)
    # b2 = benchmark.TPCCBenchmark(groupId, runId, s2, **kwargs)
    # b3 = benchmark.TPCCBenchmark(groupId, runId, s3, **kwargs)
    # b4 = benchmark.TPCCBenchmark(groupId, runId, s4, **kwargs)
    # b5 = benchmark.TPCCBenchmark(groupId, runId, s5, **kwargs)
    # b6 = benchmark.TPCCBenchmark(groupId, runId, s6, **kwargs)
    

    b1.run()
    # b2.run()
    # b3.run()
    # b4.run()
    # b5.run()
    # b6.run()
    
    if os.path.exists("/mnt/pmfs/hyrise_tpcc"):
        os.remove("/mnt/pmfs/hyrise_tpcc")

#plotter = benchmark.Plotter(groupId)
#plotter.printStatistics()

