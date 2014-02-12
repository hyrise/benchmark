import argparse
import benchmark
from benchmark.plotter_tatp import Plotter as TATPPlotter
import os
import getpass
from benchmark.plotter_tatp import Plotter as TATPPlotter


aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark for HYRISE')
aparser.add_argument('--scalefactor', default=1, type=int, metavar='SF',
                     help='Benchmark scale factor')
aparser.add_argument('--duration', default=20, type=int, metavar='D',
                     help='How long to run the benchmark in seconds')
aparser.add_argument('--clients', default=-1, type=int, metavar='N',
                     help='The number of blocking clients to fork (note: this overrides --clients-min/--clients-max')
aparser.add_argument('--clients-min', default=1, type=int, metavar='N',
                    help='The minimum number of blocking clients to fork')
aparser.add_argument('--clients-max', default=1, type=int, metavar='N',
                     help='The maximum number of blocking clients to fork')
aparser.add_argument('--no-load', action='store_true',
                     help='Disable loading the data')
aparser.add_argument('--no-execute', action='store_true',
                     help='Disable executing the workload')
aparser.add_argument('--host', default="localhost", type=str, metavar="H",
                     help='IP on which HYRISE should be run remotely')
aparser.add_argument('--remoteUser', default=getpass.getuser(), type=str, metavar="R",
                     help='remote User for remote host on which HYRISE should be run remotely')
aparser.add_argument('--remotePath', default="/home/" + getpass.getuser() +"/benchmark", type=str,
                     help='path of benchmark folder on remote host')
aparser.add_argument('--port', default=5001, type=int, metavar="P",
                     help='Port on which HYRISE should be run')
aparser.add_argument('--threads', default=0, type=int, metavar="T",
                     help='Number of server threads to use')
aparser.add_argument('--warmup', default=5, type=int,
                     help='Warmuptime before logging is activated')
aparser.add_argument('--manual', action='store_true',
                     help='Do not build and start a HYRISE instance (note: a HYRISE server must be running on the specified port)')
aparser.add_argument('--stdout', action='store_true',
                     help='Print HYRISE server\'s stdout to console')
aparser.add_argument('--stderr', action='store_true',
                     help='Print HYRISE server\'s stderr to console')
aparser.add_argument('--rebuild', action='store_true',
                     help='Force `make clean` before each build')
aparser.add_argument('--regenerate', action='store_true',
                     help='Force regeneration of TATP table files')
aparser.add_argument('--perfdata', default=False, action='store_true',
                     help='Collect additional performance data. Slows down benchmark.')
aparser.add_argument('--json', default=False, action='store_true',
                     help='Use JSON queries instead of stored procedures.')
aparser.add_argument('--uniform', default=False, action='store_true',
                     help='Use a uniform distribution for random subscriber ids during querying.')

args = vars(aparser.parse_args())

s1 = benchmark.Settings("None", PERSISTENCY="NONE")
#s2 = benchmark.Settings("Logger_1ms", PERSISTENCY="BUFFEREDLOGGER", WITH_GROUP_COMMIT=1, GROUP_COMMIT_WINDOW=1000)
#s3 = benchmark.Settings("Logger_10ms", PERSISTENCY="BUFFEREDLOGGER", WITH_GROUP_COMMIT=1, GROUP_COMMIT_WINDOW=10000)
#s4 = benchmark.Settings("Logger_50ms", PERSISTENCY="BUFFEREDLOGGER", WITH_GROUP_COMMIT=1, GROUP_COMMIT_WINDOW=50000)
#s5 = benchmark.Settings("Logger_unlimited", PERSISTENCY="BUFFEREDLOGGER", WITH_GROUP_COMMIT=1, GROUP_COMMIT_WINDOW="unlimited")
#s6 = benchmark.Settings("NVRAM", PERSISTENCY="NVRAM", NVRAM_FILENAME="hyrise_tpcc")

kwargs = {
    "remoteUser"        : args["remoteUser"],
    "remotePath"        : args["remotePath"],
    "remote"            : args["host"] is not "localhost",
    "host"              : args["host"],
    "port"              : args["port"],
    "manual"            : args["manual"],
    "warmuptime"        : args["warmup"],
    "runtime"           : args["duration"],
    "benchmarkQueries"  : [],
    "prepareQueries"    : [],
    "showStdout"        : args["stdout"],
    "showStderr"        : args["stderr"],
    "rebuild"           : args["rebuild"],
    "regenerate"        : args["regenerate"],
    "noLoad"            : args["no_load"],
    "serverThreads"     : args["threads"],
    "collectPerfData"   : args["perfdata"],
    "useJson"           : args["json"],
    "uniformSubIds"     : args["uniform"],
    "scalefactor"       : args["scalefactor"]
}

groupId = "tatp_tmp"
minClients = args["clients_min"]
maxClients = args["clients_max"]

if args["clients"] > 0:
    minClients = args["clients"]
    maxClients = args["clients"]

client_steps = set([minClients])
client_steps.update([i for i in xrange(10, maxClients+1, 10)])
if max(client_steps) < maxClients: client_steps.add(maxClients)

#for num_threads in xrange(minThreads, maxThreads+1):
for num_clients in xrange(minClients, maxClients+1):
#for num_clients in client_steps:
    #runId = "numClients_%s_numThreads_%s" % (num_clients, num_threads)
    runId = "numClients_%s" % (num_clients)
    kwargs["numUsers"] = num_clients

    #kwargs['serverThreads']=num_threads
    b1 = benchmark.TATPBenchmark(groupId, runId, s1, **kwargs)
    #b2 = benchmark.TPCCBenchmark(groupId, runId, s2, **kwargs0)
    #b3 = benchmark.TPCCBenchmark(groupId, runId, s3, **kwargs)
    #b4 = benchmark.TPCCBenchmark(groupId, runId, s4, **kwargs)
    #b5 = benchmark.TPCCBenchmark(groupId, runId, s5, **kwargs)
    #b6 = benchmark.TPCCBenchmark(groupId, runId, s6, **kwargs)

    b1.run()
    #b2.run()
    #b3.run()
    #b4.run()
    #b5.run()
    # b6.run()

    if os.path.exists("/mnt/pmfs/hyrise_tatp"):
        os.remove("/mnt/pmfs/hyrise_tatp")

plotter = TATPPlotter(groupId)
plotter.printStatistics()

