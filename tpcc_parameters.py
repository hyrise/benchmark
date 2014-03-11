import argparse
import benchmark
import os
import getpass
import shutil
import commands

def clear_dir(path):
    print "Clearing directory:", path
    if not os.path.exists(path):
        return
    for root, dirs, files in os.walk(path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

def reset():
    if not args["manual"]:
        if "hyriseDBPath" in kwargs:
            clear_dir(kwargs["hyriseDBPath"])
        pmfs_data = "/mnt/pmfs/hyrisedata/"
        clear_dir(pmfs_data)
        
aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark for HYRISE')
aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                     help='Benchmark scale factor')
aparser.add_argument('--warehouses', default=1, type=int, metavar='W',
                     help='Number of Warehouses')
aparser.add_argument('--duration', default=20, type=int, metavar='D',
                     help='How long to run the benchmark in seconds')
aparser.add_argument('--clients', default=-1, type=int, metavar='N',
                     help='The number of blocking clients to fork (note: this overrides --clients-min/--clients-max')
aparser.add_argument('--clients-min', default=1, type=int, metavar='N',
                     help='The minimum number of blocking clients to fork')
aparser.add_argument('--clients-max', default=1, type=int, metavar='N',
                     help='The maximum number of blocking clients to fork')
aparser.add_argument('--clients-step', default=1, type=int, metavar='N',
                     help='The step-width for the number of clients to fork')
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
                     help='Number of server threadsto use')
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
                     help='Force regeneration of TPC-C table files')
aparser.add_argument('--perfdata', default=False, action='store_true',
                     help='Collect additional performance data. Slows down benchmark.')
aparser.add_argument('--json', default=False, action='store_true',
                     help='Use JSON queries instead of stored procedures.')
aparser.add_argument('--ab', default=None,
                     help='Queryfile with prepared requests. If specified ab tool is used to fire queries.')
aparser.add_argument('--verbose', default=1,
                     help='Verbose output level. Default is 1. Set to 0 if nothing should be printed.')
aparser.add_argument('--abCore', default=2,
                     help='Core to bind ab to.')
aparser.add_argument('--tabledir', default=None, type=str, metavar="T",
                     help='Directory for TPCC tables to use.')
aparser.add_argument('--genCount', default=None, type=str,
                     help='Number of queries to generate')
aparser.add_argument('--genFile', default=None, type=str, metavar="T",
                     help='File to store generated queries')
aparser.add_argument('--onlyNeworders', default=False, action='store_true',
                     help='Only do new-order transactions. Otherwise full mix of tpcc transactions is executed/generated.')
aparser.add_argument('--csv', default=False, action='store_true',
                     help='Load data from csv files and do not user binary import.')

args = vars(aparser.parse_args())

if args["tabledir"] == None:
    print "Please specify a table directory."
    exit(0)
else:
    args["tabledir"] = os.path.abspath(args["tabledir"])
    print "Using table directory:", args["tabledir"]

def create_benchmark(name, settings_kwargs, groupId, parameters, benchmark_kwargs):
    reset()
    runId = str(parameters).replace(",", "@")
    s = benchmark.Settings(name, **settings_kwargs)
    return benchmark.TPCCBenchmark(groupId, runId, s, **benchmark_kwargs) 

def create_benchmark_none(name, groupId, parameters, benchmark_kwargs):
    settings_kwargs = {"PERSISTENCY":"NONE"}
    return create_benchmark(name, settings_kwargs, groupId, parameters, benchmark_kwargs)

def create_benchmark_logger(name, groupId, parameters, benchmark_kwargs, windowsize_ms):
    commands.getoutput("touch hyrise/src/lib/io/GroupCommitter.h")
    commands.getoutput("touch hyrise/src/lib/helper/Settings.cpp")  
    settings_kwargs = {"PERSISTENCY":"BUFFEREDLOGGER", "WITH_GROUP_COMMIT":1, "GROUP_COMMIT_WINDOW":windowsize_ms*1000}
    return create_benchmark(name, settings_kwargs, groupId, parameters, benchmark_kwargs)

def create_benchmark_nvram(name, groupId, parameters, benchmark_kwargs):
    settings_kwargs = {"PERSISTENCY":"NVRAM", "NVRAM_FILENAME":"hyrise_tpcc"}
    return create_benchmark(name, settings_kwargs, groupId, parameters, benchmark_kwargs)

kwargs = {
    "remoteUser"        : args["remoteUser"],
    "remotePath"        : args["remotePath"],
    "remote"            : args["host"] is not "localhost",
    "host"              : args["host"],
    "port"              : args["port"],
    "manual"            : args["manual"],
    "warmuptime"        : args["warmup"],
    "runtime"           : args["duration"],
    "warehouses"        : args["warehouses"],
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
    "abQueryFile"       : args["ab"],
    "abCore"            : args["abCore"],
    "verbose"           : args["verbose"],
    "tabledir"          : args["tabledir"],
    "onlyNeworders"     : args["onlyNeworders"],
    "csv"               : args["csv"]
}
