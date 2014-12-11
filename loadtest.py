import argparse
import benchmark
import os
import psutil
import signal
import subprocess
import sys
import threading
import time

from pylab import *

stop_get_cpu_load = False
avg_load = 0.0
actually_exit = False
login_user = os.getlogin()

def trykill():
    global login_user
    try:
        subprocess.call(["killall", "-u", login_user, "hyrise-server_release"])
    except:
        pass

def signal_handler(signal, frame):
    global stop_get_cpu_load
    global actually_exit
    stop_get_cpu_load = True
    actually_exit = True
    trykill()
    exit(-1)

def get_cpu_load(numa_node):
  global stop_get_cpu_load
  global avg_load
  load_sum = 0.0
  load_count = 0
  first = 20*numa_node
  last  = first+20

  while not stop_get_cpu_load:
    loads = psutil.cpu_percent(percpu=True)
    for l in range(first, last, 2):
        load_sum += loads[l]
    load_count += 10

  avg_load = load_sum/load_count

def start_hyrise():
    return subprocess.Popen("./build/hyrise-server_release --corecount=10 --nodes=1 --memorynodes=1 --threads=20 --port=5454", cwd="/home/Tim.Berning/hyrise_nvm/")


def load_tables(args):
    kwargs = {
        "host"              : "localhost",
        "port"              : args["port"],
        "remoteUser"        : "",
        "remotePath"        : "",
        "manual"            : True,
        "warehouses"        : 20,
        "noLoad"            : False,
        "abQueryFile"       : args["query_file"],
        "tabledir"          : args["table_dir"],
        "csv"               : False,
        "prepareQueries"    : None,
        "benchmarkQueries"  : None
    }
    s = benchmark.Settings("loader", {"PERSISTENCY":"NONE", "BLD":"release"})
    b = benchmark.TPCCBenchmark("loader", "load", s, **kwargs)
    b.benchPrepare()
    b.loadTables()

def parse_ab_log(args):
    f = open("results/loadtest/ab%i.log" % args["clients"])
    f.readline()
    collect = {}
    result = {}
    for line in f:
        items = line.replace("\r","").replace("\n","").split("\t")
        try:
            dtime, ttime, wait, tx, status = int(items[3]), int(items[4]), int(items[5]), items[6], int(items[7])
        except:
            continue
        if status is not 200:
            continue
        if collect.has_key(tx):
            collect[tx].append(ttime)
        else:
            collect[tx] = [ttime]
    f.close()
    for tx, values in collect.iteritems():
        result[tx] = {
            "min": min(values),
            "max": max(values),
            "mean": mean(values),
            "median": median(values),
            "95percentile": percentile(values, 95),
            "99percentile": percentile(values, 99)
        }
    return result

def warmup(args, seconds):
    ab = subprocess.Popen(["./ab/ab",
                           "-k",
                           "-t", str(seconds),
                           "-n", "1000000",
                           "-c", "10",
                           "-r",
                           "-u", "0",
                           "-s", "0",
                           "-m", args["query_file"],
                           "localhost:"+str(args["port"])+"/procedure/"],
                           stdout=open("/dev/null"),
                           stderr=open("/dev/null"))
    ab.wait()


def run_loadtest(args):
    global stop_get_cpu_load
    global avg_load

    # step 1: start Hyrise
    bindir = os.path.join(args["hyrise-path"], "build")
    environ = {
        "HYRISE_DB_PATH"    : args["hyrise-path"],
        "LD_LIBRARY_PATH"   : bindir+":/usr/local/lib64/"
    }
    server = os.path.join(bindir, "hyrise-server_release")
    hyrise = subprocess.Popen("%s --port=%i --threads=%i --corecount=10 --nodes=%i --memorynodes=%i" % (server, args["port"] , args["threads"], args["numa_node"], args["numa_node"]),
                              env=environ,
                              shell=True,
                              cwd=args["hyrise-path"],
                              stdout=open("/dev/null"),
                              stderr=open("/dev/null"))
    time.sleep(1)

    # step 2: load tables
    load_tables(args)

    # step 3: start the benchmark
    print "Warmup for 10 seconds"
    warmup(args, 10)
    print "Running actual benchmark now"
    if os.path.isfile("results/loadtest/ab%i.log" % args["clients"]):
        os.remove("results/loadtest/ab%i.log" % args["clients"])
    ab = subprocess.Popen(["./ab/ab",
                           "-g", "results/loadtest/ab%i.log" % args["clients"],
                           "-k",
                           "-t", str(args["runtime"]),
                           "-n", "1000000",
                           "-c", str(args["clients"]),
                           "-r",
                           "-u", str(args["think_time"]),
                           "-s", str(args["think_time_skew"]),
                           "-W", str(args["think_time_wl"]),
                           "-K", args["think_time_kind"],
                           "-m", args["query_file"],
                           "localhost:"+str(args["port"])+"/procedure/"],
                           stdout=open("/dev/null"),
                           stderr=open("/dev/null"))

    # step 4: measure CPU load
    stop_get_cpu_load = False
    avg_load = 0.0
    t1 = threading.Thread(target=get_cpu_load, kwargs={"numa_node": args["numa_node"]})
    t1.start()

    # step 5: wait for everything to finish
    ab.wait()
    stop_get_cpu_load = True
    t1.join()

    # step 6: stop hyrise
    print "Stopping Hyrise..."
    trykill()
    print "Done."

def print_results(args, avg_load, tx_results):

    print "=======\nResults [#clients: %i, runtime: %is]\n=======\n" % (args["clients"], args["runtime"])
    print "Average load: %1.2f%%" % avg_load
    print "Average latencies per TX:"
    for tx, latency in tx_results.iteritems():
        print "%16s --> %10.2fms (mean), %10.2fms (median), %10.2fms (95th percentile), %10.2fms (99th percentile)" % (tx, latency["mean"], latency["median"], latency["95percentile"], latency["99percentile"])


if __name__ == "__main__":
    aparser = argparse.ArgumentParser(description='HYRISE TPC-C Load Test')
    aparser.add_argument("hyrise-path", type=str, help="Path to Hyrise folder")
    aparser.add_argument("output-file", type=str, help="File to store results in")
    aparser.add_argument("--port", default=5000, type=int, help="Port on which HYRISE should be run")
    aparser.add_argument("--threads", default=20, type=int, help="Number of threads for Hyrise")
    aparser.add_argument("--clients", default=1, type=int, help="number of parallel clients")
    aparser.add_argument("--runtime", default=30, type=int, help="runtime in seconds")
    aparser.add_argument("--table-dir", default="/mnt/amida_02/loadtest/tables-10W/", type=str, help="location of table directory")
    aparser.add_argument("--query-file", default="/mnt/amida_02/loadtest/queries/10W_readonly_1M.txt", type=str, help="location of query file for Apache Benchmark")
    aparser.add_argument("--numa-node", default=0, type=int, help="NUMA node to be inspected for load")
    aparser.add_argument("--think-time", default=0, type=int, help="Client think time in usec")
    aparser.add_argument("--think-time-skew", default=1, type=int, help="Client think time skew in usec")
    aparser.add_argument("--think-time-wl", default=1, type=int, help="Client think time wavelength in usec")
    aparser.add_argument("--think-time-kind", default="constant", type=str, help="Kind/type of thinktime mechanism. values: sinus/normal/constant")
    args = vars(aparser.parse_args())

    try:
        signal.signal(signal.SIGINT, signal_handler)
    except:
        print "Could not attach signal handler"

    # change all the things!
    args["think_time"]    = 500000
    args["think_time_wl"] = 500000

    for kind in ["sinus", "normal"]:
        args["think_time_kind"] = kind
        for skew in [1, 100000, 200000, 300000, 400000, 500000]:
            args["think_time_skew"] = skew
            outputfile = "loadtest/loadtest_%s_skew%i.csv" % (kind, skew)
            f = open(outputfile, "w")
            f.write("num_clients;average_load;NewOrder_min;NewOrder_max;NewOrder_mean;NewOrder_median;NewOrder_95percentile;NewOrder_99percentile\n") #OrderStatus_min;OrderStatus_max;OrderStatus_mean;OrderStatus_median;OrderStatus_95percentile;OrderStatus_99percentile;StockLevel_min;StockLevel_max;StockLevel_mean;StockLevel_median;StockLevel_95percentile;StockLevel_99percentile\n")
            for n_clients in ([1]+range(100, 5001, 100)):

                # For a legit run using the legit cmd line parameters, only use this block (remove after #change all the things)
                # vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                args["clients"] = n_clients
                failed = True
                while failed:
                    failed = False
                    try:
                        run_loadtest(args)
                    except:
                        if actually_exit:
                            exit(-1)
                        print "Test failed, trying again"
                        trykill()
                        failed = True
                results = parse_ab_log(args)
                print_results(args, avg_load, results)
                print "\n\n"

                # write result to output file
                f.write("%i;%1.3f" % (n_clients, avg_load))
                for tx in ["TPCC-OrderStatus", "TPCC-StockLevel"]:
                    for param in ["min", "max", "mean", "median", "95percentile", "99percentile"]:
                        f.write(";%1.3f" % results[tx][param])
                f.write("\n")
                time.sleep(2)
                # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            f.close()
