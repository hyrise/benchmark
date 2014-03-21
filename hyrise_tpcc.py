import argparse
import benchmark
import httplib
import logging
import os
import requests
import shutil
import subprocess
import sys

# include py-tpcc files
sys.path.insert(0, os.path.join(os.getcwd(), "pytpcc", "pytpcc"))
from util import *
from runtime import *
import constants
import drivers
from tpcc import *

# disable py-tpcc internal logging
logging.getLogger("requests").setLevel(logging.WARNING)

class TPCCUser(benchmark.User):

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        benchmark.User.__init__(self, userId, host, port, dirOutput, queryDict, **kwargs)

        self.scaleParameters = kwargs["scaleParameters"]
        self.useStoredProcedures = kwargs["useStoredProcedures"] if kwargs.has_key("useStoredProcedures") else False
        self.config = kwargs["config"]
        self.config["reset"] = False
        self.config["execute"] = True
        self.perf = {}
        self.numErrors = 0

    def prepareUser(self):
        """ executed once when user starts """
        self.driver = drivers.hyrisedriver.HyriseDriver("")
        self.driver.loadConfig(self.config)
        self.driver.conn = self
        self.context = None
        self.lastResult = None
        self.lastHeader = None
        self.e = executor.Executor(self.driver, self.scaleParameters)
        self.userStartTime = time.time()

    def runUser(self):
        """ main user activity """
        self.perf = {}
        txn, params = self.e.doOne()
        tStart = time.time()
        try:
            self.driver.executeTransaction(txn, params, self.useStoredProcedures)
        except requests.ConnectionError:
            self.context = None
            self.numErrors += 1
            if self.numErrors > 5:
                print "*** TPCCUser %i: too many failed requests" % (self._userId)
                self.stopLogging()
                self.stop()
            return
        except (RuntimeError, AssertionError), e:
            print e
            self.context = None
            return
        self.numErrors = 0
        tEnd = time.time()
        self.log("transactions", [txn, tEnd-tStart, tStart-self.userStartTime, self.perf])

    def stopUser(self):
        """ executed once after stop request was sent to user """
        pass

    def formatLog(self, key, value):
        logStr = "%s;%f;%f" % (value[0], value[1], value[2])
        for op, opData in value[3].iteritems():
            logStr += ";%s,%i,%f" % (op, opData["n"], opData["t"])
        logStr += "\n"
        return logStr


    # HyriseConnection stubs
    # ======================
    def query(self, querystr, paramlist=None, commit=False, stored_procedure=None):
        if paramlist:
            for k,v in paramlist.iteritems():
                if v == True:    v = 1;
                elif v == False: v = 0;

        result = self.fireQuery(querystr, paramlist, sessionContext=self.context, autocommit=commit, stored_procedure=stored_procedure).json()

        self.lastResult = result.get("rows", None)
        self.lastHeader = result.get("header", None)

        if stored_procedure:
            return

        # check session context to make sure we are in the correct transaction
        new_session_context = result.get("session_context", None)
        if self.context != new_session_context:
            if self.context != None and new_session_context != None:
                raise RuntimeError("Session context was ignored by database")

        self.context = new_session_context

        perf = result.get("performanceData", None)
        if perf:
            for op in perf:
                self.perf.setdefault(op["name"], {"n": 0, "t": 0.0})
                self.perf[op["name"]]["n"] += 1
                self.perf[op["name"]]["t"] += op["endTime"] - op["startTime"]


    def commit(self):
        if not self.context:
            raise RuntimeError("Should not commit without running context")
        self.query("""{"operators": {"cm": {"type": "Commit"}}}""", commit=False)
        self.context = None

    def rollback(self):
        if not self.context:
            raise RuntimeError("Should not rollback without running context")
        result = self.fireQuery("""{"operators": {"rb": {"type": "Rollback"}}}""", sessionContext=self.context)
        self.context = None
        return result

    def runningTransactions(self):
        return self._session.get("http://%s:%s/status/tx" % (self._host, self._port)).json()

    def fetchone(self, column=None):
        if self.lastResult:
            r = self.lastResult.pop(0)
            return r[self.lastHeader.index(column)] if column else r
        return None

    def fetchone_as_dict(self):
        if self.lastResult:
            return dict(zip(self.lastHeader, self.lastResult.pop(0)))
        return None

    def fetchall(self):
        tmp = self.lastResult
        self.lastResult = None
        return tmp

    def fetchall_as_dict(self):
        if self.lastResult:
            r = [dict(zip(self.lastHeader, cur_res)) for cur_res in self.lastResult]
            self.lastResult = None
            return r
        return None


class TPCCBenchmark(benchmark.Benchmark):

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        benchmark.Benchmark.__init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs)

        self._dirHyriseDB = os.path.join(os.getcwd(), "hyrise")
        os.environ['HYRISE_DB_PATH'] = self._dirHyriseDB

        self.scalefactor     = kwargs["scalefactor"] if kwargs.has_key("scalefactor") else 1
        self.warehouses      = kwargs["warehouses"] if kwargs.has_key("warehouses") else 4
        self.driverClass     = createDriverClass("hyrise")
        self.driver          = self.driverClass(os.path.join(os.getcwd(), "pytpcc", "tpcc.sql"))
        self.scaleParameters = scaleparameters.makeWithScaleFactor(self.warehouses, self.scalefactor)
        self.regenerate      = False
        self.noLoad          = kwargs["noLoad"] if kwargs.has_key("noLoad") else False
        self.setUserClass(TPCCUser)

    def benchPrepare(self):
        # make sure the TPC-C query and table directories are present
        dirPyTPCC   = os.path.join(os.getcwd(), "pytpcc", "pytpcc")
        dirTables   = os.path.join(self._dirHyriseDB, "test", "tpcc", "tables")

        sys.stdout.write("Checking for table files... ")
        sys.stdout.flush()
        generate = False
        if not os.path.isdir(dirTables):
            print "no table files found"
            generate = True
            os.makedirs(dirTables)
        elif self.regenerate:
            print "table file regeneration requested"
            generate = True
        else:
            for t in self.driver.tables:
                if not os.path.isfile(os.path.join(dirTables, "%s.tbl" % t)): #or not os.path.isfile(os.path.join(dirTables, "%s.hdr" % t)):
                    print "table files incomplete"
                    generate = True
                    break

        rand.setNURand(nurand.makeForLoad())
        defaultConfig = self.driver.makeDefaultConfig()
        config = dict(map(lambda x: (x, defaultConfig[x][1]), defaultConfig.keys()))
        config["querylog"] = None
        config["print_load"] = False
        config["port"] = self._port
        config["hyrise_builddir"] = self._dirHyriseDB
        config["table_location"] = dirTables
        config["query_location"] = os.path.join(os.getcwd(), "queries", "tpcc-queries")
        self.driver.loadConfig(config)

        if generate:
            sys.stdout.write("regenerating... ")
            sys.stdout.flush()
            self.driver.deleteExistingTablefiles(dirTables)
            self.driver.createFilesWithHeader(dirTables)
            generator = loader.Loader(self.driver, self.scaleParameters, range(1,self.warehouses+1), True)
            generator.execute()
        print "done"

        if self.noLoad:
            print "Skipping table load"
        else:
            sys.stdout.write("Importing tables into HYRISE... ")
            sys.stdout.flush()
            self.driver.executeStart()
            print "done"

        self.setUserArgs({
            "scaleParameters": self.scaleParameters,
            "config": config
        })

if __name__ == "__main__":
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
    aparser.add_argument('--no-load', action='store_true',
                         help='Disable loading the data')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the workload')
    aparser.add_argument('--port', default=5001, type=int, metavar="P",
                         help='Port on which HYRISE should be run')
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
    aparser.add_argument('--stored-procedures', action='store_true',
                         help='Use TPCC stored procedures instead of regular queries')
    args = vars(aparser.parse_args())

    s1 = benchmark.Settings("NoLogger", PERSISTENCY="NONE")
    s2 = benchmark.Settings("BufferedLogger", PERSISTENCY="BUFFEREDLOGGER")
    s3 = benchmark.Settings("NVRAM", PERSISTENCY="NVRAM", NVRAM_FILENAME="hyrise_tpcc")

    kwargs = {
        "port"                : args["port"],
        "manual"              : args["manual"],
        "warmuptime"          : args["warmup"],
        "runtime"             : args["duration"],
        "warehouses"          : args["warehouses"],
        "benchmarkQueries"    : [],
        "prepareQueries"      : [],
        "showStdout"          : args["stdout"],
        "showStderr"          : args["stderr"],
        "rebuild"             : args["rebuild"],
        "regenerate"          : args["regenerate"],
        "noLoad"              : args["no_load"],
        "useStoredProcedures" : args["stored_procedures"],
        "collectPerfData"     : True
    }

    groupId = "tpcc"
    num_clients = args["clients"]
    minClients = args["clients_min"]
    maxClients = args["clients_max"]
    if args["clients"] > 0:
        minClients = args["clients"]
        maxClients = args["clients"]

    for num_clients in xrange(minClients, maxClients+1):
        runId = "numClients_%s" % num_clients
        kwargs["numUsers"] = num_clients

        b1 = TPCCBenchmark(groupId, runId, s1, **kwargs)
        b2 = TPCCBenchmark(groupId, runId, s2, **kwargs)
        #b3 = TPCCBenchmark(groupId, runId, s3, **kwargs)

        b1.run()
        b2.run()
        # b3.run()

        if os.path.exists(os.path.expandvars("/mnt/pmfs/$USER/hyrise_tpcc")):
            os.remove(os.path.expandvars("/mnt/pmfs/$USER/hyrise_tpcc"))

    #plotter = benchmark.Plotter(groupId)
    #plotter.printStatistics()
