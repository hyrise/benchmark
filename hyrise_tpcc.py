import argparse
import benchmark
import os
import shutil
import sys

# include py-tpcc files
sys.path.insert(0, os.path.join(os.getcwd(), "pytpcc"))
from util import *
from runtime import *
import drivers
from tpcc import *

class TPCCUser(benchmark.User):

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        benchmark.User.__init__(self, userId, host, port, dirOutput, queryDict, kwargs)
        self.scaleParameters = kwargs["scaleParameters"]
        self.config = kwargs["config"]
        self.config["reset"] = False
        self.config["execute"] = True
        self.config["querylog"] = None
        self.config["print_load"] = False
        self.config["port"] = self._port
        self.driver = HyriseDriver()
        self.driver.loadConfig(self.config)
        self.driver.conn = self
        self.context = None
        self.lastResult = None
        self.lastHeader = None

    def prepareUser(self):
        """ executed once when user starts """
        self.e = executor.Executor(self.driver, self.scaleParameters)
        self.r = results.Results()

    def runUser(self):
        """ main user activity """
        txn, params = self.e.doOne()
        #txn_id = self.r.startTransaction(txn)
        self.driver.executeTransaction(txn, params)

    def stopUser(self):
        """ executed once after stop request was sent to user """
        pass

    # HyriseConnection stubs
    # ======================
    def query(self, querystr, paramlist=None, commit=False):
        print "query"
        for k,v in paramlist.iteritems():
            if v == True:    v = 1;
            elif v == False: v = 0;
        result = self.fireQuery(querystr, paramlist, sessionContext=self.context, autocommit=commit).json()
        self.context = result.get("session_context", None)
        self.lastResult = result.get("rows", None)
        self.lastHeader = result.get("header", None)

    def commit(self):
        print "commit"
        if not self.context:
            raise Exception("Should not commit without running context")
        result = self.fireQuery("""{"operators": {"cm": {"type": "Commit"}}}""", sessionContext=self.context)
        self.context = None
        return result

    def rollback(self):
        print "rollback"
        if not self.context:
            raise Exception("Should not commit without running context")
        result = self.fireQuery("""{"operators": {"rb": {"type": "Rollback"}}}""", sessionContext=self.context)
        self.context = None
        return result

    def runningTransactions(self):
        print "runningTransactions"
        return self._session.get("http://%s:%s/status/tx" % (self._host, self._port)).json()

    def fetchone(self, column=None):
        print "fetchone"
        if self.result:
            r = self.lastResult.pop()
            return r[self.lastHeader.index(column)] if column else r
        return None

    def fetchone_as_dict(self):
        print "fetchone_as_dict"
        if self.lastResult:
            return dict(zip(self.lastHeader, self.lastResult.pop()))
        return None

    def fetchall(self):
        print "fetchall"
        tmp = self.lastResult
        self.lastResult = None
        return tmp

    def fetchall_as_dict(self):
        print "fetchall_as_dict"
        if self.lastResult:
            r = [dict(zip(self.lastHeader, cur_res)) for cur_res in self.lastResult]
            self.lastResult = None
            return r
        return None


class TPCCBenchmark(benchmark.Benchmark):

    def __init__(self, benchmarkGroupId, buildSettings, **kwargs):
        os.environ['HYRISE_DB_PATH'] = os.path.join(os.getcwd(), "builds/%s" % buildSettings.getName())
        kwargs["userClass"] = TPCCUser
        benchmark.Benchmark.__init__(self, benchmarkGroupId, buildSettings, **kwargs)

        self.scalefactor     = kwargs["scalefactor"] if kwargs.has_key("scalefactor") else 1
        self.warehouses      = kwargs["warehouses"] if kwargs.has_key("warehouses") else 4
        self.driverClass     = createDriverClass("hyrise")
        self.driver          = self.driverClass(os.path.join(os.getcwd(), "pytpcc/tpcc.sql"))
        self.scaleParameters = scaleparameters.makeWithScaleFactor(self.warehouses, self.scalefactor)

    def benchPrepare(self):
        """ executed once after benchmark was started and HYRISE server is running """
        rand.setNURand(nurand.makeForLoad())

        dirTPCC = os.path.join(self._dirBinary, "tpcc")
        if not os.path.isdir(dirTPCC):
            os.makedirs(dirTPCC)
        dirTPCCTables = os.path.join(dirTPCC, "tables")
        dirTPCCQUeries = os.path.join(dirTPCC, "queries")
        if not os.path.islink(dirTPCCQUeries):
            os.symlink(os.path.join(os.getcwd(), "pytpcc", "queries"), dirTPCCQUeries)
        if not os.path.isdir(dirTPCCTables):
            os.makedirs(dirTPCCTables)

        defaultConfig = self.driver.makeDefaultConfig()
        config = dict(map(lambda x: (x, defaultConfig[x][1]), defaultConfig.keys()))
        config["querylog"] = None
        config["print_load"] = False
        config["reset"] = False
        config["port"] = self._port
        self.driver.loadConfig(config)

        l = loader.Loader(self.driver, self.scaleParameters, range(self.scaleParameters.starting_warehouse, self.scaleParameters.ending_warehouse+1), True)
        #l.execute()

        self._userArgs = {
            "scaleParameters": self.scaleParameters,
            "config": config
        }

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark for HYRISE')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                         help='Benchmark scale factor')
    aparser.add_argument('--warehouses', default=1, type=int, metavar='W',
                         help='Number of Warehouses')
    aparser.add_argument('--duration', default=60, type=int, metavar='D',
                         help='How long to run the benchmark in seconds')
    aparser.add_argument('--clients', default=1, type=int, metavar='N',
                         help='The number of blocking clients to fork')
    aparser.add_argument('--no-load', action='store_true',
                         help='Disable loading the data')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the workload')
    args = vars(aparser.parse_args())

    s1 = benchmark.Settings("none", PERSISTENCY="NONE")

    b1 = TPCCBenchmark("testrun", s1, port=5001, warmuptime=10, runtime=args["duration"], numUsers=args["clients"], warehouses=args["warehouses"], benchmarkQueries=["q13insert"], prepareQueries=[])

    b1.run()
