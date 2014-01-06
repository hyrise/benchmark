
import httplib
import logging
import os
import requests
import shutil
import signal
import subprocess
import sys

# include py-tpcc files
sys.path.insert(0, os.path.join(os.getcwd(), "benchmark", "bench-tpcc"))
from util import *
from runtime import *
import constants
import drivers
from tpcc import *

from benchmark import Benchmark
from user import User


# disable py-tpcc internal logging
logging.getLogger("requests").setLevel(logging.WARNING)

class TPCCUser(User):

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        User.__init__(self, userId, host, port, dirOutput, queryDict, **kwargs)

        self.scaleParameters = kwargs["scaleParameters"]
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
        self.context = None
        try:
            self.driver.executeTransaction(txn, params, use_stored_procedure= not self._useJson)
        except requests.ConnectionError:
            self.numErrors += 1
            if self.numErrors > 5:
                print "*** TPCCUser %i: too many failed requests" % (self._userId)
                self.stopLogging()
                os.kill(os.getppid(), signal.SIGINT)
            return
        except RuntimeWarning, e:
            # these are transaction errors, e.g. abort due to concurrent commits
            tEnd = time.time()
            self.log("failed", [txn, tEnd-tStart, tStart-self.userStartTime])
            return
        except RuntimeError, e:
            print "%s: %s" % (txn, e)
            tEnd = time.time()
            self.log("failed", [txn, tEnd-tStart, tStart-self.userStartTime])
            return
        except AssertionError, e:
            return
        self.numErrors = 0
        tEnd = time.time()
        self.log("transactions", [txn, tEnd-tStart, tStart-self.userStartTime, self.perf])

    def stopUser(self):
        """ executed once after stop request was sent to user """
        pass

    def formatLog(self, key, value):
        if key == "transactions":
            logStr = "%s;%f;%f" % (value[0], value[1], value[2])
            for op, opData in value[3].iteritems():
                logStr += ";%s,%i,%f" % (op, opData["n"], opData["t"])
            logStr += "\n"
            return logStr
        elif key == "failed":
            return "%s;%f;%f\n" % (value[0], value[1], value[2])
        else:
            return "%s\n" % str(value)

    def addPerfData(self, perf):
        if perf:
            for op in perf:
                self.perf.setdefault(op["name"], {"n": 0, "t": 0.0})
                self.perf[op["name"]]["n"] += 1
                self.perf[op["name"]]["t"] += op["endTime"] - op["startTime"]

    # HyriseConnection stubs
    # ======================
    def stored_procedure(self, stored_procedure, querystr, paramlist=None, commit=False):
        if paramlist:
            for k,v in paramlist.iteritems():
                if v == True:    v = 1;
                elif v == False: v = 0;

        result = self.fireQuery(querystr, paramlist, sessionContext=self.context, autocommit=commit, stored_procedure=stored_procedure).json()
        self.addPerfData(result.get("performanceData", None))
        return result


    def query(self, querystr, paramlist=None, commit=False):
        if paramlist:
            for k,v in paramlist.iteritems():
                if v == True:    v = 1;
                elif v == False: v = 0;

        result = self.fireQuery(querystr, paramlist, sessionContext=self.context, autocommit=commit).json()

        self.lastResult = result.get("rows", None)
        self.lastHeader = result.get("header", None)

        # check session context to make sure we are in the correct transaction
        new_session_context = result.get("session_context", None)
        if self.context != new_session_context:
            if self.context != None and new_session_context != None:
                raise RuntimeError("Session context was ignored by database")

        self.context = new_session_context
        self.addPerfData(result.get("performanceData", None))


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


class TPCCBenchmark(Benchmark):

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        Benchmark.__init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs)

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
        config["query_location"] = os.path.join("queries", "tpcc-queries")
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

