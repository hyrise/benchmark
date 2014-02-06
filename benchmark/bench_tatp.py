import sys
import os
import logging
import time
import requests
# include tatp files
from py_tatp.hyrisedriver import HyriseDriver, TATPFailedAccordingToSpec, TATPAcceptableError
from py_tatp import constants
#sys.path.insert(0, os.path.join(os.getcwd(), "benchmark", "bench-tatp"))
#from util import *
#from runtime import *
#import constants
#import drivers
from py_tatp.generator import Generator

#from tatp import *


from benchmark import Benchmark
from user import User


logging.getLogger("requests").setLevel(logging.WARNING)


class TATPUser(User):

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        User.__init__(self, userId, host, port, dirOutput, queryDict, **kwargs)
        self.perf = {}
        self.numErrors = 0
        self.driver = HyriseDriver( kwargs['scalefactor'],
                                    kwargs['hyrise_builddir'],
                                    kwargs['hyrise_tabledir'],
                                    kwargs['querydir'],
                                    kwargs['uniform_sids'])

    def prepareUser(self):
        """ executed once when user starts """
        self.driver.conn = self
        self.context = None
        self.lastResult = None
        self.lastHeader = None
        self.userStartTime = time.time()

    def runUser(self):
        """ main user activity """
        self.perf = {}
        txn, params = self.driver.doOne()
        tStart = time.time()
        self.context = None
        try:
            self.driver.executeTransaction(txn, params, use_stored_procedure= not self._useJson)
        except requests.ConnectionError:
            self.numErrors += 1
            if self.numErrors > 5:
                print "*** TATP %i: too many failed requests" % (self._userId)
                self.stopLogging()
                os.kill(os.getppid(), signal.SIGINT)
            return
        except TATPFailedAccordingToSpec, e:
            # these are rollbacks triggered from the client because the results didn't match the expectation
            tEnd = time.time()
            self.log("failed", [txn, tEnd-tStart, tStart-self.userStartTime])
            return
        except TATPAcceptableError, e:
            # these are rollbacks triggered from the client because the results didn't match the expectation
            tEnd = time.time()
            self.log("failed_acceptably", [txn, tEnd-tStart, tStart-self.userStartTime])
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
        #print result

        self.lastResult = result.get("rows", None)
        self.lastHeader = result.get("header", None)
        self.affectedRows = result.get("affectedRows", None)

        #print self.lastResult

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


class TATPBenchmark(Benchmark):

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        Benchmark.__init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs)

        self._dirHyriseDB = os.path.join(os.getcwd(), "hyrise")
        os.environ['HYRISE_DB_PATH'] = self._dirHyriseDB

        self.scalefactor     = kwargs["scalefactor"] if kwargs.has_key("scalefactor") else 1
        self.regenerate      = kwargs["regenerate"] if kwargs.has_key("regenerate") else False
        self.noLoad          = kwargs["noLoad"] if kwargs.has_key("noLoad") else False
        self.uniform = kwargs["uniformSubIds"]
        self.setUserClass(TATPUser)

    def benchPrepare(self):
        # make sure the TPC-C query and table directories are present
        dirTables   = os.path.join(self._dirHyriseDB, "test", "tatp", "tables")
        dirQueries = os.path.join("queries", "tatp-queries")
        #dirQueries = os.path.join("queries", "tatp-queries_no_index")

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
            for t in constants.ALL_TABLES:
                if not os.path.isfile(os.path.join(dirTables, "%s.tbl" % t)): #or not os.path.isfile(os.path.join(dirTables, "%s.hdr" % t)):
                    print "table files incomplete"
                    generate = True
                    break

        if generate:
            sys.stdout.write("regenerating... ")
            sys.stdout.flush()
            g = Generator(dirTables, self.scalefactor)
            g.deleteExistingTablefiles()
            g.execute()
        print "done"

        if self.noLoad:
            print "Skipping table load"
        else:
            sys.stdout.write("Importing tables into HYRISE... ")
            sys.stdout.flush()
            tableload_filename = os.path.abspath(os.path.join(dirQueries, constants.LOAD_FILE))
            with open(tableload_filename, 'r') as loadfile:
                self.fireQuery(loadfile.read(), autocommit=True)
            #test = self.fireQuery("""{
            """  "operators": {
                "load": {
                   "type": "TableLoad",
                   "table": "SUBSCRIBER"
                    },
                    "validate" : {
                      "type": "ValidatePositions"
                    },
                    "project": {
                       "type": "ProjectionScan",
                       "fields": ["S_ID", "SUB_NBR"]
                    }
                  },
                  "edges": [["load", "validate"], ["validate","project"]]
                }
                """
                #""", autocommit=True)
                #            import pdb; pdb.set_trace()
            print "done"

        self.setUserArgs({
            "scalefactor": self.scalefactor,
            "hyrise_builddir": self._dirHyriseDB,
            "hyrise_tabledir": dirTables,
            "querydir": dirQueries,
            "uniform_sids": self.uniform
        })

