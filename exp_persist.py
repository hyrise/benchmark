import argparse
import os
import requests
import sys
import time

import matplotlib as mpl
mpl.use('Agg')
from pylab import *

LOAD_JSON = """
{"operators": {"l": {"type": "TableLoad", "table": "PERSISTBENCHTABLE", "filename" : "%s"}}, "edges": [["l","l"]]}
"""

INSERT_JSON = """
{"operators": {"g": {"type": "GetTable", "name": "PERSISTBENCHTABLE"}, "i": {"type": "InsertScan", "data": [%s]}}, "edges": [["g","i"]]}
"""

PERSIST_JSON = """
{"operators": {"l": {"type": "PersistTable", "table_name": "PERSISTBENCHTABLE"}}, "edges": [["l","l"]]}
"""

RECOVER_JSON = """
{"operators": {"l": {"type": "RecoverTable", "table_name": "PERSISTBENCHTABLE", "replayLog": true}}, "edges": [["l","l"]]}
"""

UNLOADALL_JSON = """
{"operators": {"l": {"type": "UnloadAll"}}, "edges": [["l","l"]]}
"""


class PersistBenchmark:

    def __init__(self, hyrisepath, filename):
        self._hyrisePath      = hyrisepath
        self._tableFileName   = filename
        self._host            = "localhost"
        self._port            = 5454
        self._session         = requests.Session()
        self._session.headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        self._dirResults      = "results/persist/"
        self._dirOutput       = "plots/persist/"

        if not os.path.isdir(self._dirResults):
            os.makedirs(self._dirResults)
        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)

    def _generateTableFile(self, size):
        tf = open(os.path.join(self._hyrisePath, self._tableFileName), "w")
        tf.write("col_0\nINTEGER\n0_C\n===\n")
        for i in range(0, size):
            tf.write("%d\n" % i)
        tf.close()

    def _fireQuery(self, queryString, autocommit=False):
        data = {"query": queryString}
        if autocommit: data["autocommit"] = "true"
        self._session.post("http://%s:%s/" % (self._host, self._port), data=data)

    def _out(self, msg):
        sys.stdout.write(msg)
        sys.stdout.flush()

    def run(self, mainSizes, deltaSizes):
        results = []
        for szd in deltaSizes:
            for sz in mainSizes:
                print "=========="
                self._out("Creating table file with %d rows... " % sz)
                self._generateTableFile(sz)
                print "done."
                rowsToInsert = 0
                nextValue = sz+1
                self._out("Loading table into Hyrise... ")
                self._fireQuery(LOAD_JSON % self._tableFileName)
                self._out("done.\nCreating Delta with %d rows... " % int(szd * sz))
                rowsToInsert = int(szd * sz)
                if rowsToInsert > 0:
                    insertStr = ""
                    for i in range(rowsToInsert):
                        insertStr += "[%d]," % nextValue
                        nextValue += 1
                    insertStr = insertStr[:-1]
                    self._fireQuery(INSERT_JSON % insertStr, autocommit=True)
                self._out("done.\nPersisting table... ")
                self._fireQuery(PERSIST_JSON)
                self._fireQuery(UNLOADALL_JSON)
                self._out("done.\nRecovering table... ")
                tStart = time.time()
                self._fireQuery(RECOVER_JSON)
                tRecover = time.time() - tStart
                print "done. took %1.3f s\n" % tRecover
                results.append({"rows": sz, "delta": szd, "tRecover": tRecover})
                self._fireQuery(UNLOADALL_JSON)
        f = open(os.path.join(self._dirResults, "times.csv"), "w")
        for result in results:
            f.write("%d,%f,%f\n" % (result["rows"], result["delta"], result["tRecover"]))

    def plot(self):
        plt.title("Recovery Times (One Integer Column, Varying Rows)")
        plt.ylabel("Time in Seconds")
        plt.xlabel("Number of Rows")
        data = {}
        for rawline in open(os.path.join(self._dirResults, "times.csv"), "r"):
            lineitems = rawline.split(",")
            dsz = float(lineitems[1])
            if data.has_key(dsz):
                data[dsz]["rows"].append(int(lineitems[0]))
                data[dsz]["time"].append(float(lineitems[2]))
            else:
                data[dsz] = {"rows": [int(lineitems[0])], "time": [float(lineitems[2])]}
        deltaSizes = data.keys()
        deltaSizes.sort()
        for dsz in deltaSizes:
            plt.plot(data[dsz]["rows"], data[dsz]["time"], label="Delta %d%% of Main" % (dsz*100))
        plt.xticks(data[deltaSizes[0]]["rows"], rotation=45, fontsize=5)
        #plt.xscale("log")
        #plt.yscale("log")
        ax = plt.gca()
        ax.get_xaxis().get_major_formatter().set_scientific(False)
        plt.legend(loc='upper left', prop={'size':10})
        fname = os.path.join(self._dirOutput, "recover.pdf")
        plt.savefig(fname)
        plt.close()

if __name__ == "__main__":
    mainSizes = [500000, 1000000, 5000000, 10000000]
    deltaSizes = [0.0, 0.05, 0.2]
    b = PersistBenchmark("/home/Tim.Berning/merge/hyrise/", "test/tpcc/persist/table.tbl")
    b.run(mainSizes, deltaSizes)
    b.plot()
