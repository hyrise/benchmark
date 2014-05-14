import argparse
import benchmark
import os
import shutil
import signal
import subprocess
import sys
import time

aparser = argparse.ArgumentParser(description='HYRISE Recovery Benchmark for TPC-C STOCK Table with varying sizes')
aparser.add_argument('--binary', action='store_true',
                     help='Restore Main partitions from existing binary dumps')
aparser.add_argument('--skipGenerate', action='store_true',
                     help='Skip generation of table dump files')
aparser.add_argument('--manual', action='store_true',
                     help='execute generation on a manual Hyrise instance')
args = vars(aparser.parse_args())

def clear_dir(path):
    print "Clearing directory:", path
    if not os.path.exists(path):
        return
    for root, dirs, files in os.walk(path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

def clear_file(filename):
    if os.path.isfile(filename):
        os.remove(filename)
        print "Deleted file:", filename

def reset_nvram_directory():
    if not args["manual"]:
        pmfs_data = os.path.expandvars("/mnt/pmfs/$USER/hyrisedata/")
        clear_dir(pmfs_data)
        hyrise_tpcc = os.path.expandvars("/mnt/pmfs/$USER/hyrise_tpcc")
        txmgr = os.path.expandvars("/mnt/pmfs/$USER/txmgr.bin")
        hyrise = os.path.expandvars("/mnt/pmfs/$USER/hyrise")
        clear_file(hyrise)
        clear_file(hyrise_tpcc)
        clear_file(txmgr)

Q_LOAD_STOCK = """ {
    "operators": {
        "loadStock": {
            "type": "TableLoad",
            "table": "STOCK",
            "filename": "%(filename)s",
            "path": "%(path)s"
            }
        },
        "createMainIdx": {
             "type": "CreateGroupkeyIndex",
             "index_name": "mcidx__STOCK__main__S_W_ID__S_I_ID",
             "fields": ["S_W_ID", "S_I_ID"]
        },
        "createDeltaIdx": {
            "type": "CreateDeltaIndex",
            "index_name" : "mcidx__STOCK__delta__S_W_ID__S_I_ID",
            "fields": ["S_W_ID", "S_I_ID"]
        },
        "noop": {
            "type": "NoOp"
        }
    },
    "edges": [
        ["loadStock", "createMainIdx"], ["createMainIdx", "noop"],
        ["loadStock", "createDeltaIdx"], ["createDeltaIdx", "noop]
    ]
} """

Q_LOAD_STOCK_BINARY = """ {
    "operators": {
        "loadStock": {
            "type": "RecoverTable",
            "tablename": "STOCK",
            "path": "%(path)s",
            "threads": 20
        },
        "noop": {
            "type": "NoOp"
        }
    },
    "edges": [
        ["loadStock", "noop"]
    ]
} """

Q_INSERT_LINE = """ {
    "operators": {
        "getStock": {
            "type": "GetTable",
            "name": "STOCK"
        },
        "insert": {
            "type": "InsertScan",
            "data" : [%s]
        },
        "commit": {
            "type": "Commit"
        },
        "noop": {
            "type": "NoOp"
        }
    },
    "edges": [
        ["getStock", "insert"], ["insert", "commit"], ["commit", "noop"]
    ]
} """

Q_CHECKPOINT = """ {
    "operators": {
        "checkpoint": {
            "type": "Checkpoint",
            "withMain": %(withMain)s
        }
    },
    "edges": [
        ["checkpoint","checkpoint"]
    ]
} """

def clearFileSystemCache():
    sys.stdout.write("Clearing file system cache...")
    sys.stdout.flush()
    subprocess.call('sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"', shell=True)
    print " done."

class RecoveryBenchmark(benchmark.Benchmark):

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        benchmark.Benchmark.__init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs)
        self._tableSize       = int(kwargs["tableSize"])
        self._deltaPercentage = int(kwargs["deltaPercentage"])
        self._binaryLoad      = kwargs["binary"]
        self._withCheckpoint  = kwargs["withCheckpoint"] if kwargs.has_key("withCheckpoint") else False
        self._persistencyDir  = os.path.join("/mnt", "ramdisk", "STOCK_recovery", "%imio" % self._tableSize, "bin", "delta%i%s" % (self._deltaPercentage, "_log" if not self._withCheckpoint else ""))
        self._outputFile      = os.path.join(self._dirResults, "recoverytime.txt")

    def setTableSize(self, newSize):
        self._tableSize       = newSize
        self._persistencyDir  = os.path.join("/mnt", "ramdisk", "STOCK_recovery", "%imio" % self._tableSize, "bin", "delta%i%s" % (self._deltaPercentage, "_log" if not self._withCheckpoint else ""))

    def setDeltaPercentage(self, newPercentage):
        self._deltaPercentage = newPercentage
        self._persistencyDir  = os.path.join("/mnt", "ramdisk", "STOCK_recovery", "%imio" % self._tableSize, "bin", "delta%i%s" % (self._deltaPercentage, "_log" if not self._withCheckpoint else ""))

    def run(self):
        # first check if this one was generated already
        #if os.path.isfile(os.path.join(self._persistencyDir, "checkpoints", "__2__")):
        #    print "already processed, skipping"
        #    return
        #if self._binaryLoad and self._deltaPercentage == 0:
        #    print "nothing to do for Delta Percentage 0"
        #    return

        try:
            signal.signal(signal.SIGINT, self._signalHandler)
        except:
            print "Could not add signal handler."

        print "Generating into '%s'" % self._persistencyDir

        if not os.path.isdir(os.path.join(self._persistencyDir, "logs")):
            os.makedirs(os.path.join(self._persistencyDir, "logs"))
        if not os.path.isdir(os.path.join(self._persistencyDir, "checkpoints")):
            os.makedirs(os.path.join(self._persistencyDir, "checkpoints"))
        if not os.path.isdir(os.path.join(self._persistencyDir, "tables")):
            os.makedirs(os.path.join(self._persistencyDir, "tables"))

        if not self._manual:
            self._buildServer()
            self._startServer()
            print "---\nHYRISE server running on port %s\n---" % self._port
        else:
            print "---\nExpecting HYRISE server running on port %s\n---" % self._port

        print "Loading STOCK Table"
        csvPath = os.path.join("/mnt/fusion/STOCK_recovery", "%imio" % self._tableSize, "csv")
        binPath = os.path.join(self._persistencyDir, "tables")
        tableFileName = "STOCK_main_%imio_delta%i.tbl" % (self._tableSize, self._deltaPercentage)
        loadQuery = ""
        if self._binaryLoad:
            loadQuery = Q_LOAD_STOCK_BINARY % {"path": binPath}
        else:
            loadQuery = Q_LOAD_STOCK % {"filename": tableFileName, "path": csvPath}
        self.fireQuery(loadQuery)
        print "done."

        if self._deltaPercentage > 0:
            insertstr = ""
            queryFileName = os.path.join(csvPath, "STOCK_delta_%imio_delta%i.tbl" % (self._tableSize, self._deltaPercentage))
            deltaLines = int(1000000 * self._tableSize * (float(self._deltaPercentage) / 100.0))
            currentLine = 0
            for line in open(queryFileName):
                currentLine += 1
                values = tuple(line.replace("\n", "").split("|"))
                oneLine = '[%s, %s, %s, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", %s, %s, %s, "%s"]' % values
                if insertstr == "":
                    insertstr = oneLine
                else:
                    insertstr += ",%s" % oneLine
            query = Q_INSERT_LINE % insertstr
            print "Inserting Delta values..."
            self.fireQuery(query)
            print "done."

        print "Generating Checkpoint"
        self.fireQuery(Q_CHECKPOINT % {"withMain": "true" if not self._binaryLoad else "false"})
        print "done."

        self._stopServer()

    def runRecovery(self):
        print "\n=== Recovery with '%s' for STOCK of size %im (%i%% Delta) ===" % (self._buildSettings.getName(), self._tableSize, self._deltaPercentage)
        print "[Recovery Directory: %s]" % self._persistencyDir
        self._buildServer()

        env = {
            "HYRISE_DB_PATH"    : self._dirHyriseDB,
            "LD_LIBRARY_PATH"   : self._dirBinary+":/usr/local/lib64/"
        }
        server = os.path.join(self._dirBinary, "hyrise-server_%s" % self._buildSettings["BLD"])
        logdef = os.path.join(self._dirBinary, "log.properties")
        threadstring = ""
        if (self._serverThreads > 0):
            threadstring = "--threads=%s" % self._serverThreads
        persistency_str = ""
        if (self._persistencyDir != None):
            persistency_str = "--persistencyDir=%s" % self._persistencyDir

        # run recovery `n` times and take the average
        n = 3
        avgRecoveryTime = 0.0
        for i in range(n):
            clearFileSystemCache()
            proc = subprocess.Popen([server, "--port=%s" % self._port, "--logdef=%s" % logdef, threadstring, persistency_str, "--recoverAndExit"],
                                                cwd=self._dirBinary,
                                                env=env,
                                                stdout=open("/dev/null") if not self._stdout else None,
                                                stderr=open("/dev/null") if not self._stderr else None)
            proc.wait() # wait for server to terminate
            recoveryTimeFile = open(os.path.join(self._dirBinary, "recoverytime.txt"))
            recoveryTime = int(recoveryTimeFile.readline())
            recoveryTimeFile.close()
            print "Run #%i: Recovery time was %1.5fs" % (i+1, recoveryTime / 1000.0 / 1000.0)
            avgRecoveryTime += recoveryTime / n
        recoveryTimeFile = open(os.path.join(self._dirBinary, "recoverytime.txt"))
        recoveryTimeFile.readline()
        stockDeltaSize = 0
        for line in recoveryTimeFile:
            if line.find(";") < 0:
                continue
            name, size = line.split(";")
            if name == "STOCK":
                stockDeltaSize = int(size)
                break
        recoveryTimeFile.close()
        print "Average Recovery time was %1.5fs" % (avgRecoveryTime / 1000.0 / 1000.0)
        print "STOCK Table Delta size was %i" % stockDeltaSize
        open(self._outputFile, "w").write("%s;%s" % (str(avgRecoveryTime), str(stockDeltaSize)))


kwArgs = {}
kwArgs["host"]             = "localhost"
kwArgs["port"]             = 5432
kwArgs["serverThreads"]    = 20
kwArgs["tableSize"]        = 2
kwArgs["deltaPercentage"]  = 0
kwArgs["prepareQueries"]   = None
kwArgs["benchmarkQueries"] = None
kwArgs["remotePath"]       = None
kwArgs["stdout"]           = True
kwArgs["stderr"]           = True
kwArgs["binary"]           = args["binary"]
kwArgs["manual"]           = args["manual"]

s = benchmark.Settings("Generator",
                       BLD="release",
                       PERSISTENCY="BUFFEREDLOGGER",
                       WITH_GROUP_COMMIT=1,
                       WITH_MYSQL=0,
                       WITH_PAPI=0)
b = RecoveryBenchmark("Generator", "testrun", s, withCheckpoint=True, **kwArgs)

if args["skipGenerate"]:
    print "Skipping table dump generation..."
else:
    for tableSize in [2, 4, 6, 8, 10]:
        for deltaPercentage in [10, 20, 50]:
            print "\n=== Generating STOCK Table with %i,000,000 entries, Delta size %i%%" % (tableSize, deltaPercentage)
            b.setTableSize(tableSize)
            b.setDeltaPercentage(deltaPercentage)
            b.run()
            time.sleep(1)

# now for the actual benchmark
groupId = "STOCK_recovery"
sLogger = benchmark.Settings("Logger",
                             BLD="release",
                             PERSISTENCY="BUFFEREDLOGGER",
                             WITH_GROUP_COMMIT=1,
                             WITH_MYSQL=0,
                             WITH_PAPI=0)
sCheckpoint = benchmark.Settings("LoggerWithCheckpoint",
                                 BLD="release",
                                 PERSISTENCY="BUFFEREDLOGGER",
                                 WITH_GROUP_COMMIT=1,
                                 WITH_MYSQL=0,
                                 WITH_PAPI=0)
sNVRAM = benchmark.Settings("NVRAM",
                            BLD="release",
                            PERSISTENCY="NVRAM",
                            WITH_GROUP_COMMIT=1,
                            WITH_MYSQL=0,
                            WITH_PAPI=0)
clearFileSystemCache()
reset_nvram_directory()

for tableSize in [2, 4, 6, 8, 10]:
    for deltaPercentage in [0, 10, 20, 50]:
        kwArgs["tableSize"] = tableSize
        kwArgs["deltaPercentage"] = deltaPercentage
        runId = "%imio_delta%i" % (tableSize, deltaPercentage)

        bLogger = RecoveryBenchmark(groupId, runId, sLogger, **kwArgs)
        bLogger.runRecovery()

        bCheckpoint = RecoveryBenchmark(groupId, runId, sCheckpoint, withCheckpoint=True, **kwArgs)
        bCheckpoint.runRecovery()

        bNVRAM = RecoveryBenchmark(groupId, runId, sNVRAM, withCheckpoint=True, **kwArgs)
        bNVRAM.run()
        bNVRAM.runRecovery()
        reset_nvram_directory()
        time.sleep(20)
