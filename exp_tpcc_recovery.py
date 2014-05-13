from tpcc_parameters import *
import time
import subprocess
import os
import requests
import sys

CHECKPOINT_WITH_MAIN_JSON = """
{"operators": {"l": {"type": "Checkpoint", "withMain": true}}, "edges": [["l","l"]]}
"""
CHECKPOINT_JSON = """
{"operators": {"l": {"type": "Checkpoint", "withMain": false}}, "edges": [["l","l"]]}
"""

def clearFileSystemCache():
    sys.stdout.write("Clearing file system cache...")
    sys.stdout.flush()
    subprocess.call('sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"', shell=True)
    print " done."

class RecoveryBenchmark(benchmark.TPCCBenchmark):

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        benchmark.TPCCBenchmark.__init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs)
        self.createCheckpoint = kwargs["createCheckpoint"] if kwargs.has_key("createCheckpoint") else False
        self.outputFile = os.path.join(self._dirResults, "recoverytime.txt")
        self.skipRecovery = kwargs["skipRecovery"] if kwargs.has_key("skipRecovery") else False
        self.persistOnLoad = kwargs["persistOnLoad"] if kwargs.has_key("persistOnLoad") else False
        self.failed = False

    def benchAfterLoad(self):
        # persist the main after load
        if not self.noLoad and self.persistOnLoad:
            self.fireQuery(CHECKPOINT_WITH_MAIN_JSON)


    def benchBeforeStop(self):
        if self.createCheckpoint:
            print "Creating Checkpoint"
            try:
                self.fireQuery(CHECKPOINT_JSON)
            except requests.ConnectionError:
                # probably means Hyrise died in the meantime
                self.failed = True
            print "Done"
        else:
            if self._serverProc.poll() != None:
                # server has stopped and that should not be the case just yet...
                self.failed = True

    def benchAfter(self):
        """ benchmark run is finished, hyrise server stopped, tables persisted
            restart with --recoverAndExit flag and take the time """
        if self.skipRecovery or self.failed:
            return
        if self._remote or self._manual:
            print "cannot benchmark recovery on manual or remote build!"
            return
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
        print "Average Recovery time was %1.5fs" % (avgRecoveryTime / 1000.0 / 1000.0)
        print "STOCK Table Delta size was %i" % stockDeltaSize
        open(self.outputFile, "w").write("%s;%s" % (str(avgRecoveryTime), str(stockDeltaSize)))

# Recovery Benchmark Settings
groupId   = "tpcc_recovery"
tmpGroupId= "tpcc_recovery_tmp"
sLogger   = benchmark.Settings("Logger",
                               BLD="release",
                               PERSISTENCY="BUFFEREDLOGGER",
                               WITH_GROUP_COMMIT=1,
                               WITH_MYSQL=0,
                               WITH_PAPI=0)
sLoggerCP = benchmark.Settings("LoggerWithCheckpoint",
                               BLD="release",
                               PERSISTENCY="BUFFEREDLOGGER",
                               WITH_GROUP_COMMIT=1,
                               WITH_MYSQL=0,
                               WITH_PAPI=0)
sNVRAM    = benchmark.Settings("NVRAM",
                               BLD="release",
                               PERSISTENCY="NVRAM",
                               NVRAM_FOLDER=os.path.join("/mnt/pmfs", os.environ["USER"]),
                               WITH_GROUP_COMMIT=1,
                               WITH_MYSQL=0,
                               WITH_PAPI=0)
persistencyDirLogger   = os.path.join("/", "mnt", "fusion", "persistency", sLogger.getName())
if not os.path.isdir(persistencyDirLogger):
    os.makedirs(persistencyDirLogger)
else:
    clear_dir(persistencyDirLogger)
os.makedirs(os.path.join(persistencyDirLogger, "logs"))
os.makedirs(os.path.join(persistencyDirLogger, "checkpoints"))
os.makedirs(os.path.join(persistencyDirLogger, "tables"))
persistencyDirLoggerCP = os.path.join("/", "mnt", "fusion", "persistency", sLoggerCP.getName())
if not os.path.isdir(persistencyDirLoggerCP):
    os.makedirs(persistencyDirLoggerCP)
else:
    clear_dir(persistencyDirLoggerCP)
os.makedirs(os.path.join(persistencyDirLoggerCP, "logs"))
os.makedirs(os.path.join(persistencyDirLoggerCP, "checkpoints"))
os.makedirs(os.path.join(persistencyDirLoggerCP, "tables"))
reset_nvram_directory()
clearFileSystemCache()

# Prepare benchmarks by initally loading and dumping tables
print "===== Running Preparation =====\n"
tmpRunId = "recovery_prepare"
kwargs["scheduler"] = "WSCoreBoundQueuesScheduler"
kwargs["warmuptime"] = 0
kwargs["runtime"] = 0
kwargs["numUsers"] = 20
bTmpLogger = RecoveryBenchmark(tmpGroupId, tmpRunId, sLogger, persistencyDir=persistencyDirLogger, persistOnLoad=True, skipRecovery=True, **kwargs)
bTmpLogger.run()
time.sleep(1)
clear_dir(os.path.join(persistencyDirLogger, "logs"))
clear_dir(os.path.join(persistencyDirLogger, "checkpoints"))
bTmpLoggerCP = RecoveryBenchmark(tmpGroupId, tmpRunId, sLoggerCP, persistencyDir=persistencyDirLoggerCP, persistOnLoad=True, skipRecovery=True, **kwargs)
bTmpLoggerCP.run()
time.sleep(1)
clear_dir(os.path.join(persistencyDirLoggerCP, "logs"))
clear_dir(os.path.join(persistencyDirLoggerCP, "checkpoints"))
clearFileSystemCache()
print "\n===== Finished Preparation =====\n"

for runtime in [0, 20, 40, 60, 80, 100]:
    runId = "deltaFilltime%s" % runtime
    kwArgs = kwargs.copy()
    kwArgs["runtime"] = runtime

    success = False
    while not success:
        bLogger = RecoveryBenchmark(groupId, runId, sLogger, persistencyDir=persistencyDirLogger, **kwArgs)
        success = bLogger.run()
        clear_dir(os.path.join(persistencyDirLogger, "logs"))
        clear_dir(os.path.join(persistencyDirLogger, "checkpoints"))

    success = False
    while not success:
        bLoggerCP = RecoveryBenchmark(groupId, runId, sLoggerCP, persistencyDir=persistencyDirLoggerCP, createCheckpoint=True, **kwArgs)
        success = bLoggerCP.run()
        clear_dir(os.path.join(persistencyDirLoggerCP, "logs"))
        clear_dir(os.path.join(persistencyDirLoggerCP, "checkpoints"))

    success = False
    while not success:
        bNVRAM = RecoveryBenchmark(groupId, runId, sNVRAM, **kwArgs)
        success = bNVRAM.run()
        reset_nvram_directory()
