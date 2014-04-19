from tpcc_parameters import *
import time
import subprocess
import os
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
    subprocess.call('sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"', shell=True)
    print " done."

class RecoveryBenchmark(benchmark.TPCCBenchmark):

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        benchmark.TPCCBenchmark.__init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs)
        self.createCheckpoint = kwargs["createCheckpoint"] if kwargs.has_key("createCheckpoint") else False
        self.outputFile = os.path.join(self._dirResults, "recoverytime.txt")

    def benchAfterLoad(self):
        # persist the main after load
        self.fireQuery(CHECKPOINT_WITH_MAIN_JSON)

    def benchBeforeStop(self):
        if self.createCheckpoint:
            print "Creating Checkpoint"
            self.fireQuery(CHECKPOINT_JSON)
            print "Done"

    def benchAfter(self):
        """ benchmark run is finished, hyrise server stopped, tables persisted
            restart with --recoverAndExit flag and take the time """
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
        clearFileSystemCache()
        proc = subprocess.Popen([server, "--port=%s" % self._port, "--logdef=%s" % logdef, threadstring, "--recoverAndExit"],
                                            cwd=self._dirBinary,
                                            env=env,
                                            stdout=open("/dev/null") if not self._stdout else None,
                                            stderr=open("/dev/null") if not self._stderr else None)
        proc.wait() # wait for server to terminate
        recoveryTime = int(open(os.path.join(self._dirBinary, "recoverytime.txt")).read())
        print "Recovery time was %1.5fs" % (recoveryTime / 1000.0 / 1000.0)
        open(self.outputFile, "w").write(str(recoveryTime))

groupId = "tpcc_recovery"

sLogger   = benchmark.Settings("Logger", BLD="release", PERSISTENCY="BUFFEREDLOGGER", WITH_GROUP_COMMIT=0)
sLoggerCP = benchmark.Settings("LoggerWithCheckpoint", BLD="release", PERSISTENCY="BUFFEREDLOGGER", WITH_GROUP_COMMIT=0)
sNVRAM    = benchmark.Settings("NVRAM", BLD="release", PERSISTENCY="NVRAM", NVRAM_FOLDER="/mnt/pmfs/Tim.Berning", WITH_GROUP_COMMIT=0)

clear_dir(os.path.join(os.getcwd(), "builds", "Logger", "persistency"))
clear_dir(os.path.join(os.getcwd(), "builds", "LoggerWithCheckpoint", "persistency"))
clear_dir(os.path.join("/mnt/pmfs", os.environ["USER"], "hyrisedata"))
clearFileSystemCache()

for runtime in [0, 20, 40]:
    runId = "deltaFilltime%s" % runtime
    kwArgs = kwargs.copy()
    kwArgs["runtime"] = runtime
    kwArgs["numUsers"] = 20
    kwArgs["warmuptime"] = 0
    kwArgs["scheduler"] = "WSCoreBoundQueuesScheduler"

    bLogger   = RecoveryBenchmark(groupId, runId, sLogger, **kwArgs)
    bLoggerCP = RecoveryBenchmark(groupId, runId, sLoggerCP, createCheckpoint=True, **kwArgs)
    bNVRAM    = RecoveryBenchmark(groupId, runId, sNVRAM, **kwArgs)

    bLogger.run()
    clear_dir(os.path.join(os.getcwd(), "builds", "Logger", "persistency"))
    bLoggerCP.run()
    clear_dir(os.path.join(os.getcwd(), "builds", "LoggerWithCheckpoint", "persistency"))
    reset_nvram_directory()
    bNVRAM.run()
    reset_nvram_directory()
    clear_dir(os.path.join("/mnt/pmfs", os.environ["USER"], "hyrisedata"))

