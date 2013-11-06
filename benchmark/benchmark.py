import build
import os
import queries
import requests
import settings
import signal
import subprocess
import sys
import time
import user
import multiprocessing

class Benchmark:

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        self._pid           = os.getpid()
        self._id            = benchmarkGroupId
        self._runId         = benchmarkRunId
        self._buildSettings = buildSettings
        self._userClass     = kwargs["userClass"] if kwargs.has_key("userClass") else user.User
        self._numUsers      = kwargs["numUsers"] if kwargs.has_key("numUsers") else 1
        self._mysqlDB       = kwargs["mysqlDB"] if kwargs.has_key("mysqlDB") else "cbtr"
        self._mysqlHost     = kwargs["mysqlHost"] if kwargs.has_key("mysqlHost") else "vm-hyrise-jenkins.eaalab.hpi.uni-potsdam.de"
        self._mysqlPort     = kwargs["mysqlPort"] if kwargs.has_key("mysqlPort") else 3306
        self._mysqlUser     = kwargs["mysqlUser"] if kwargs.has_key("mysqlUser") else "hyrise"
        self._mysqlPass     = kwargs["mysqlPass"] if kwargs.has_key("mysqlPass") else "hyrise"
        self._papi          = kwargs["papi"] if kwargs.has_key("papi") else "NO_PAPI"
        self._prepQueries   = kwargs["prepareQueries"] if kwargs.has_key("prepareQueries") else queries.QUERIES_PREPARE
        self._prepArgs      = kwargs["prepareArgs"] if kwargs.has_key("prepareArgs") else {"db": "cbtr"}
        self._queries       = kwargs["benchmarkQueries"] if kwargs.has_key("benchmarkQueries") else queries.QUERIES_ALL
        self._host          = kwargs["host"] if kwargs.has_key("host") else "127.0.0.1"
        self._port          = kwargs["port"] if kwargs.has_key("port") else 5000
        self._warmuptime    = kwargs["warmuptime"] if kwargs.has_key("warmuptime") else 0
        self._runtime       = kwargs["runtime"] if kwargs.has_key("runtime") else 5
        self._thinktime     = kwargs["thinktime"] if kwargs.has_key("thinktime") else 0
        self._manual        = kwargs["manual"] if kwargs.has_key("manual") else False
        self._rebuild       = kwargs["rebuild"] if kwargs.has_key("rebuild") else False
        self._userArgs      = kwargs["userArgs"] if kwargs.has_key("userArgs") else {"queries": self._queries}
        self._stdout        = kwargs["showStdout"] if kwargs.has_key("showStdout") else False
        self._stderr        = kwargs["showStderr"] if kwargs.has_key("showStderr") else True
        self._dirBinary     = os.path.join(os.getcwd(), "builds/%s" % buildSettings.getName())
        self._dirHyriseDB   = kwargs["hyriseDBPath"] if kwargs.has_key("hyriseDBPath") else self._dirBinary
        self._dirResults    = os.path.join(os.getcwd(), "results", self._id, self._runId, buildSettings.getName())
        self._queryDict     = self._readDefaultQueryFiles()
        self._session       = requests.Session()
        self._build         = None
        self._serverProc    = None
        self._users         = []

        self._session.headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        if not os.path.isdir(self._dirResults):
            os.makedirs(self._dirResults)

    def benchPrepare(self):
        """ implement this in subclasses """
        pass

    def run(self):
        signal.signal(signal.SIGINT, self._signalHandler)

        print "+------------------+"
        print "| HYRISE benchmark |"
        print "+------------------+\n"

        if not self._manual:
            self._buildServer()
            self._startServer()
            print "---\nHYRISE server running on port %s\n---" % self._port
        else:
            print "---\nManual mode, expecting HYRISE server running on port %s\n---" % self._port

        self._runPrepareQueries()

        print "Preparing benchmark..."
        self.benchPrepare()

        self._createUsers()
        sys.stdout.write("Starting %s user(s)...\r" % self._numUsers)
        sys.stdout.flush()
        for i in range(self._numUsers):
            sys.stdout.write("Starting %s user(s)... %i%%      \r" % (self._numUsers, (i+1.0) / self._numUsers * 100))
            sys.stdout.flush()
            self._users[i].start()
        print "Starting %s user(s)... done     " % self._numUsers

        for i in range(self._warmuptime):
            sys.stdout.write("Warming up... %i   \r" % (self._warmuptime - i))
            sys.stdout.flush()
            time.sleep(1)
        print "Warming up... done     "

        sys.stdout.write("Logging results for %i seconds... \r" % self._runtime)
        sys.stdout.flush()
        for i in range(self._numUsers):
            self._users[i].startLogging()
        for i in range(self._runtime):
            sys.stdout.write("Logging results for %i seconds... \r" % (self._runtime - i))
            sys.stdout.flush()
            time.sleep(1)
        #time.sleep(self._runtime)
        for i in range(self._numUsers):
            self._users[i].stopLogging()
        print "Logging results for %i seconds... done" % self._runtime

        sys.stdout.write("Stopping %s user(s)...\r" % self._numUsers)
        sys.stdout.flush()
        for i in range(self._numUsers):
            self._users[i].stop()
        for i in range(self._numUsers):
            sys.stdout.write("Stopping %s user(s)... %i%%      \r" % (self._numUsers, (i+1.0) / self._numUsers * 100))
            sys.stdout.flush()
            self._users[i].join()
        print "Stopping %s user(s)... done     " % self._numUsers
        self._stopServer()

    def addQuery(self, queryId, queryStr):
        if self._queryDict.has_key(queryId):
            raise Exception("a query with id '%s' is already registered" % queryId)
        else:
            self._queryDict[queryId] = queryStr

    def addQueryFile(self, queryId, filename):
        if self._queryDict.has_key(queryId):
            raise Exception("a query with id '%s' is already registered" % queryId)
        else:
            self._queryDict[queryId] = open(filename).read()

    def setUserClass(self, userClass):
        self._userClass = userClass

    def setUserArgs(self, args):
        self._userArgs = args

    def fireQuery(self, queryString, queryArgs={}, sessionContext=None, autocommit=False):
        query = queryString % queryArgs
        data = {"query": query}
        if sessionContext: data["sessionContext"] = sessionContext
        if autocommit: data["autocommit"] = "true"
        return self._session.post("http://%s:%s/" % (self._host, self._port), data=data)

    def _readDefaultQueryFiles(self):
        cwd = os.getcwd()
        queryDict = {}
        for queryId, filename in queries.QUERY_FILES.iteritems():
            queryDict[queryId] = open(os.path.join(cwd, filename)).read()
        return queryDict

    def _buildServer(self):
        sys.stdout.write("%suilding server for build '%s'... " % ("B" if not self._rebuild else "Reb", self._buildSettings.getName()))
        sys.stdout.flush()
        if self._build == None:
            self._build = build.Build(settings=self._buildSettings)
            if self._rebuild:
                self._build.makeClean()
            self._build.makeAll()
        elif self._rebuild:
            self._build.makeClean()
            self._build.makeAll()
        print "done"

    def _startServer(self):
        sys.stdout.write("Starting server for build '%s'... " % self._buildSettings.getName())
        sys.stdout.flush()
        env = {
            "HYRISE_DB_PATH"    : self._dirHyriseDB,
            "LD_LIBRARY_PATH"   : self._dirBinary+":/usr/local/lib64/",
            "HYRISE_MYSQL_PORT" : str(self._mysqlPort),
            "HYRISE_MYSQL_HOST" : self._mysqlHost,
            "HYRISE_MYSQL_USER" : self._mysqlUser,
            "HYRISE_MYSQL_PASS" : self._mysqlPass
        }
        if self._buildSettings.oldMode():
            server = os.path.join(self._dirBinary, "hyrise_server")
        else:
            server = os.path.join(self._dirBinary, "hyrise-server_%s" % self._buildSettings["BLD"])
        logdef = os.path.join(self._dirBinary, "log.properties")
        self._serverProc = subprocess.Popen([server, "--port=%s" % self._port, "--logdef=%s" % logdef, "--scheduler=CoreBoundQueuesScheduler"],
                                            cwd=self._dirBinary,
                                            env=env,
                                            stdout=open("/dev/null") if not self._stdout else None,
                                            stderr=open("/dev/null") if not self._stderr else None)
        time.sleep(1)
        print "done"

    def _runPrepareQueries(self):
        if self._prepQueries == None or len(self._prepQueries) == 0:
            return
        numQueries = len(self._prepQueries)
        for i, q in enumerate(self._prepQueries):
            sys.stdout.write("Running prepare queries... %i%%      \r" % ((i+1.0) / numQueries * 100))
            sys.stdout.flush()
            try:
                self.fireQuery(self._queryDict[q], self._prepArgs)
                self._session.post("http://%s:%s/" % (self._host, self._port), data={"query": queryString})
            except Exception:
                print "Running prepare queries... %i%% --> Error" % ((i+1.0) / numQueries * 100)
        print "Running prepare queries... done"

    def _createUsers(self):
        for i in range(self._numUsers):
            self._users.append(self._userClass(userId=i, host=self._host, port=self._port, dirOutput=self._dirResults, queryDict=self._queryDict, **self._userArgs))

    def _stopServer(self):
        if not self._manual and self._serverProc:
            sys.stdout.write("Stopping server... ")
            sys.stdout.flush()
            self._serverProc.terminate()
            print "done."

    def _signalHandler(self, signal, frame):
        print "\n*** received SIGINT, initiating graceful shutdown"
        if self._build:
            self._build.unlink()
        for u in self._users:
            u.stopLogging()
            u.stop()
        self._stopServer()
        for u in self._users:
            u.join()
        exit()

