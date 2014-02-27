import build
import os
import requests
import settings
import signal
import subprocess
import sys
import time
import user
import multiprocessing
import paramiko

from queries import *
import queries

class Benchmark:

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        if(kwargs.has_key("remote") and (kwargs.has_key("dirBinary") or kwargs.has_key("hyriseDBPath"))):
            print "dirBinary and hyriseDBPath cannot be used with remote"
            exit()

        self._pid               = os.getpid()
        self._id                = benchmarkGroupId
        self._runId             = benchmarkRunId
        self._buildSettings     = buildSettings
        self._userClass         = kwargs["userClass"] if kwargs.has_key("userClass") else user.User
        self._numUsers          = kwargs["numUsers"] if kwargs.has_key("numUsers") else 1
        self._mysqlDB           = kwargs["mysqlDB"] if kwargs.has_key("mysqlDB") else "cbtr"
        self._mysqlHost         = kwargs["mysqlHost"] if kwargs.has_key("mysqlHost") else "vm-hyrise-jenkins.eaalab.hpi.uni-potsdam.de"
        self._mysqlPort         = kwargs["mysqlPort"] if kwargs.has_key("mysqlPort") else 3306
        self._mysqlUser         = kwargs["mysqlUser"] if kwargs.has_key("mysqlUser") else "hyrise"
        self._mysqlPass         = kwargs["mysqlPass"] if kwargs.has_key("mysqlPass") else "hyrise"
        self._papi              = kwargs["papi"] if kwargs.has_key("papi") else "NO_PAPI"
        self._prepQueries       = kwargs["prepareQueries"] if kwargs.has_key("prepareQueries") else queries.QUERIES_PREPARE
        self._prepArgs          = kwargs["prepareArgs"] if kwargs.has_key("prepareArgs") else {"db": "cbtr"}
        self._queries           = kwargs["benchmarkQueries"] if kwargs.has_key("benchmarkQueries") else queries.QUERIES_ALL
        self._host              = kwargs["host"] if kwargs.has_key("host") else "127.0.0.1"
        self._port              = kwargs["port"] if kwargs.has_key("port") else 5000
        self._warmuptime        = kwargs["warmuptime"] if kwargs.has_key("warmuptime") else 0
        self._runtime           = kwargs["runtime"] if kwargs.has_key("runtime") else 5
        self._thinktime         = kwargs["thinktime"] if kwargs.has_key("thinktime") else 0
        self._manual            = kwargs["manual"] if kwargs.has_key("manual") else False
        self._rebuild           = kwargs["rebuild"] if kwargs.has_key("rebuild") else False
        self._userArgs          = kwargs["userArgs"] if kwargs.has_key("userArgs") else {"queries": self._queries}
        self._stdout            = kwargs["showStdout"] if kwargs.has_key("showStdout") else False
        self._stderr            = kwargs["showStderr"] if kwargs.has_key("showStderr") else True
        self._remote            = kwargs["remote"] if kwargs.has_key("remote") else False
        self._dirBinary         = kwargs["dirBinary"] if kwargs.has_key("dirBinary") else os.path.join(os.getcwd(), "builds/%s" % buildSettings.getName())
        self._dirHyriseDB       = kwargs["hyriseDBPath"] if kwargs.has_key("hyriseDBPath") else self._dirBinary
        self._dirResults        = os.path.join(os.getcwd(), "results", self._id, self._runId, buildSettings.getName())
        # self._queryDict         = self._readDefaultQueryFiles()
        self._queryDict         = {}
        self._session           = requests.Session()
        self._serverThreads     = kwargs["serverThreads"] if kwargs.has_key("serverThreads") else 0
        self._collectPerfData   = kwargs["collectPerfData"] if kwargs.has_key("collectPerfData") else False
        self._useJson           = kwargs["useJson"] if kwargs.has_key("useJson") else False
        self._build             = None
        self._serverProc        = None
        self._users             = []
        self._scheduler         = kwargs["scheduler"] if kwargs.has_key("scheduler") else "CoreBoundQueuesScheduler"
        self._serverIP          = kwargs["serverIP"] if kwargs.has_key("serverIP") else "127.0.0.1"
        self._remoteUser        = kwargs["remoteUser"] if kwargs.has_key("remoteUser") else "hyrise"
        self._remotePath        = kwargs["remotePath"] if kwargs.has_key("remotePath") else "/home/" + kwargs["remoteUser"] + "/benchmark"
        self._abQueryFile       = kwargs["abQueryFile"] if kwargs.has_key("abQueryFile") else None
        self._abCore            = kwargs["abCore"] if kwargs.has_key("abCore") else 2
        self._verbose           = kwargs["verbose"] if kwargs.has_key("verbose") else 0
        self._write_to_file     = kwargs["write_to_file"] if kwargs.has_key("write_to_file") else None
        if self._remote:
            self._ssh               = paramiko.SSHClient()
        else:
            self._ssh           = None
        self._exiting           = False


        self._session.headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        if not os.path.isdir(self._dirResults):
            os.makedirs(self._dirResults)

    def benchPrepare(self):
        """ implement this in subclasses """
        pass

    def preexec(self): # Don't forward signals.
        os.setpgrp()

    def run(self):
        signal.signal(signal.SIGINT, self._signalHandler)

        print "+------------------+"
        print "| HYRISE benchmark |"
        print "+------------------+\n"

        if self._remote:
            subprocess.call(["mkdir", "-p", "remotefs/" + self._host])
            subprocess.call(["fusermount", "-u", "remotefs/127.0.0.1"])
            subprocess.Popen(["sshfs", self._remoteUser + "@" + self._host + ":" + self._remotePath, "remotefs/" + self._host + "/"], preexec_fn = self.preexec)
            self._olddir = os.getcwd()
            os.chdir("remotefs/" + self._host + "/")
            self._dirBinary         = os.path.join(os.getcwd(), "builds/%s" % self._buildSettings.getName())
            self._dirHyriseDB       = os.path.join(os.getcwd(), "hyrise")
            self._startSSHConnection()


        if not self._manual:
            # no support for building on remote machine yet
            self._buildServer()
            self._startServer()
            print "---\nHYRISE server running on port %s\n---" % self._port
        else:
            print "---\nManual mode, expecting HYRISE server running on port %s\n---" % self._port

        self._runPrepareQueries()

        print "Preparing benchmark..."
        self.benchPrepare()
        self.loadTables()

        if self._abQueryFile != None:
            print "---"
            print "Using ab with queryfile=" + self._abQueryFile + ", concurrency=" + str(self._numUsers) + ", time=" + str(self._runtime) +"s"
            print "Output File: ", self._dirResults + "/ab.log"
            print "---"
            ab = subprocess.Popen(["./ab/ab","-g", self._dirResults + "/ab.log", "-l", str(self._abCore), "-v", str(self._verbose), "-k", "-t", str(self._runtime), "-c", str(self._numUsers), "-m", self._abQueryFile, self._host+":"+str(self._port)+"/procedure/"])
            ab.wait()
        else:
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
            print "users stopped"
            time.sleep(2)
            for i in range(self._numUsers):
                sys.stdout.write("Stopping %s user(s)... %i%%      \r" % (self._numUsers, (i+1.0) / self._numUsers * 100))
                sys.stdout.flush()
                self._users[i].join()
        
        print "Stopping %s user(s)... done     " % self._numUsers
        self._stopServer()
        print "all set"

        if self._remote:
            os.chdir(self._olddir)


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

    # def _readDefaultQueryFiles(self):
    #     cwd = os.getcwd()
    #     queryDict = {}
    #     for queryId, filename in queries.QUERY_FILES.iteritems():
    #         queryDict[queryId] = open(os.path.join(cwd, filename)).read()
    #     return queryDict

    def _buildServer(self):
        sys.stdout.write("%suilding server for build '%s'... " % ("B" if not self._rebuild else "Reb", self._buildSettings.getName()))
        sys.stdout.flush()
        if self._build == None:
            self._build = build.Build(settings=self._buildSettings, ssh=self._ssh, remotePath = self._remotePath)
            if self._rebuild:
                self._build.makeClean()
            self._build.makeAll()
        elif self._rebuild:
            self._build.makeClean()
            self._build.makeAll()
        print "done"

    def _startServer(self):
        if not self._remote:
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

            #server = os.path.join(self._dirBinary, "hyrise-server_debug")

            logdef = os.path.join(self._dirBinary, "log.properties")
            threadstring = ""
            if (self._serverThreads > 0):
                threadstring = "--threads=%s" % self._serverThreads
            self._serverProc = subprocess.Popen([server, "--port=%s" % self._port, "--logdef=%s" % logdef, "--scheduler=%s" % self._scheduler, threadstring],
                                                cwd=self._dirBinary,
                                                env=env,
                                                stdout=open("/dev/null") if not self._stdout else None,
                                                stderr=open("/dev/null") if not self._stderr else None)
        else: 
            self._startRemoteServer()

        time.sleep(1)
        print "done"


    def _startRemoteServer(self):
        print("Starting server for build '%s'... remotely on '%s'" % (self._buildSettings.getName(), self._host))

        env = "HYRISE_DB_PATH="+str(self._dirHyriseDB)+\
              " LD_LIBRARY_PATH="+str(self._dirBinary)+":/usr/local/lib64/"+\
              " HYRISE_MYSQL_PORT="+str(self._mysqlPort)+\
              " HYRISE_MYSQL_HOST="+str(self._mysqlHost)+\
              " HYRISE_MYSQL_USER="+str(self._mysqlUser)+\
              " HYRISE_MYSQL_PASS="+str(self._mysqlPass)

        if self._buildSettings.oldMode():
            server = os.path.join(self._dirBinary, "hyrise_server")
        else:
            server = os.path.join(self._dirBinary, "hyrise-server_%s" % self._buildSettings["BLD"])

        logdef = os.path.join(self._dirBinary, "log.properties")
        
        threadstring = ""
        
        if (self._serverThreads > 0):
            threadstring = "--threads=%s" % self._serverThreads
          
        # note: there is an issue with large outputs of the server command;
        # the remote command hangs, probably when the channel buffer is full
        # either write to /dev/null on server machine of a file on server side
        # otherwise, get the transport and read from a channel
        command_str = "cd " + str(self._dirBinary) + "; env " + env + " " + server + " --port=%s" % self._port + " --logdef=%s" % logdef + " --scheduler=%s" % self._scheduler + " " + threadstring + " 2&>1 &> ~/hyriselog"
        command_str = command_str.replace(os.path.join(os.getcwd()), self._remotePath)
        stdin, stdout, stderr = self._ssh.exec_command(command_str);

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
                r = self.fireQuery(self._queryDict[q], self._prepArgs)
                #self._session.post("http://%s:%s/" % (self._host, self._port), data={"query": queryString})
            except Exception:
                print "Running prepare queries... %i%% --> Error" % ((i+1.0) / numQueries * 100)
        print "Running prepare queries... done"



    def _createUsers(self):
        for i in range(self._numUsers):
            self._users.append(self._userClass(userId=i, host=self._host, port=self._port, dirOutput=self._dirResults, queryDict=self._queryDict, collectPerfData=self._collectPerfData, useJson=self._useJson, write_to_file=self._write_to_file, **self._userArgs))

    def _stopServer(self):
        if not self._remote: 
            if not self._manual and self._serverProc:
                sys.stdout.write("Stopping server... ")
                sys.stdout.flush()
                self._serverProc.terminate()
                time.sleep(0.5)
                if self._serverProc.poll() is None:
                    #print "Server still running. Waiting 2 sec and forcing shutdown..."
                    time.sleep(2)
                    self._serverProc.kill()
                time.sleep(0.5)
                if self._serverProc.poll() is None:
                    subprocess.call("killall hyrise-server_release")
                time.sleep(0.5)
        else:
            print "kill server, close connection"
            self._ssh.exec_command("killall hyrise-server_release");
            self._stopSSHConnection()
        print "done."

    def _signalHandler(self, signal, frame):
        if os.getppid() == self._pid or self._exiting:
            return
        self._exiting = True
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

    def _startSSHConnection(self):
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # expects authentication per key on remote server
        self._ssh.connect(self._host, username=self._remoteUser)
        print "connected"

    def _stopSSHConnection(self):
        self._ssh.close()

