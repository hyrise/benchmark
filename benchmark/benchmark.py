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
from profiler import Profiler

class Benchmark:

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        if(kwargs.has_key("remote") and kwargs["remote"]==True and (kwargs.has_key("dirBinary") or kwargs.has_key("hyriseDBPath"))):
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
        self._scheduler         = kwargs["scheduler"] if kwargs.has_key("scheduler") else "PartitionedQueuesScheduler"
        self._serverIP          = kwargs["serverIP"] if kwargs.has_key("serverIP") else "127.0.0.1"
        self._remoteUser        = kwargs["remoteUser"] if kwargs.has_key("remoteUser") else "hyrise"
        self._remotePath        = kwargs["remotePath"] if kwargs.has_key("remotePath") else "/home/" + kwargs["remoteUser"] + "/benchmark"
        self._abQueryFile       = kwargs["abQueryFile"] if kwargs.has_key("abQueryFile") else None
        self._abCore            = kwargs["abCore"] if kwargs.has_key("abCore") else 2
        self._verbose           = kwargs["verbose"] if kwargs.has_key("verbose") else 1
        self._write_to_file     = kwargs["write_to_file"] if kwargs.has_key("write_to_file") else None
        self._write_to_file_count = kwargs["write_to_file_count"] if kwargs.has_key("write_to_file_count") else None
        self._checkpoint_interval = str(kwargs["checkpointInterval"]) if kwargs.has_key("checkpointInterval") else None
        self._commit_window     = str(kwargs["commitWindow"]) if kwargs.has_key("commitWindow") else None
        self._csv                = kwargs["csv"] if kwargs.has_key("csv") else False
        self._nodes             = kwargs["nodes"] if kwargs.has_key("nodes") else None
        self._memorynodes       = kwargs["memorynodes"] if kwargs.has_key("memorynodes") else None
        self._vtune             = os.path.expanduser(kwargs["vtune"]) if kwargs.has_key("vtune") and kwargs["vtune"] is not None else None
        self._persistencyDir    = kwargs["persistencyDir"] if kwargs.has_key("persistencyDir") else None
        self._recoverOnStart    = kwargs["recoverOnStart"] if kwargs.has_key("recoverOnStart") else False
        self._with_profiler     = kwargs["profiler"] if kwargs.has_key("profiler") else None
        self._profiler = None

        if self._vtune is not None:
            self._manual = True
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

    def loadTables(self):
        """ implement this in subclasses """
        pass

    def benchAfterLoad(self):
        """ implement this in subclasses """
        pass

    def benchBeforeStop(self):
        """ implement this in subclasses """
        pass

    def benchAfter(self):
        """ implement this in subclasses """
        pass

    def preexec(self): # Don't forward signals.
        os.setpgrp()

    def allUsersFinished(self):
        for user in self._users:
            if user.is_alive():
                return False
        print "All users have terminated."
        return True

    def run(self):
        attempt = 0
        max_attempts = 3
        while(True):
            self.failed = False

            try:
                signal.signal(signal.SIGINT, self._signalHandler)
            except:
                print "Could not add signal handler."

            if self._with_profiler is not None:        
                self._profiler = Profiler(self._dirBinary)
                self._profiler.setup(self._with_profiler)

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
                if self._abQueryFile != None:
                    self._buildAb()
                self._startServer()
                print "---\nHYRISE server running on port %s\n---" % self._port
            else:
                print "---\nManual mode, expecting HYRISE server running on port %s\n---" % self._port

            self._runPrepareQueries()

            print "Preparing benchmark..."
            self.benchPrepare()
            self.loadTables()
            self.benchAfterLoad()

            if self._vtune is not None:
                subprocess.check_output("amplxe-cl -command resume", cwd=self._vtune, shell=True)

            if self._with_profiler is not None:        
                print "---\n"
                self._profiler.start(str(self._serverProc.pid))

            if self._runtime > 0:
                if self._abQueryFile != None:
                    print "---"
                    print "Using ab with queryfile=" + self._abQueryFile + ", concurrency=" + str(self._numUsers) + ", time=" + str(self._runtime) +"s"
                    print "Output File: ", self._dirResults + "/ab.log"
                    print "---"
                    ab = subprocess.Popen(["./ab/ab","-g", self._dirResults + "/ab.log", "-l", str(self._abCore), "-v", str(self._verbose), "-k", "-t", str(self._runtime), "-n", "99999999", "-c", str(self._numUsers), "-m", self._abQueryFile, self._host+":"+str(self._port)+"/procedure/"])
                    ab.wait()
                    r = ab.returncode
                    print "ab returned " + str(r)
                    if r:
                        self.failed = True
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
                        if self.allUsersFinished():
                            break
                        time.sleep(1)
                    print "Warming up... done     "

                    sys.stdout.write("Logging results for %i seconds... \r" % self._runtime)
                    sys.stdout.flush()
                    for i in range(self._numUsers):
                        self._users[i].startLogging()
                    for i in range(self._runtime):
                        sys.stdout.write("Logging results for %i seconds... \r" % (self._runtime - i))
                        sys.stdout.flush()
                        if self.allUsersFinished():
                            break
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
            if self._vtune is not None:
                subprocess.check_output("amplxe-cl -command stop", cwd=self._vtune, shell=True)
            self.benchBeforeStop()

            self._stopServer()


            if self._with_profiler is not None:
                print "---\n"
                self._profiler.end()

            print "all set"

            if self._remote:
                os.chdir(self._olddir)

            self.benchAfter()

            if not self.failed:
                return True

            attempt += 1
            if(attempt == max_attempts):
                print "Failed - not trying again after " + str(max_attempts) + " attempts"
                subprocess.call("rm " + os.path.expandvars("/mnt/pmfs/$USER/hyrisedata/") + "* " + os.path.expandvars("/mnt/pmfs/$USER/") + "*", shell=True)
                return False
            print "Failed - trying again (attempt " + str(attempt) + " of " + str(max_attempts) + ")"
            try:
                subprocess.call("rm " + os.path.expandvars("/mnt/pmfs/$USER/hyrisedata/") + "*", shell=True)
                subprocess.call("rm " + os.path.expandvars("/mnt/pmfs/$USER/") + "*", shell=True)
                subprocess.call("kill -9 `pgrep hyrise`", shell=True)
            except OSError:
                pass
            time.sleep(10)


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

    def _buildAb(self):
        sys.stdout.write("Building ab tool... ")
        sys.stdout.flush()
        process = subprocess.Popen("make ab", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd="./ab")
        (stdout, stderr) = process.communicate()
        returncode = process.returncode
        if returncode != 0:
            print stderr
            raise Exception("ERROR: building ab tool failed with return code %s:\n===\n%s" % (self._settings.getName(), returncode, stderr))
        else:
            print "done"

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

    def _startServer(self, paramString=""):
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

            checkpoint_str = ""
            if (self._checkpoint_interval != None):
                checkpoint_str = "--checkpointInterval=%s" % self._checkpoint_interval

            commit_window_str = ""
            if (self._commit_window != None):
                commit_window_str = "--commitWindow=%s" % self._commit_window

            nodes_str = ""
            if (self._nodes != None):
                nodes_str = "--nodes=%s" % self._nodes

            memorynodes_str = ""
            if (self._memorynodes != None):
                memorynodes_str = "--memorynodes=%s" % self._memorynodes

            persistency_str = ""
            if (self._persistencyDir != None):
                persistency_str = "--persistencyDir=%s" % self._persistencyDir

            recovery_str = ""
            if (self._recoverOnStart):
                recovery_str = "--recover"

            self._serverProc = subprocess.Popen([server, "--port=%s" % self._port, "--logdef=%s" % logdef, "--scheduler=%s" % self._scheduler, nodes_str, memorynodes_str, checkpoint_str, threadstring, commit_window_str, persistency_str, recovery_str],
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
            self._users.append(self._userClass(userId=i, host=self._host, port=self._port, dirOutput=self._dirResults, queryDict=self._queryDict, collectPerfData=self._collectPerfData, useJson=self._useJson, write_to_file=self._write_to_file, write_to_file_count=self._write_to_file_count, **self._userArgs))

    def _stopServer(self):
        if not self._remote:
            if not self._manual and self._serverProc:
                try:
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
                        subprocess.call(["killall", "-u", os.getlogin(), "hyrise-server_release"])
                    time.sleep(5)
                    del self._serverProc
                except:
                    self.failed = True
                    del self._serverProc
                    return
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

