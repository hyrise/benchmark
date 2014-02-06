import json
import os
import random
import requests
import multiprocessing
import time
import cProfile

class User(multiprocessing.Process):
    """
    Benchmark User base class
    """

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        multiprocessing.Process.__init__(self)

        self._userId            = userId
        self._host              = host
        self._port              = port
        self._session           = requests.Session()
        self._dirOutput         = os.path.join(dirOutput, str(userId))
        self._queryDict         = queryDict
        self._thinkTime         = kwargs["thinkTime"] if kwargs.has_key("thinkTime") else 0
        self._papi              = kwargs["papi"] if kwargs.has_key("papi") else "NO_PAPI"
        self._stopevent         = multiprocessing.Event()
        self._logevent          = multiprocessing.Event()
        self._logging           = False
        self._log               = {}
        self._lastQuery         = None
        self._collectPerfData   = kwargs["collectPerfData"] if kwargs.has_key("collectPerfData") else False
        self._useJson           = kwargs["useJson"] if kwargs.has_key("useJson") else False
        self._totalRuns         = 0
        self._totalTime         = 0
        self._totalQueryTime    = 0

    def prepareUser(self):
        """ implement this in subclasses """
        pass

    def runUser(self):
        """ implement this in subclasses """
        pass

    def stopUser(self):
        """ implement this in subclasses """
        pass

    def formatLog(self, key, value):
        """ implement this in subclasses """
        return "%s\n" % str(value)

    def run(self):
        self._prepare()
        self.prepareUser()
        cProfile.runctx('self.runWorkload()', globals(), locals(), 'cProfileUser%s.prof' % self._userId)
        self.stopUser()
        self._writeLogs()
        print "User runtime: ", self._totalTime, " User querytime: ", self._totalQueryTime

    def runWorkload(self):
        while not self._stopevent.is_set():
            tStart = time.time()
            self.runUser()
            self._totalTime += time.time() - tStart
            self._totalRuns += 1
        return


    def stop(self):
        self._stopevent.set()


    def fireQuery(self, queryString, queryArgs={"papi": "NO_PAPI"}, sessionContext=None, autocommit=False, stored_procedure=None):
        #try:
        #    if queryArgs: query = queryString % queryArgs
        #    else:         query = queryString
        #except ValueError:
        #    print queryArgs
        #    print queryString
        #    import sys
        #    sys.exit(0)
        if queryArgs: query = queryString % queryArgs
        else:         query = queryString
        data = {"query": query}
        #print "User self.context: %s\n" % self.context
        #print "Session context is: %s\n" % sessionContext
        if sessionContext:        data["session_context"] = sessionContext
        if autocommit:            data["autocommit"] = "true"
        if self._collectPerfData: data["performance"] = "true"
        self._lastQuery = data
        tStart = time.time()
        if stored_procedure:
            result = self._session.post("http://%s:%s/%s/" % (self._host, self._port, stored_procedure), data=data, timeout=100000)
        else:
            result = self._session.post("http://%s:%s/" % (self._host, self._port), data=data, timeout=100000)
        self._totalQueryTime += time.time() - tStart

        if result.status_code == 200:
            return result
        elif result.status_code == 501:
            raise RuntimeWarning(result.text)
        else:
            raise RuntimeError("Request failed --> %s" % result.text)

    def startLogging(self):
        self._logevent.set()

    def stopLogging(self):
        self._logevent.clear()

    def log(self, key, value):
        if not self._logevent.is_set():
            return
        if not self._log.has_key(key):
            self._log[key] = [value]
        else:
            self._log[key].append(value)

    def getThroughput(self):
        return self._totalRuns / self._totalTime

    def _prepare(self):
        self._session.headers = {"Content-type": "application/json", "Accept": "text/plain"}
        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)

    def _writeLogs(self):
        for k, vals in self._log.iteritems():
            logfile = open(os.path.join(self._dirOutput, "%s.log" % str(k)), "w")
            for v in vals:
                logfile.write(self.formatLog(k,v))
            logfile.close()
