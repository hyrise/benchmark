import json
import os
import queries
import random
import requests
import threading
import time

class User(threading.Thread):
    """
    Benchmark User base class
    """

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        threading.Thread.__init__(self)

        self._userId        = userId
        self._host          = host
        self._port          = port
        self._session       = requests.Session()
        self._dirOutput     = os.path.join(dirOutput, str(userId))
        self._queryDict     = queryDict
        self._queries       = kwargs["queries"] if kwargs.has_key("queries") else queries.QUERIES_ALL
        self._thinkTime     = kwargs["thinkTime"] if kwargs.has_key("thinkTime") else 0
        self._papi          = kwargs["papi"] if kwargs.has_key("papi") else "NO_PAPI"
        self._stop          = threading.Event()
        self._logging       = False
        self._log           = {}

        self._totalRuns     = 0
        self._totalTime     = 0

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
        while not self._stop.isSet():
            tStart = time.time()
            self.runUser()
            self._totalTime += time.time() - tStart
            self._totalRuns += 1
        self.stopUser()
        self._writeLogs()

    def stop(self):
        self._stop.set()

    def fireQuery(self, queryString, queryArgs={}, sessionContext=None, autocommit=False):
        query = queryString % queryArgs
        data = {"query": query}
        if sessionContext: data["sessionContext"] = sessionContext
        if autocommit: data["autocommit"] = "true"
        return self._session.post("http://%s:%s/" % (self._host, self._port), data=data)

    def startLogging(self):
        self._logging = True

    def stopLogging(self):
        self._logging = False

    def log(self, key, value):
        if not self._logging:
            return
        if not self._log.has_key(key):
            self._log[key] = [value]
        else:
            self._log[key].append(value)

    def getThroughput(self):
        return self._totalRuns / self._totalTime

    def _prepare(self):
        self._session.headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)

    def _writeLogs(self):
        for k, vals in self._log.iteritems():
            logfile = open(os.path.join(self._dirOutput, "%s.log" % str(k)), "w")
            for v in vals:
                logfile.write(self.formatLog(k,v))
            logfile.close()
