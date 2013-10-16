import json
import os
import queries
import requests
import threading
import time

class User(threading.Thread):

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
        self._logfiles      = {}

        self._totalQueries  = 0
        self._totalTime     = 0

        self._prepare()

    def __del__(self):
        for _, f in self._logfiles.iteritems():
            f.close()

    def run(self):
        while not self._stop.isSet():
            tStart = time.time()
            q = self._queries[self._totalQueries % len(self._queries)]
            result = self._fireQuery(self._queryDict[q])
            tEnd = time.time()
            self._logResult(q, result)
            self._totalQueries += 1
            self._totalTime += tEnd - tStart

    def stop(self):
        self._stop.set()

    def startLogging(self):
        self._logging = True

    def stopLogging(self):
        self._logging = False

    def getThroughput(self):
        return self._totalQueries / self._totalTime

    def _prepare(self):
        if not os.path.isdir(self._dirOutput):
            os.makedirs(self._dirOutput)
        for q in self._queries:
            fn = os.path.join(self._dirOutput, "%s.csv" % q)
            self._logfiles[q] = open(fn, "w")
            self._logfiles[q].write("operator,duration\n")

    def _fireQuery(self, queryString):
        query = queryString % {"papi": self._papi}
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        return self._session.post("http://%s:%s/" % (self._host, self._port), data={"query": query}, headers=headers)

    def _logResult(self, query, result):
        if self._logging:
            try:
                #jsonresult = json.loads(result, encoding='latin-1')
                jsonresult = result.json()
            except ValueError:
                print "***ValueError!!!"
                return
            for operator in jsonresult["performanceData"]:
                self._logfiles[query].write("%s,%f\n" % (operator["name"], operator["endTime"] - operator["startTime"]))
