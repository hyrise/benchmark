import httplib
import logging
import os
import requests
import shutil
import subprocess
import sys
import multiprocessing
import time

from benchmark import Benchmark
from user import User

from queries import *
import queries

# disable py-tpcc internal logging
logging.getLogger("requests").setLevel(logging.WARNING)

class MixedWLUser(User):

    def __init__(self, userId, host, port, dirOutput, queryDict, **kwargs):
        User.__init__(self, userId, host, port, dirOutput, queryDict, **kwargs)

#        self.scaleParameters = kwargs["scaleParameters"]
        self._benchmarkQueries = kwargs["queries"] if kwargs.has_key("queries") else queries.QUERIES_ALL
        
        self._core = kwargs["core"] if kwargs.has_key("core") else 1
        self._prio = kwargs["prio"] if kwargs.has_key("prio") else 2
        # default assumes hyperthreading count of 2
        self._instances = kwargs["instances"] if kwargs.has_key("instances") else multiprocessing.cpu_count()/2
        self._microsecs = kwargs["microsecs"] if kwargs.has_key("microsecs") else 1000
        self._db = kwargs["db"] if kwargs.has_key("db") else "cbtr"

        self._oltpQC = {}
        for q in OLTP_QUERY_FILES:
            self._oltpQC[q] = open(OLTP_QUERY_FILES[q], "r").read()

        self._olapQC = {}
        for q in OLAP_QUERY_FILES:
            self._olapQC[q] = open(OLAP_QUERY_FILES[q], "r").read()
        self.perf = {}

    def prepareUser(self):
        self.userStartTime = time.time()
        self.initDistinctValues()

    def runUser(self):
        """ main user activity """
        # choose query from _queries
        self.perf = {}
        query = ''
        current_query = self._totalRuns % len(self._benchmarkQueries)
        tStart = time.time()
        element = self._benchmarkQueries[current_query]

        # Execute all queries in order                                                                                                                                                           
        if reduce(lambda i,q: True if q[0] == element or i == True else False, OLTP_WEIGHTS, False):
            result = self.oltp(element)
        else:
            result = self.olap(element)

        #result = self.fireQuery(querystr, paramlist, sessionContext=self.context, autocommit=commit, stored_procedure=stored_procedure).json()
        self.addPerfData(result.get("performanceData", None))
        tEnd = time.time()
        self.log("transactions", [0, tEnd-tStart, tStart-self.userStartTime, self.perf])

    def oltp(self, predefined=None):
        if predefined == None:
            queryid = self.weighted_choice(OLTP_WEIGHTS)
        else:
            queryid = predefined

      #  vbeln = random.choice(self.distincts["distinct_vbeln_vbak"])[0]                                                                                                                                    
      #  matnr = random.choice(self.distincts["distinct_matnr_mara"])[0]                                                                                                                                    
      #  addrnumber = random.choice(self.distincts["distinct_kunnr_adrc"])[0]                                                                                                                               
      #  kunnr = random.choice(self.distincts["distinct_kunnr_kna1"])[0]                                                                                                                                    

        query = self._oltpQC[queryid] % {'papi': self._papi,
          #  "vbeln": vbeln,                                                                                                                                                                                
          #  "matnr": matnr,                                                                                                                                                                                
          # "addrnumber": addrnumber,                                                                                                                                                                      
          #  "kunnr": kunnr,                                                                                                                                                                                
            "core": str(self._core), "db": self._db, "sessionId": str(self._userId), "priority": str(self._prio), "microsecs": str(self._microsecs)}

        result = self.fireQuery(query).json()
        self._queries[queryid] += 1
        #self._queryRowsFile.write("%s %d\n" % (queryid,  len(result[0]["rows"])))
        #return result[1]
        return result

    def olap(self, predefined=None):
        if predefined == None:
            queryid = self.weighted_choice(OLAP_WEIGHTS)
        else:
            queryid = predefined

        # Find random matnr                                                                                                                                                                                 
        # matnr = random.choice(self.distincts["distinct_matnr_mara"])                                                                                                                                       

        # Now run the query                                                                                                                                                                                 
#        query = self._olapQC[queryid] % {'papi': self._papi, 'matnr': matnr[0], "db": self._db, "core": self._core, "instances": self._instances}                                                          
        query = self._olapQC[queryid] % {'papi': self._papi, "db": self._db, "instances": self._instances, "sessionId": str(self._userId), "priority": str(self._prio), "microsecs": str(self._microsecs)}

        result = self.fireQuery(query).json()
        return result

        #self._queries[queryid] += 1
        #self._queryRowsFile.write("%s %d\n" % (queryid, len(result[0]["rows"])))                                                                                                                           
        #return result[1]


    def stopUser(self):
        """ executed once after stop request was sent to user """
        pass

    def formatLog(self, key, value):
        logStr = "%s;%f;%f" % (value[0], value[1], value[2])
        for op, opData in value[3].iteritems():
            logStr += ";%s,%i,%f" % (op, opData["n"], opData["t"])
        logStr += "\n"
        return logStr

    def addPerfData(self, perf):
        if perf:
            for op in perf:
                self.perf.setdefault(op["name"], {"n": 0, "t": 0.0})
                self.perf[op["name"]]["n"] += 1
                self.perf[op["name"]]["t"] += op["endTime"] - op["startTime"]


    def stats(self):
        """ Print some execution statistics about the User """

        print "Overall tp/%ds (mean): %f" % (self._interval, numpy.array(self._throughputAll).mean())
        print "Overall tp/%ds (median): %f" % (self._interval, numpy.median(numpy.array(self._throughputAll)))

        # Mean over all queries                                                                                                                                                                             
        all_mean = numpy.array(self._throughputAll).mean()
        all_median = numpy.median(numpy.array(self._throughputAll))

        # Build the ordered list of all queryids                                                                                                                                                            
        query_ids = map(lambda k: k[0], OLTP_WEIGHTS) + map(lambda k: k[0], OLAP_WEIGHTS)

                                                                                                                                                       
    def initDistinctValues(self):

        self.distincts = {}
        print "... beginning prepare ..."
        for q in PREPARE_QUERIES_USER:
            with open(PREPARE_QUERIES_USER[q], "r") as f:
                query = f.read() % {"db": self._db}
            data = self.fireQuery(query)
            print data.json()                                                                                                                                                                          
            if "rows" in data[0]:
                self.distincts[q] = data[0]["rows"]

        print "... finished prepare ..."

    @staticmethod
    def weighted_choice(choices_and_weights):
        ''' method used for weighted choice of queries according to its input '''
        totals = []
        running_total = 0

        for c, w in choices_and_weights:
            running_total += w
            totals.append(running_total)

        rnd = random.random() * running_total

        for i, total in enumerate(totals):
            if rnd < total:
                return choices_and_weights[i][0]


class MixedWLBenchmark(Benchmark):

    def __init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs):
        Benchmark.__init__(self, benchmarkGroupId, benchmarkRunId, buildSettings, **kwargs)

        #self._dirHyriseDB = os.path.join(os.getcwd(), "hyrise")
        os.environ['HYRISE_DB_PATH'] = self._dirHyriseDB

        self.setUserClass(MixedWLUser)
        self._queryDict = self.loadQueryDict()

    def loadQueryDict(self):
        queryDict = {}
        # read PREPARE queries
        for q in PREPARE_QUERIES_USER:
            queryDict[q] = open(PREPARE_QUERIES_USER[q], "r").read()
        for q in PREPARE_QUERIES_SERVER:
            queryDict[q] = open(PREPARE_QUERIES_SERVER[q], "r").read()
        # read OLTP queries
        for q in OLTP_QUERY_FILES:
            queryDict[q] = open(OLTP_QUERY_FILES[q], "r").read()
        # read OLAP queries
        for q in OLAP_QUERY_FILES:
            queryDict[q] = open(OLAP_QUERY_FILES[q], "r").read()
        return queryDict