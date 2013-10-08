import json
import os
import random
import threading
import time
import urllib
import httplib
import collections
import numpy


from queries import *
import queries

#-----------------------------------------------------------------------------
# USER class
class User(threading.Thread):
    ''' A user is created to fire queries onto the database. 
        Users can send OLTP and OLAP queries.
        A user sends queries when it is started upto its stopping. 
        Between each query or transaction it "thinks" (waits) for a specific time.
        The User class needs the globals: OLAP_QUERY_FILES, OLTP_QUERY_FILES,
        OLTP_WEIGHTS, and OLAP_WEIGHTS, where the former two are dictionaries 
        in the form {"id": "location in filesystem"} and the latter two 
        are sequences containing tuples in the form ("id", relative weight)

        Public methods:
            User(userid, address, port, oltppercentage, thinktime)
            updateOltpPercentage(oltppercentage)
            updateThinktime(thinktime)
            stop() - let the user finish the currrent query and die afterwards
            start() - the user starts firing queries '''

    def __init__(self, userid, address, port, oltppercentage, thinktime, papi='NO_PAPI', queries=queries.ALL_QUERIES, prefix="", db="cbtr2"):
        threading.Thread.__init__(self)
        
        # self.http = urllib3.HTTPConnectionPool("127.0.0.1", port, maxsize=1)
        host = "%s:%s" % (address, port)
        self.conn = httplib.HTTPConnection(host)
        self._benchmarkQueries = queries

        self._stop = False
        self._address = address
        self._port = port
        self._oltppercentage = oltppercentage
        self._thinktime = thinktime
        self._userid = userid
        self._papi = papi

        # Counter threashold defines a warmup phase
        self._counter = 0
        self._counter_threshold = 1
        self._interval = 1

        # Core execution
        self._core = 1
        self._db = db

        # Counter fo queries
        self._queries = collections.defaultdict(int)
        self._throughput = []
        self._throughputAll = []
        self._tpTimes = []
        self._queryCount = 0
        self._totalQueryCount = 0
    
        # For result file writing
        self._startTime = time.time()
        
        self._prefix = prefix
        if not os.path.isdir(self._prefix):
            os.makedirs(self._prefix)
        
        print "Starting Benchmark, writing to " + self._prefix

        if not os.path.exists(self.basePath()):
            os.makedirs("%s" % self._prefix)

        self.initResultFiles()
        self.initDistinctValues()

        # self._accuResultFile = open( os.path.join(self.basePath(), "accumulated.txt"), "a+")
        # self._queryRowsFile = open( os.path.join(self.basePath(), "queryrows.txt"), "a+")

        self._oltpQC = {}
        for q in OLTP_QUERY_FILES:
            self._oltpQC[q] = open(OLTP_QUERY_FILES[q], "r").read()

        self._olapQC = {}
        for q in OLAP_QUERY_FILES:
            self._olapQC[q] = open(OLAP_QUERY_FILES[q], "r").read()

    def __del__(self):
        print "close conn"
        self.conn.close()

    def basePath(self):
        return self._prefix

    def resetThroughput(self):
        self._queries = collections.defaultdict(int)

    def initDistinctValues(self):

        self.distincts = {}
        print "... beginning prepare ..."
        for q in PREPARE_QUERIES:
            with open(PREPARE_QUERIES[q], "r") as f:
                query = f.read() % {"db": self._db}
            
            data = self.fireQuery(query, q)
            #print data[0]["rows"]
            
            if "rows" in data[0]:
                self.distincts[q] = data[0]["rows"]
            else:
                print "error, no rows"


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


    def initResultFiles(self):
        resultpath = os.path.join(self.basePath(), "%d" % self._userid)
        if not os.path.exists(resultpath):
            os.makedirs(resultpath)

        for queryid in (OLTP_QUERY_FILES.keys() + OLAP_QUERY_FILES.keys()):
            queryresultfilename = os.path.join(self.basePath(), str(self._userid), "%s.json" % queryid)
            with open(queryresultfilename, "a+") as f:
                f.write('endtime;exec_time\n')
                


    def stop(self):
        ''' This method should be called from outside, 
            setting the _stop attribute, which shall be taken account of by 
            the run() method.'''
        self._stop = True


    def run(self):
        """
        Starts the running of the execution loop. For each query we 
        identify how often it is run and dump the throughput in certain
        intervals.
        """
        delta = time.time()
        while(not self._stop):
            query = ''
            current_query = self._totalQueryCount % len(self._benchmarkQueries)
            element = self._benchmarkQueries[current_query]
            # Execute all queries in order
            exec_time = 0
            # if reduce(lambda i,q: True if q[0] == element or i == True else False, OLTP_WEIGHTS, False):
            exec_time = self.oltp(element)
            # else:
                # exec_time = self.olap(element)
            # self._accuResultFile.write("%f %d\n" % (time.time(), self._totalQueryCount))
            # Sleep and increment count
            self._totalQueryCount += 1
            time.sleep(self._thinktime)

    def stats(self):
        print "Query count: ", self._totalQueryCount

    def oltp(self, predefined=None):

        if predefined == None:
            queryid = self.weighted_choice(OLTP_WEIGHTS)
        else:
            queryid = predefined


        vbeln = random.choice(self.distincts["distinct_vbeln_vbak"])[0]
        matnr = random.choice(self.distincts["distinct_matnr_mara"])[0]
        addrnumber = random.choice(self.distincts["distinct_kunnr_adrc"])[0]
        kunnr = random.choice(self.distincts["distinct_kunnr_kna1"])[0]

        query = self._oltpQC[queryid] % {'papi': self._papi,
            "vbeln": vbeln,
            "matnr": matnr,
            "addrnumber": addrnumber,
            "kunnr": kunnr,
            "core": str(self._core), "db": self._db}
            
        result = self.fireQuery(query, queryid)
        
        self._queries[queryid] += 1
        # self._queryRowsFile.write("%s %d\n" % (queryid,  len(result[0]["rows"])))

        return result[1]


    def olap(self, predefined=None):

        if predefined == None:
            queryid = self.weighted_choice(OLAP_WEIGHTS)
        else:
            queryid = predefined

        # Find random matnr
        matnr = random.choice(self.distincts["distinct_matnr_mara"])

        # Now run the query
        query = self._olapQC[queryid] % {'papi': self._papi, 'matnr': matnr[0], "db": self._db, "core": self._core}

        result = self.fireQuery(query, queryid)
        self._queries[queryid] += 1
        # self._queryRowsFile.write("%s %d\n" % (queryid, len(result[0]["rows"])))
        return result[1]


    def fireQuery(self, query, queryid):
        
        # Capture the time the request started
        req_begin = time.time()      

        values = { 'query': query }
        data = urllib.urlencode(values)
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        self.conn.request("POST", "/", data, headers)
        response = self.conn.getresponse()
        response.status, response.reason
        result = response.read()
        
        # Check for warmup phase
        try:
            timer = self.logResults(result, queryid, req_begin)
            return (json.loads(result, encoding='latin-1'), timer)
        except:
            print ""
            print "Query:"
            print query
            print ""
            print "Server Response:"
            print result
            raise Exception("Log Result failed.")

        


    def logResults(self, result, queryid, req_begin):
        try:
            jsonresult = json.loads(result, encoding='latin-1')
        except ValueError:
            print '--------------------------------------------------------------------------'
            print 'Value Error in JSON Decode'
            print 'User:', self._userid
            print 'Query: ', queryid
            print '--------------------------------------------------------------------------'
            return 0


        filename = os.path.join(self.basePath(), str(self._userid), "%s.json" % queryid)
        timer = 0
        with open(filename, "a+") as f:
            for operator in jsonresult["performanceData"]:
                if operator["id"] == "respond":
                    timer = float(operator["startTime"])
                    break
            f.write( "%f;%s\n" %(req_begin,str(timer)))


        folderpath = os.path.join(self.basePath(), str(self._userid), "%s" % queryid)
        if not os.path.exists(folderpath):
            os.makedirs(folderpath)
        
        for operator in jsonresult["performanceData"]:
            operatorfilename = os.path.join(self.basePath(), str(self._userid), "%s" % queryid, "%s.json" % operator["id"])
            if not os.path.isfile(operatorfilename):
                with open(operatorfilename, "a+") as f:
                    f.write("start_time;duration\n")
            with open(operatorfilename, "a+") as f:
                w_data = "%f;%f\n" % (operator["startTime"], operator["endTime"] - operator["startTime"])
                f.write(w_data)

        return timer

    def startLogging(self):
        self._counter = self._counter_threshold + 1

    def updateOltpPercentage(self, oltppercentage):
        self._oltppercentage = oltppercentage

    def updateThinktime(self, thinktime):
        self._thinktime = thinktime

