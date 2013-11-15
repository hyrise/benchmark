import json
import requests
import sys

from datetime import datetime

class HyriseConnection(object):

    def __init__(self, host="localhost", port=5000, querylog=None):
        self._host = host
        self._port = port
        self._context = None
        self._url = "http://%s:%s/" % (host, port)
        self.header = None
        self._result = None
        self._log = "{}_{}.log".format(querylog, datetime.now().strftime('%m_%d_%H_%M')) if querylog else None
        self.counter = 0
        if self._log:
            with open(self._log,'w') as logfile:
                logfile.write('[')

    def query(self, querystr, paramlist=None, commit=False):
        q = querystr
        if paramlist:
            assert isinstance(paramlist, dict)
            for k,v in paramlist.iteritems():
                if v == True:
                    v = 1;
                elif v == False:
                    v = 0;
            q = q % paramlist

        r = self.query_raw(query=q, context=self._context, commit=commit)
        json_response = json.loads(r)

        if json_response.has_key('error'):
            print "#######QueryError#########"
            print r
            sys.exit(-1)

        if self._log:
            self.writeLogentry(json_response)

        self._context = json_response.get("session_context", None)
        self._result = json_response.get('rows', None)
        self.header = json_response.get('header', None)

        sys.stdout.write('.')
        sys.stdout.flush()

    def writeLogentry(self, json_response):
        data = {'id':self.counter, 'query':q}

        if json_response.has_key('error'):
            data['error'] = json_response['error']
        else:
            data['time'] = json_response['performanceData'][-1]['endTime']
            data['performancedata']= json_response['performanceData']

        with open(self._log,'a') as logfile:
            logfile.write(json.dumps(data) + '\n')

    def query_raw(self, query, context, commit=False):
        payload = { "query" : query }
        if context:
            payload["session_context"] = context
        if commit:
            payload["autocommit"] = "true"
        result = requests.post(self._url + "query/",
                               data = payload, timeout=500)
        return result.text

    def commit(self):
        if not self._context:
            raise Exception("Should not commit without running context")
        r = self.query("""{"operators": {"cm": {"type": "Commit"}}}""")
        self._context = None
        return r

    def rollback(self):
        if not self._context:
            raise Exception("Should not rollback without running context")
        r = self.query("""{"operators": {"rb": {"type": "Rollback"}}}""")
        self._context = None
        return r

    def runningTransactions(self):
        return json.loads(requests.get(self._server_base_url + "status/tx").text)

    def fetchone(self, column=None):
        if self._result:
            r = self._result.pop()
            if column:
                return r[self.header.index(column)]
            return r
        return None

    def fetchone_as_dict(self):
        if self._result:
            return dict(zip(self.header, self._result.pop()))
        return None

    def fetchall(self):
        if self._result:
            temp = self._result
            self._result = None
            return temp
        return None

    def fetchall_as_dict(self):
        if self._result:
            r = [dict(zip(self.header, cur_res)) for cur_res in self._result]
            return r
            self._result = None
        return None
