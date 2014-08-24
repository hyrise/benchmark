#!/usr/bin/env python
import SimpleHTTPServer
import BaseHTTPServer
import SocketServer
import json
import os
import pandas as pd
import math
import numpy as np
from functools import partial
import os.path, time
import cherrypy
import random
from subprocess import call
import requests

#dict of tuples of result file and event file.
#event file is None if no events should be displayed
AB_LOGS = {
    # 'NVRAM': ('../results/recovery_demo/{}/NVRAM/ab.log', None),
    # 'Logger': ('../results/recovery_demo/{}/Logger/ab.log', None),
    'master_writes': ('./ab_writes.log', "./events_master.json"),
    'reads': ('./ab_reads.log', "./events_reads.json")
}

def readlog(logfilename):
    try:
        with open(logfilename) as f:
            content = f.readlines()
            return content
    except:
        return ""

def create_json_from_ab(logfilename):
    start_time = time.time()
    jsonstring = ""
    fname = AB_LOGS[logfilename][0]

    if not os.path.isfile(fname):
        print "ab log not found " + fname
        return

    df = pd.read_csv(fname, sep="\t", engine='c', usecols=['seconds', 'status'], dtype={
        'seconds': np.uint64,
        'status': np.uint64,
    })

    df = df[df.status == 200]
    df = df[df.seconds > 1008914214395976] # filter out the crap

    if math.isnan(df.seconds.max()):
        print "no valid data found " + fname
        return

    start_timestamp = int(df.seconds.min())
    df.seconds -= start_timestamp

    df.seconds /= 1e6
    df.seconds = np.round(df.seconds, 1)
    
    df = df.status.groupby(df.seconds).agg('count')

    df = df.head(len(df.index)-10)
    df = df * 10 # normalize to seconds

    jsonstring += '{"data":'
    jsonstring += df.to_json()
    jsonstring += ', "events":'

    eventfile = AB_LOGS[logfilename][1]
    if os.path.isfile(eventfile):
        with open(eventfile) as f:
            events = json.load(f)
        events_normalized = {}
        for k, v in events.iteritems():
            print int(k.replace('.',''))
            print start_timestamp
            events_normalized[(int(k.replace('.',''))-start_timestamp)/1e6] = v
        jsonstring += json.dumps(events_normalized)
    else:
        jsonstring += '[]'
    jsonstring += '}'               

    print str(time.time() - start_time) + " elapsed"
    return jsonstring

class MyServerHandler(object):
    @cherrypy.expose
    def index(self):
        with open("index.html") as f:
            content = f.readlines()
            return content
    
    @cherrypy.expose
    def log1(self):
        return readlog("log1.txt")
        
    @cherrypy.expose
    def log2(self):
        return readlog("log2.txt")
    
    @cherrypy.expose
    def log3(self):
        return readlog("log3.txt")
    
    @cherrypy.expose
    def log4(self):
        return readlog("log4.txt")

    @cherrypy.expose
    def delay(self):
        payload = {'query': '{"operators": {"0": {"type": "ClusterMetaData"} } }'}
        r = requests.post("http://localhost:5000/query/", data=payload)
        return r.text

    @cherrypy.expose
    def master_writes(self):
        return create_json_from_ab("master_writes")

    @cherrypy.expose
    def reads(self):
        return create_json_from_ab("reads")

    @cherrypy.expose
    def NVRAM(self):
        return create_json_from_ab("NVRAM")

    @cherrypy.expose
    def Logger(self):
        return create_json_from_ab("Logger")

    @cherrypy.expose
    def builds(self):
        return json.dumps(AB_LOGS.keys())

    @cherrypy.expose
    def startserver(self):
        call(["bash", "start.sh"])
        return ""
  
if __name__ == '__main__':

    random.seed()
    cherrypy.config.update({
        'server.socket_host': '192.168.30.177',
        'server.socket_port': 8080,
    })
    cherrypy.quickstart(MyServerHandler())
