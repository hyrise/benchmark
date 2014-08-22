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


AB_LOGS = {
    'NVRAM': '../results/recovery_demo/{}/NVRAM/',
    # 'Logger': '../results/recovery_demo/{}/Logger/',
}

class MyRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.path = '/index.html'
            return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)
        if self.path == '/builds':
            self.send_response(200)
            self.send_header("Content-type:", "application/json")
            self.end_headers()
            json.dump(AB_LOGS.keys(), self.wfile)
            return

        if self.path.endswith('.json'):
            start_time = time.time()

            self.send_response(200)
            self.send_header("Content-type:", "application/json")
            self.end_headers()

            resultdir = AB_LOGS[self.path.replace('/', '').replace('.json', '')]
            fname = resultdir + "ab.log"
            if not os.path.isfile(fname):
                print "ab log not found " + fname
                return

            df = pd.read_csv(fname, sep="\t", engine='c', usecols=['seconds', 'txname'], dtype={
                'seconds': np.uint64,
                'txname': object,
            })
            df = df[df.txname == "TPCC-NewOrder"]

            if math.isnan(df.seconds.max()):
                print "no valid data found " + fname
                return

            start_timestamp = int(df.seconds.min())
            df.seconds -= start_timestamp
            df.seconds /= 1e6
            df.seconds = np.round(df.seconds, 1)
            df = df.txname.groupby(df.seconds).agg('count')

            df = df.head(len(df.index)-10)

            self.wfile.write('{"data":')                
            df.to_json(path_or_buf=self.wfile) # leave out last rows because they might be incomplete

            self.wfile.write(', "events":')                
            eventfile = resultdir + "/events.json"
            if os.path.isfile(eventfile):
                with open(eventfile) as f:
                    events = json.load(f)
                events_normalized = {}
                for k, v in events.iteritems():
                    print int(k.replace('.',''))
                    print start_timestamp
                    events_normalized[(int(k.replace('.',''))-start_timestamp)/1e6] = v
                self.wfile.write(json.dumps(events_normalized))
            else:
                self.wfile.write('[]')                
            self.wfile.write('}')                

            print str(time.time() - start_time) + " elapsed"
            return
        print "route not found " + self.path

class ThreadingServer(SocketServer.ThreadingMixIn,
                   BaseHTTPServer.HTTPServer):
    pass

Handler = MyRequestHandler
ThreadingServer.allow_reuse_address = True
server = ThreadingServer(('0.0.0.0', 8080), Handler)

server.serve_forever()