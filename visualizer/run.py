#!/usr/bin/env python
import SimpleHTTPServer
import SocketServer
import json
import os
import pandas as pd
import math
import numpy as np
from functools import partial
import os.path, time


AB_LOGS = {'NVRAM': '../results/recovery_demo/{}/NVRAM/ab.log'}

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

            fname = AB_LOGS[self.path.replace('/', '').replace('.json', '')]
            if not os.path.isfile(fname):
                print "ab log not found " + fname
                return

            df = pd.read_csv(fname, sep="\t")
            df = df[df.txname == "TPCC-NewOrder"]

            if math.isnan(df.seconds.max()):
                print "no valid data found " + fname
                return

            df.seconds -= int(df.seconds.min())
            df.seconds /= 1e6
            df.seconds = np.round(df.seconds, 1)
            df = df.status.groupby(df.seconds).agg('count')

            df = df.head(len(df.index)-10)
            df.to_json(path_or_buf=self.wfile) # leave out last rows because they might be incomplete

            print str(time.time() - start_time) + " elapsed"
            return
        print "route not found " + self.path

Handler = MyRequestHandler
SocketServer.TCPServer.allow_reuse_address = True
server = SocketServer.TCPServer(('0.0.0.0', 8080), Handler)

server.serve_forever()