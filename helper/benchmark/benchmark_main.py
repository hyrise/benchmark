#!/usr/bin/env python

import time

# Import all queries
from benchmark.queries import *
from benchmark.benchuser import User as User
from benchmark.layoutuser import LayoutUser as LayoutUser
import benchmark.tools as tools
import sys


class Test(object):
    """docstring for Test"""
    def __init__(self, arg):
        super(Test, self).__init__()
        self.arg = arg
        
#-----------------------------------------------------------------------------
# SCRIPT 
#-----------------------------------------------------------------------------
def script(num_users = 1, time_factor = 30, prefix="result", port="5000", thinktime=0, warmup=0):
    ''' example of how to use the User class to do benchmarking 
    '''

    server = "127.0.0.1"
    # time_factor = 30
    # prefix = tools.getlastprefix("./tmp/bencmark")
    # queries = ("q6a", "q6b", "q7", "q8", "q10", "q11", "q12") # "q7idx", "q8idx"
    queries = ["q13insert"]
    port = int(port)
    users = []
    for i in range(num_users):
         users.append(User(i, server, port, 100, thinktime, "NO_PAPI", queries, prefix=prefix, db="cbtr"))

    print 'Starting users for %s seconds ...' % time_factor
    for user in users:
        user.start()
        
    print "Users started. Warming up %s sec..." % warmup
    if warmup > 0:
        time.sleep(warmup)
    
    print "Start logging..."
    for user in users:
        user.startLogging()

    print 'Waiting for %s seconds...' % time_factor
    time.sleep(time_factor)

    print "End logging..."
    for user in users:
        user.stopLogging()

    print 'Stopping users...'
    for user in users:
        user.stop()

    print 'Waiting for users to finish...'

    for user in users:
        user.join()

    sys.stdout.write('Writing logfiles')

    for user in users:
        user.write_log()
        sys.stdout.write('.')
    sys.stdout.write("/n")

    print 'Script finished'
    for user in users:
        user.stats()

if __name__ == '__main__':
    script()
