#!/usr/bin/env python

import time

# Import all queries
from benchmark.queries import *
from benchmark.benchuser import User as User
from benchmark.layoutuser import LayoutUser as LayoutUser
import benchmark.tools as tools



class Test(object):
    """docstring for Test"""
    def __init__(self, arg):
        super(Test, self).__init__()
        self.arg = arg
        
#-----------------------------------------------------------------------------
# SCRIPT 
#-----------------------------------------------------------------------------
def script(num_users = 1, time_factor = 30, prefix="result", port="5000", thinktime=0):
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


    # lu = LayoutUser(99, "VBAP", "layouts/VBAP_ROW.tbl", prefix=prefix, port=port)
    # lu.start()
    # lu.join()

    # lu = LayoutUser(99, "VBAK", "layouts/VBAK_ROW.tbl", prefix=prefix, port=port)
    # lu.start()
    # lu.join()

    print 'starting users for %s seconds ...' % time_factor
    for user in users:
        user.start()
        # user.startLogging()
    print 'users started'

    # print "warming up 1 sec..."
    # time.sleep(1)
    
    # for user in users:
        # user.startLogging()

    print 'waiting...'
    time.sleep(time_factor)

    # print "now"
    # #####################################################
    # lu = LayoutUser(99, "VBAP", "layouts/VBAP_COL.tbl", 1, prefix=prefix, port=port)
    # lu.start()
    # lu.join()

    # lu = LayoutUser(99, "VBAK", "layouts/VBAK_COL.tbl", 1, prefix=prefix, port=port)
    # lu.start()
    # lu.join()
    # #####################################################

    # time.sleep(time_factor)
    print 'waiting finished'

    print 'stopping users...'
    for user in users:
        user.stop()

    print 'waiting for users to finish...'

    for user in users:
        print user.join()

    print 'script finished'
    for user in users:
        user.stats()

if __name__ == '__main__':
    script()
