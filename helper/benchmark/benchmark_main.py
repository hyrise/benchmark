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
def script(num_users = 1, time_factor = 30):
    ''' example of how to use the User class to do benchmarking 
    '''

    server = "http://127.0.0.1"
    port = 5000
    # time_factor = 30
    prefix = tools.getlastprefix("queued_1k_idx")
    # queries = ("q6a", "q6b", "q7", "q8", "q10", "q11", "q12") # "q7idx", "q8idx"
    queries = ["q13insert"]

    users = []
    for i in range(num_users):
         users.append(User(i, server, port, 100, 0, "NO_PAPI", queries, prefix=prefix, db="cbtr"))


    # lu = LayoutUser(99, "VBAP", "layouts/VBAP_ROW.tbl", prefix=prefix, port=port)
    # lu.start()
    # lu.join()

    # lu = LayoutUser(99, "VBAK", "layouts/VBAK_ROW.tbl", prefix=prefix, port=port)
    # lu.start()
    # lu.join()

    print 'starting users...'
    for user in users:
        user.start()
    print 'users started'

    time.sleep(min(15, time_factor))
    for user in users:
        user.startLogging()

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
        user.join()

    print 'script finished'
    for user in users:
        user.stats()

if __name__ == '__main__':
    script()
