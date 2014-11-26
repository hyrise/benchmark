#!/bin/bash

#../ab/ab -G ab_writes.log -l 1 -v 1 -k -t 60 -n 9999999 -c 50 -r -m /home/David.Schwalb/tpcc/queries_gen/revenue_inserts_1M.txt 127.0.0.1:6667/jsonQuery/
#../ab/ab -G ab_writes.log -l 4 -v 1 -k -t 180 -n 9999999 -c 150 -r -m revenue_inserts_6M.txt 192.168.30.112:6666/jsonQuery/
../ab/ab -G ab_writes.log -l 40 -v 0 -k -t 60 -n 9999999 -c 50 -r -m /home/David.Schwalb/tpcc/queries_gen/revenue_insert_storedproc_5M.txt 127.0.0.1:6666/procedureRevenueInsert/ &
