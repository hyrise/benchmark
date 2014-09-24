#!/bin/bash

#../ab/ab -G ab_reads.log -l 6 -v 1 -k -t 180 -n 9999999 -c 50 -r -m revenue_reads_8M.txt 192.168.30.112:6666/jsonQuery
../ab/ab -G ab_reads.log -l 6 -v 1 -k -t 180 -n 9999999 -c 150 -r -m /home/David.Schwalb/tpcc/queries_gen/revenue_select_storedproc_5M.txt 192.168.30.112:5001/procedureRevenueSelect/