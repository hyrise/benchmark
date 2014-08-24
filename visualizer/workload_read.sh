#!/bin/bash

../ab/ab -G ab_reads.log -l 1 -v 1 -k -t 60 -n 9999999 -c 1 -r -m /home/David.Schwalb/tpcc/queries_gen/revenue_reads_1M.txt 127.0.0.1:6667/jsonQuery/