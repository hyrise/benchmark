#!/bin/bash

../ab/ab -G ab_reads.log -l 8 -v 1 -k -t 60 -n 9999999 -c 50 -r -m /home/David.Schwalb/tpcc/queries_gen/noop_1M.txt 127.0.0.1:6666/jsonQuery/