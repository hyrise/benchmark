#!/bin/bash

../ab/ab -g ab_writes.log -l 1 -v 1 -k -t 60 -n 9999999 -c 100 -r -m ~/tpcc/queries_gen/revenue_inserts_1M.txt 127.0.0.1:5000/procedure/