#!/bin/bash


CLIENTS="1000"
PORT="5222"
QUERYFILE="/mnt/amida_02/tpcc/queries_wid/20W_neworder_10M.txt"
TABLEDIR="/mnt/amida_02/tpcc/20W-tables"
DURATION="60"
VERBOSE="0"
PARAMETER="--ab=$QUERYFILE --clients=$CLIENTS --threads=20 --abCore=22 --port=$PORT --tabledir=$TABLEDIR --duration=$DURATION --verbose=$VERBOSE --stdout --stderr"
