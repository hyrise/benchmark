#!/bin/bash


PS3='Choose benchmark: '
options=("Logger-Window" "Clients" "Quit")

CLIENTS="1000"
PORT="5431"
QUERYFILE="/mnt/amida_02/tpcc/queries_gen/20W_neworder_5M.txt"
TABLEDIR="/mnt/amida_02/tpcc/20W-tables"
DURATION="60"
VERBOSE="0"
PARAMETER="--ab=$QUERYFILE --clients=$CLIENTS --threads=20 --abCore=22 --port=$PORT --tabledir=$TABLEDIR --duration=$DURATION --verbose=$VERBOSE --stdout --stderr"

ulimit -n 4096

select opt in "${options[@]}"
do
    case $opt in
        "Logger-Window")
            python exp_tpcc_logger_windowsize.py $PARAMETER
            break
            ;;
        "Clients")
            echo $PARAMETER
            python exp_tpcc_clients.py $PARAMETER
            break
            ;;
        "Quit")
            exit 0
            ;;
        *) echo invalid option;;
    esac
done