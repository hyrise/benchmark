#!/bin/bash


PS3='Choose benchmark: '
options=("Logger-Window" "Quit")

select opt in "${options[@]}"
do
    case $opt in
        "Logger-Window")
            python exp_tpcc_logger_windowsize.py --ab=/mnt/amida_02/tpcc/queries_gen/20W_neworder_5M.txt --clients=1000 --threads=20 --abCore=22 --stdout --stderr --port=5300 --tabledir=/mnt/amida_02/tpcc/20W-tables --duration=60 --verbose=0
            break
            ;;
        "Quit")
            exit 0
            ;;
        *) echo invalid option;;
    esac
done
