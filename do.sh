#!/bin/bash

source do_parameters.sh


PS3='Choose benchmark: '
options=("Logger-Window" "Clients" "Quit")
CMD=""

select opt in "${options[@]}"
do
    case $opt in
        "Logger-Window")
            CMD="python exp_tpcc_logger_windowsize.py"
            break
            ;;
        "Clients")
            CMD="python exp_tpcc_clients.py"
            break
            ;;
        "Quit")
            exit 0
            ;;
        *) echo invalid option;;
    esac
done

echo "---------------------------"
echo "executing: " $CMD $PARAMETER
echo "---------------------------"

$CMD $PARAMETER