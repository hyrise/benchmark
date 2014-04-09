#!/bin/bash

source do_parameters.sh


PS3='Choose benchmark: '
options=("Logger-Window" "Clients" "CheckpointInterval" "Recovery" "Quit")
CMD=""
PLT=""

ulimit -n 4096

select opt in "${options[@]}"
do
    case $opt in
        "Logger-Window")
            CMD="python exp_tpcc_logger_windowsize.py"
            PLT="python plt_tpcc_logger_windowsize.py"
            break
            ;;
        "Clients")
            CMD="python exp_tpcc_clients.py"
            PLT="python plt_tpcc.py"
            break
            ;;
        "CheckpointInterval")
            CMD="python exp_tpcc_checkpoint_throughput.py"
            PLT="python plt_tpcc_checkpoint_throughput.py"
            break
            ;;
        "Recovery")
            CMD="python exp_tpcc_recovery.py"
            PLT="python plt_tpcc_recovery.py"
            break
            ;;
        "Quit")
            exit 0
            ;;
        *) echo invalid option;;
    esac
done

read -p "With plotting? [y/n] " answer

echo "---------------------------"
echo "executing experiment: " $CMD $PARAMETER
echo "---------------------------"

$CMD $PARAMETER

if [[ $answer = y ]] ; then
    echo "---------------------------"
    echo "executing plot: " $PLT
    echo "---------------------------"
    $PLT
fi

