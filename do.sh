#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PWD_DIR=`pwd`

source $SCRIPT_DIR/do_parameters.sh

cd $SCRIPT_DIR


BENCHMARK="unknown"
options=("Logger-Window" "Clients" "CheckpointInterval" "Recovery" "Single-Run" "Profiler" "Quit")

if [ "$#" -eq 1 ]; then

    BENCHMARK=$1
    if echo "${options[@]}" | fgrep --word-regexp "$BENCHMARK"; then
        t=1
    else
        echo "ERROR: Invalid benchmark specified - $BENCHMARK"
        echo "Please choose from: ${options[@]}"
        exit 1
    fi
else

    PS3='Choose benchmark: '
    CMD=""
    PLT=""

    ulimit -n 4096

    select opt in "${options[@]}";
    do
        BENCHMARK=$opt
        break
    done
fi

case $BENCHMARK in
    "Logger-Window")
        CMD="python exp_tpcc_logger_windowsize.py"
        PLT="python plt_tpcc_logger_windowsize.py"
        ;;
    "Clients")
        CMD="python exp_tpcc_clients.py"
        PLT="python plt_tpcc.py"
        ;;
    "CheckpointInterval")
        CMD="python exp_tpcc_checkpoint_throughput.py"
        PLT="python plt_tpcc_checkpoint_throughput.py"
        ;;
    "Recovery")
        CMD="python exp_tpcc_recovery.py"
        PLT="python plt_tpcc_recovery.py"
        ;;
    "Single-Run")
        CMD="python exp_tpcc_single_run.py"
        ;;
    "Profiler")
        CMD="python exp_tpcc_profiler_run.py"
        ;;
    "Quit")
        exit 0
        ;;
    *) echo invalid option;;
esac

echo "Chosen benachmark: $BENCHMARK"
echo "---------------------------"
echo "executing experiment: " $CMD $PARAMETER
echo "---------------------------"

$CMD $PARAMETER

# read -p "With plotting? [y/n] " answer
# if [[ $answer = y ]] ; then
#     echo "---------------------------"
#     echo "executing plot: " $PLT
#     echo "---------------------------"
#     $PLT
# fi

cd $PWD_DIR









