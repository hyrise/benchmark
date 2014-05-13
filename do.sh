#!/bin/bash




SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PWD_DIR=`pwd`

source $SCRIPT_DIR/do_parameters.sh

cd $SCRIPT_DIR

PS3='Choose benchmark: '
options=("Logger-Window" "Clients" "CheckpointInterval" "Recovery" "Single-Run" "Quit")
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
        "Single-Run")
            CMD="python exp_tpcc_single_run.py"
            break
            ;;
        "Quit")
            exit 0
            ;;
        *) echo invalid option;;
    esac
done

# read -p "With plotting? [y/n] " answer

echo "---------------------------"
echo "executing experiment: " $CMD $PARAMETER
echo "---------------------------"

$CMD $PARAMETER

# if [[ $answer = y ]] ; then
#     echo "---------------------------"
#     echo "executing plot: " $PLT
#     echo "---------------------------"
#     $PLT
# fi

cd $PWD_DIR