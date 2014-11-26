#!/bin/bash


function checkprocess {
 if ps -p $1 > /dev/null
 then
  echo "$1 is running"
 else
  exit 1
 fi
}

rm ab_reads.log
rm ab_writes.log

cwd=$(pwd)
cd ~/hyrise

BINARY=~/hyrise/build/hyrise-server_debug
DISPATCHER=~/dispatcher/dispatcher
DISPATCHPORT=6666
SETUPQUERY=$cwd/insert_test.json
PERSISTENCYDIR=~



echo "Starting Hyrise cluster with 1 master and 3 replica..."
echo "Binary file is $BINARY"
pwd


#wait
#echo "waiting for dispatcher port..."
#n=1
#while [ $n -gt 0 ] 
#do
# n=$(netstat | grep $DISPATCHPORT | wc -l)
# echo $n
# sleep 1
#done

# start master
echo "Starting master..."
($BINARY -p 5000 -n 0 --corecount 10 --coreoffset 0 --memorynode=0 --nodes=0 --commitWindow 1 --persistencyDirectory $PERSISTENCYDIR > ~/benchmarkNew/benchmark/visualizer2/log1.txt) &
master_pid=$!
echo $cwd
echo $master_pid > $cwd/masterid.txt
sleep 1
checkprocess $master_pid

# create table on master
echo "Executing $SETUPQUERY @ Master..."
curl -X POST --data-urlencode "query@$SETUPQUERY" http://localhost:5000/jsonQuery

sleep 1

# # start replica 1-3
# echo "Starting replica 1..."
# ($BINARY -p 5001 -n 1 --corecount 8 --coreoffset 0 --memorynode=1 --nodes=1 --commitWindow 1 --persistencyDirectory $PERSISTENCYDIR > ~/benchmarkNew/benchmark/visualizer2/log2.txt) &
# r1_pid=$!
# sleep 1
# #checkprocess $r1_pid

# echo "Executing $SETUPQUERY @ R1..."
# curl -X POST --data-urlencode "query@$SETUPQUERY" http://localhost:5001/jsonQuery

# echo "Starting replica 2..."
# ($BINARY -p 5002 -n 2 --corecount 8 --coreoffset 0 --memorynode=2 --nodes=2 --commitWindow 1 --persistencyDirectory $PERSISTENCYDIR > ~/benchmarkNew/benchmark/visualizer2/log3.txt)&
# r2_pid=$!
# sleep 1
# #checkprocess $r2_pid

# echo "Executing $SETUPQUERY @ R2..."
# curl -X POST --data-urlencode "query@$SETUPQUERY" http://localhost:5002/jsonQuery

# echo "Starting replica 3..."
# ($BINARY -p 5003 -n 3 --corecount 8 --coreoffset 0 --memorynode=3 --nodes=3 --commitWindow 1 --persistencyDirectory $PERSISTENCYDIR > ~/benchmarkNew/benchmark/visualizer2/log4.txt)&
# r3_pid=$!
# sleep 1
# #checkprocess $r2_pid

# echo "Executing $SETUPQUERY @ R3..."
# curl -X POST --data-urlencode "query@$SETUPQUERY" http://localhost:5003/jsonQuery

#echo "Starting replica 3..."
#($BINARY --port 5003 --persistencyDirectory ~ -n 3 --threads 1 --corecount 1 --coreoffset 6 > ~/benchmark/visualizer/log4.txt)&
#r3_pid=$!
#sleep 1
#checkprocess $r3_pid

# create table on replica 1-3

#echo "Executing $SETUPQUERY @ R2..."
#curl -X POST --data-urlencode "query@$SETUPQUERY" http://localhost:5002/jsonQuery

#echo "Executing $SETUPQUERY @ R3..."
#curl -X POST --data-urlencode "query@$SETUPQUERY" http://localhost:5003/jsonQuery


#echo "starting dispatcher on port $DISPATCHPORT..."
#(taskset -c 0,1 $DISPATCHER $DISPATCHPORT > ~/benchmarkNew/benchmark/visualizer2/log_disp.txt)&
#d_pid=$!
#sleep 1
#checkprocess $d_pid


echo "All processes started."


