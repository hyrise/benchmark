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

BINARY=./build/hyrise-server_release
DISPATCHER=~/dispatcher/dispatcher
DISPATCHPORT=6666
SETUPQUERY=$cwd/insert_test.json


echo "Starting Hyrise cluster with 1 master and 3 replica..."
echo "Binary file is $BINARY"
pwd

# start master
echo "Starting master..."
($BINARY --persistencyDirectory ~ -n 0 --threads 2 --corecount 2 --coreoffset 4 > ~/benchmark/visualizer/log1.txt) &
master_pid=$!
echo $master_pid > $cwd/masterid.txt
sleep 1
checkprocess $master_pid

# create table on master
echo "Executing $SETUPQUERY @ Master..."
curl -X POST --data-urlencode "query@$SETUPQUERY" http://localhost:5000/jsonQuery

# start replica 1-3
echo "Starting replica 1..."
($BINARY --port 5001 --persistencyDirectory ~ -n 1 --threads 2 --corecount 2 --coreoffset 0 > ~/benchmark/visualizer/log2.txt) &
r1_pid=$!
sleep 1
#checkprocess $r1_pid

echo "Executing $SETUPQUERY @ R1..."
curl -X POST --data-urlencode "query@$SETUPQUERY" http://localhost:5001/jsonQuery

#echo "Starting replica 2..."
#($BINARY --port 5002 --persistencyDirectory ~ -n 2 --threads 1 --corecount 1 --coreoffset 4 > ~/benchmark/visualizer/log3.txt)&
#r2_pid=$!
#sleep 1
#checkprocess $r2_pid

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


echo "starting dispatcher on port $DISPATCHPORT..."
($DISPATCHER $DISPATCHPORT)&
d_pid=$!
sleep 1
checkprocess $d_pid


echo "All processes started."


