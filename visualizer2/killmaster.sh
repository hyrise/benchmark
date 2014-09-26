#!/bin/bash

cat masterid.txt | xargs kill -15
#curl -X POST --data "x=1" http://127.0.0.1:6666/new_master
sleep 1
rm masterid.txt