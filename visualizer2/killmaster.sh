#!/bin/bash

cat masterid.txt | xargs kill -15
curl -X POST --data "p" localhost:6666/new_master
sleep 1
rm masterid.txt