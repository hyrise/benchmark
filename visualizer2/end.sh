#!/bin/bash

killall -9 ab
killall -9 hyrise-server_release
killall -9 hyrise-server_debug
killall -15 dispatcher
sleep 1
killall -9 dispatcher
