#!/bin/bash

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 2 ] || die "2 argument required (num write users, num read users), $# provided"

../ab/ab -l 16 -v 0 -k -t 999 -n 9999999 -c $2 -r -p ./postdata.txt 192.168.30.112:6666/procedureRevenueSelect/ &
../ab/ab -l 18 -v 0 -k -t 999 -n 9999999 -c $1 -r -p ./postdata.txt 127.0.0.1:6666/procedureRevenueInsert/ &