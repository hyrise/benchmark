#!/bin/bash

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 2 ] || die "2 argument required (num write users, num read users), $# provided"

# python delay.py &
# ../ab/ab -l 79 -v 0 -k -t 10 -n 9999999 -c $2 -r -p ./postdata.txt 127.0.0.1:6666/procedureRevenueSelect/ &
../ab/ab -l 77 -v 0 -k -t 30 -n 9999999 -c $1 -r -p ./postdata.txt 127.0.0.1:6666/procedureRevenueInsert/ &