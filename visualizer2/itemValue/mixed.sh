taskset -c 9,10,11,12,13,14,15,16 ab -k -l -n 500 -c $1 -T "application/x-www-form-urlencoded" -r -p "writeRequestShort.txt" 127.0.0.1:6666/procedureRevenueInsert/ &
sleep 0.5
taskset -c 17,18,19,20,21,22,23,24 ab -k -l -n 2000 -c $2 -T "application/x-www-form-urlencoded" -r -p "readRequest.txt" 127.0.0.1:6666/procedureRevenueSelect/