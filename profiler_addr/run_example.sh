#!/bin/bash
die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required (output directory), $# provided"

OUT_DIR=$1

make -j 2

sizes=( 1 1024 4096 32768 1048576 4194304 8388608)

for i in "${sizes[@]}"
do
  CMD="taskset -c 0 ./bin/example -s $i -o $OUT_DIR"
  echo "-------------"
  echo "Executing: $CMD"
  echo "-------------"
  $CMD
done

for file in $OUT_DIR/*_loads.perf.csv
do
  CMD="python plt/plt_hist.py $file"
  echo "-------------"
  echo "Plotting: $CMD"
  echo "-------------"
  $CMD
done