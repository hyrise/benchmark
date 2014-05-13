#!/bin/bash
die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 arguments required (result foldername), $# provided"

FOLDER=$1
# sudo chown $USER ./profiler/$FOLDER/perf.data

echo "Parsing data: /mnt/amida_02/markus_pmfs/tools/perf/perf mem rep --field-separator \";\" -i ./profiler/$FOLDER/perf.data > ./profiler/$FOLDER/perf.txt"
/usr/lib/linux-tools/3.11.0-12-generic/perf mem rep --field-separator ";" -i ./profiler/$FOLDER/perf.data > ./profiler/$FOLDER/perf.txt

echo "Creating plots: python ./profiler/mem_plt.py ./profiler/$FOLDER"
python ./profiler/mem_plt.py ./profiler/$FOLDER

