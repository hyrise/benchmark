#!/bin/bash
die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 2 ] || die "2 arguments required (process id and hyrise path), $# provided"

HYRISE_PATH=$1
PROCESS_ID=$2
VTUNE_COMMAND="/opt/intel/vtune_amplifier_xe_2013/bin64/amplxe-cl "
VTUNE_PARAMETER="-target-pid $PROCESS_ID\
    -follow-child -mrte-mode=auto -target-duration-type=short -no-allow-multiple-runs \
    -no-analyze-system -data-limit=500 -slow-frames-threshold=40 -fast-frames-threshold=100 \
    --search-dir all:rp=$HYRISE_PATH/build \
    --search-dir all:rp=/usr/local/lib64/ \
    --search-dir all:rp=/lib/x86_64-linux-gnu/ \
    --search-dir all:rp=/home/David.Schwalb/libev-4.15/ \
    --search-dir all:rp=/usr/lib/debug/usr/lib/ \
    --search-dir all:rp=/usr/lib/debug/lib/x86_64-linux-gnu/ \
    --user-data-dir ./"

PS3='Choose analysys: '
options=("Vtune General-Exploration" "Vtune Advanced-Hotspots" "Vtune Locks and Waits" "Vtune Memory Access" "Vtune Read-Bandwidth" "Perf Mem" "Quit")

select opt in "${options[@]}"
do
    case $opt in
        "Vtune General-Exploration")
            VTUNE_COLLECT=" -collect nehalem-general-exploration -knob enable-stack-collection=true"
            COMMAND="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER &"
            break
            ;;
        "Vtune Advanced-Hotspots")
          	VTUNE_COLLECT="-collect advanced-hotspots -knob collection-detail=stack-and-callcount"
            COMMAND="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER &"
            break
            ;;
        "Vtune Locks and Waits")
          	VTUNE_COLLECT="-collect locksandwaits"
            COMMAND="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER &"
            break
            ;;
        "Vtune Memory Access")
          	VTUNE_COLLECT="-collect nehalem-memory-access -knob enable-stack-collection=true"
            COMMAND="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER &"
            break
            ;;
        "Vtune Read-Bandwidth")
            VTUNE_COLLECT="-collect wsmex-read-bandwidth"
            COMMAND="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER &"
            break
            ;;
        "Perf Mem") 
            foldername=$(date +%Y%m%d-%H%M%S)
            COMMAND="./profiler_addr/addr -p $PROCESS_ID -o ./profiler/$foldername/perf.data &"
            break
            ;;
        "Quit")
			exit 0
            ;;
        *) echo invalid option;;
    esac
done

echo "---------------------------"
echo "Starting profiler: " $COMMAND
echo "---------------------------"
$COMMAND
