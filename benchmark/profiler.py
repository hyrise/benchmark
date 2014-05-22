
import os
import subprocess
import time
import sys
import signal

class Profiler():


    def __init__(self, hyrise_path):
        self.hyrise_path = hyrise_path
        self.vtune_command="/opt/intel/vtune_amplifier_xe_2013/bin64/amplxe-cl "
        self.vtune_parameter="-target-pid $PROCESS_ID\
            -follow-child -mrte-mode=auto -target-duration-type=short -no-allow-multiple-runs \
            -no-analyze-system -data-limit=500 -slow-frames-threshold=40 -fast-frames-threshold=100 \
            --search-dir all:rp=$HYRISE_PATH/build \
            --search-dir all:rp=/usr/local/lib64/ \
            --search-dir all:rp=/lib/x86_64-linux-gnu/ \
            --search-dir all:rp=/home/David.Schwalb/libev-4.15/ \
            --search-dir all:rp=/usr/lib/debug/usr/lib/ \
            --search-dir all:rp=/usr/lib/debug/lib/x86_64-linux-gnu/ \
            --user-data-dir ./"
        self.profile_process = None
        self.postprocess_command = None
        self.command=None
        self.hyrise_pid = None

    def choice(self, choices, title):
        print title
        letters = "abcdefghijklmnopqrstuvwxyz"
        i = 0
        for x in choices:
            print "("+letters[i]+") " + x
            i = i + 1

        sys.stdout.write("-> ")
        level = raw_input()
        chosen = letters.find(level)

        if chosen < 0 or chosen > len(choices):
            print "Invalid choice. Try again."
            return choice(choices, title)
        return choices[chosen]

    def setup(self, profiler_type):

        choices = ["vtune_ge", "vtune_ah", "vtune_lw", "vtune_ma", "vtune_rb", "sampler", "None"]
        if profiler_type is "interactive":
            chosen = self.choice(choices, "Choose profiler analysis:")
        else:
            if profiler_type in choices:
                chosen = profiler_type
            else:
                print "ERROR: unknown profiler specified (" + profiler_type + ")"
                print "Available: ", str(choices)
                exit(1)

        if chosen == "vtune_ge":
            VTUNE_COLLECT=" -collect nehalem-general-exploration -knob enable-stack-collection=true"
            self.command="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER"
        if chosen == "vtune_ah":
            VTUNE_COLLECT="-collect advanced-hotspots -knob collection-detail=stack-and-callcount"
            self.command="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER"
        if chosen == "vtune_lw":
            VTUNE_COLLECT="-collect locksandwaits"
            self.command="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER"
        if chosen == "vtune_ma":
            VTUNE_COLLECT="-collect nehalem-memory-access -knob enable-stack-collection=true"
            self.command="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER"
        if chosen == "vtune_rb":
            VTUNE_COLLECT="-collect wsmex-read-bandwidth"
            self.command="$VTUNE_COMMAND $VTUNE_COLLECT $VTUNE_PARAMETER"
        if chosen == "sampler":
            foldername=str(int(time.time()))
            os.makedirs("./profiler_results/" + foldername)
            self.command="./profiler_addr/bin/latency_profiler -p $PROCESS_ID -o ./profiler_results/" + foldername + "/perf.data"
            # self.command="echo 'test'"
            # self.postprocess_command = "./profiler/perf_postprocess.sh " + foldername
            # print "Perf needs sudo..."
            # subprocess.call(["/usr/bin/sudo", "touch", "."])
        if chosen == "None":
            self.command=None

        print "Profiler command:", self.command

    def start(self, hyrise_pid):

        if self.command is None:
            return

        self.hyrise_pid = hyrise_pid
        self.command = self.command.replace("$PROCESS_ID", hyrise_pid)

        print "---------------------------"
        print "Starting profiler: ", self.command
        sys.stdout.flush()
        self.profile_process = subprocess.Popen(self.command, shell=True, preexec_fn=os.setsid)
        time.sleep(1)
        print "---------------------------"

    def end(self):
        if self.profile_process is not None:
            print "Profiler terminating..."
            # os.killpg(self.profile_process.pid, signal.SIGTERM)
            print self.profile_process.communicate()
            print "Profiler terminated"
        if self.postprocess_command is not None:
            self.postprocess_command = self.postprocess_command.split(" ")
            print "Profiler postprocess"
            # print "Please execute command"
            # print self.postprocess_command
            # print "OK?"
            # raw_input()
            subprocess.call(self.postprocess_command)
            print "Profiler postprocess done"


