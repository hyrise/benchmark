from tpcc_parameters import *
from thread import start_new_thread
import shutil
import subprocess
import os
import time
import sys
import json
import time

groupId = "recovery_demo"
num_clients = args["clients"]
kwargs["numUsers"] = args["clients"]
kwargs["hyriseDBPath"] = "/mnt/fusion/nichtmarkus/hyrise_persistency/"

clear_dir(kwargs["hyriseDBPath"])

benchmarks = {}
ports = {'nvram': 5142, 'loggercp': 5143, 'logger': 5144}
ab_cores = {'nvram': 43, 'loggercp': 45, 'logger': 47}
nodes = {'nvram': 1, 'loggercp': 2, 'logger': 0}

CHECKPOINT_WITH_MAIN_JSON = """
{"operators": {"l": {"type": "Checkpoint", "withMain": true}}, "edges": [["l","l"]]}
"""

os.system("kill -9 `pgrep hyrise`")
os.system("clear")


print """
      +-------------+
      |             |
      |             |
+-----|-------+     |
|     |       |     |
|     |  HPI  |     |
|     |       |     |
|     +-------------+
|             |
|             |
+-------------+

HYRISE Instant Recovery Demo


===================================================================================
Please stand by while HYRISE instances are being started and initial data is loaded
===================================================================================

"""

benchmarks['nvram'] = create_benchmark_nvram("NVRAM", groupId, {}, kwargs)
# benchmarks['logger'] = create_benchmark_logger("Logger", groupId, {}, kwargs, windowsize_ms=20, checkpoint_interval_ms=0)
# benchmarks['loggercp'] = create_benchmark_logger("LoggerCP", groupId, {}, kwargs, windowsize_ms=20, checkpoint_interval_ms=10000)

def write_event(benchmark, event):
	eventfile = benchmark._dirResults + "/events.json"
	if os.path.isfile(eventfile):
		with open(eventfile) as f:
			data = json.load(f)
	else:
		data = {}

	data.update({time.time(): event})

	with open(eventfile, 'w') as f:
	    json.dump(data, f)
	    f.flush()

def run_server(name, benchmark):
	print "starting " + name + "...",
	while True:
		benchmark._startServer()
		write_event(benchmark, "restart")
		benchmark._recoverOnStart = True
		print "done."
		subprocess.Popen.wait(benchmark._serverProc)
		print "restarting " + name + "...",

def check_abs(ab_processes):
	while True:
		for ab in ab_processes:
			if subprocess.Popen.poll(ab) is not None:
				print "Query execution has ended"
				sys.exit()
		time.sleep(2)

for name, benchmark in benchmarks.iteritems():
	benchmark._buildServer()
	benchmark._buildAb()
	benchmark._port = ports[name]
	benchmark._stdout = benchmark._stderr = False
	benchmark._abCore = ab_cores[name]
	benchmark._memorynodes = benchmark._nodes = nodes[name]
	benchmark._verbose = 0
	start_new_thread(run_server, (name, benchmark))
	time.sleep(1)
	benchmark.benchPrepare()
	benchmark.loadTables()
	if benchmark._buildSettings['PERSISTENCY'] == 'BUFFEREDLOGGER':
		benchmark.fireQuery(CHECKPOINT_WITH_MAIN_JSON)

ab_processes = []
for name, benchmark in benchmarks.iteritems():
	clear_dir(benchmark._dirResults)
	ab_processes.append(subprocess.Popen(["./ab/ab","-G", benchmark._dirResults + "/ab.log", "-l", str(benchmark._abCore), "-v", str(benchmark._verbose), "-k", "-t", str(benchmark._runtime), "-n", "99999999", "-c", str(benchmark._numUsers), "-r", "-m", benchmark._abQueryFile, benchmark._host+":"+str(benchmark._port)+"/procedure/"]))
start_new_thread(check_abs, (ab_processes,))

time.sleep(10)
os.system("clear")

print """
      +-------------+
      |             |
      |             |
+-----|-------+     |
|     |       |     |
|     |  HPI  |     |
|     |       |     |
|     +-------------+
|             |
|             |
+-------------+

HYRISE Instant Recovery Demo

=====================================
Press RETURN to kill HYRISE instances
=====================================

"""

while True:
	raw_input()
	os.system("kill -9 `pgrep hyrise`")
	write_event(benchmark, "kill")
	print "BAM!"
	print ""