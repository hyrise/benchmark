import sys
import os
import csv
import subprocess
import numpy as np
import time
import signal
import shutil
import string
import random
import optparse

import helper.benchmark.benchmark_main as b 
from helper.Build import *

DEVNULL = open(os.devnull, 'wb')
PMFS_FILE = "/mnt/pmfs/hyrise_david"

def id_generator(size=10, chars=string.ascii_uppercase + string.digits):
	return ''.join(random.choice(chars) for x in range(size))

def start_server(settingsfile, port, verbose):
	hyrise_dir = "./hyrise/"
	bin_dir = "./builds/"+settingsfile+"/"

	if os.path.exists(PMFS_FILE):
		print "Deleting " + PMFS_FILE
		os.remove(PMFS_FILE)

	cwd = os.getcwd()
	os.chdir(hyrise_dir)
	exp_env = os.environ.copy()
	exp_env["HYRISE_DB_PATH"] = "." + bin_dir
	exp_env["LD_LIBRARY_PATH"] = "." + bin_dir+":/usr/local/lib64/"
	exp_env["HYRISE_MYSQL_PORT"] = "3306"
	exp_env["HYRISE_MYSQL_HOST"] = "vm-hyrise-jenkins.eaalab.hpi.uni-potsdam.de"
	exp_env["HYRISE_MYSQL_USER"] = "hyrise"
	exp_env["HYRISE_MYSQL_PASS"] = "hyrise"

	devnull = open('/dev/null', 'w')
	server = "."+bin_dir+"hyrise_server"
	print "Starting server: " + server, port
	if verbose:
		proc = subprocess.Popen([server, "--port="+port], env=exp_env)
	else:
		proc = subprocess.Popen([server, "--port="+port], stdout=DEVNULL, env=exp_env)
	os.chdir(cwd)
	time.sleep(1)
	return proc

def kill_server(proc):
	print "Shutting down server..."
	proc.terminate()
	time.sleep(0.5)

	if proc.poll() is None:
		print "Server still running. Waiting 2 sec and forcing shutdown..."
		time.sleep(2)
		proc.kill()

	if proc.poll() is None:
		raise Exception("Something went wrong shutting down the server...")
	else:
		print "Server shut down..."



def run_benchmark(resultdir, settingsfile, opts):
	run_resultdir = resultdir+settingsfile+"/"
	os.makedirs(run_resultdir)
	for num_users in xrange(int(opts.lower), int(opts.upper)+1, int(opts.step)):

		# start server
		if not opts.manual:
			p = start_server(s, port=opts.port, verbose=opts.verbose)
		else:
			print "Not starting server, manual mode..."

		print ""
		print "Executing with " + str(num_users) + " Users..."
		print "######################################"
		
		try:
			b.script(num_users=num_users, time_factor=float(opts.runtime), prefix=run_resultdir+"users_"+str(num_users), port=opts.port, thinktime=float(opts.thinktime))
		finally:
			# kill server
			if not opts.manual:
				kill_server(p)		

	print "Finished."


parser = optparse.OptionParser()
parser.add_option("-m", "--manual", dest="manual", default=False, action="store_true",
    help="Start server manually")
parser.add_option("-c", "--clean", dest="clean", default=False, action="store_true",
    help="Execute clean before build")
parser.add_option("-p", "--port", dest="port", metavar="PORT", default="5000",
    help="Server port")
parser.add_option("-v", "--verbose",
	action="store_true", dest="verbose", default=False, 
    help='Verbose server output')

parser.add_option("-l", "--lower",
	dest="lower", default=1, 
    help='Lower bound for number of users')
parser.add_option("-u", "--upper",
	dest="upper", default=10, 
    help='Upper bound for number of users')
parser.add_option("-s", "--step",
	dest="step", default=1,
    help='Stepsize number of users')
parser.add_option("-t", "--thinktime",
	dest="thinktime", default=0, 
    help='Thinktime for users')
parser.add_option("-r", "--runtime",
	dest="runtime", default=5, 
    help='Runtime for benchmark')

opts, args = parser.parse_args()

identifier = id_generator()
result_dir = "./results/" + identifier + "/"
os.makedirs(result_dir)

print "Starting benchmark, writing to " + result_dir

settingsfiles = ["nvram.mk", "nologger.mk", "bufferedlogger.mk"]
# settingsfiles = ["nologger.mk"]

for s in settingsfiles:

	try:
		build = Build(s)
		if opts.clean:
			build.make_clean()
		build.make_all()
		print ""
		run_benchmark(resultdir=result_dir, settingsfile=s, opts=opts)
	finally:
		build.cleanup()

print "Finished benchmark, results in " + result_dir




