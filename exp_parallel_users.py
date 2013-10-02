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


def id_generator(size=10, chars=string.ascii_uppercase + string.digits):
	return ''.join(random.choice(chars) for x in range(size))

def run_benchmark(max_users, resultdir, settingsfile, port):
	run_resultdir = resultdir+settingsfile+"/"
	os.makedirs(run_resultdir)
	for num_users in xrange(1, max_users+1):
		print ""
		print "Executing with " + str(num_users) + " Users..."
		print "######################################"
		b.script(num_users = num_users, time_factor = 2, prefix=run_resultdir+"users_"+str(num_users), port=port)
	print "Finished."

def start_server(settingsfile, port, verbose):
	hyrise_dir = "./hyrise/"
	bin_dir = "./builds/"+settingsfile+"/"

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
		proc = subprocess.Popen([server, "--port="+port], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=exp_env)
	os.chdir(cwd)
	time.sleep(1)
	return proc


def kill_server(pid):
	subprocess.call(["kill", "-SIGINT", "%d" % p.pid]) 
	p.communicate()


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
opts, args = parser.parse_args()

identifier = id_generator()
result_dir = "./results/" + identifier + "/"
os.makedirs(result_dir)

print "Starting benchmark, writing to " + result_dir

settingsfiles = ["nvram.mk", "nologger.mk", "bufferedlogger.mk"]
# settingsfiles = ["nvram.mk"]

for s in settingsfiles:
	build = Build(s)
	if opts.clean:
		build.make_clean()
	build.make_all()
	print ""
	if not opts.manual:
		p = start_server(s, port=opts.port, verbose=opts.verbose)
	else:
		print "Not starting server, manual mode..."
	try:
		run_benchmark(max_users=1, resultdir=result_dir, settingsfile=s, port=opts.port)
	finally:
		if not opts.manual:
			kill_server(p)
		build.cleanup()

print "Finished benchmark, results in " + result_dir




