import sys
import os
import csv
import subprocess
import numpy as np
import time
import signal
import shutil

import helper.benchmark.benchmark_main as b 
from helper.Build import *

def crunch_numbers(max_users, resultfilename, resultdir):
	results = []
	for num_users in xrange(1, max_users+1):
		result_dict = crunch_numbers_for_users(num_users, resultdir)
		result_dict = result_dict[0]
		result_dict["num_users"] = num_users
		results.append(result_dict)

	f = open(resultfilename, 'wt')
	try:
		fieldnames = sorted(results[0].keys())
		writer = csv.DictWriter(f, fieldnames=fieldnames)
		headers = dict( (n,n) for n in fieldnames )
		writer.writerow(headers)
		for result_dict in results:
			writer.writerow(result_dict)
	finally:
		f.close()

def crunch_numbers_for_users(num_users, resultdir):

	execution_times_by_query = {}
	end_times_by_query = {}
	queries = []

	for user in range(num_users):
		path = resultdir + ("resultsqueued_1k_idx_1_users_%s/%s/" %(num_users, user))
		resultfiles = [ f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f)) ]

		for f in resultfiles:
			with file(os.path.join(path,f)) as csvfile:
				csvreader = csv.reader(filter(lambda row: row[0]!='#', csvfile), delimiter=' ', quotechar='|')
				for row in csvreader:
					end_time = float(row[0])
					exec_time = float(row[1])
					f = "all"
					if f in queries:
						execution_times_by_query[f].append(exec_time)
						end_times_by_query[f].append(end_time)
					else:
						queries.append(f)
						execution_times_by_query[f] = [exec_time]
						end_times_by_query[f] = [end_time]

	result = []
	for query in queries:
		result_dict = {}
		result_dict["query"] = query

		execution_times = execution_times_by_query[query]
		result_dict["exec_avg"] = np.average(execution_times)
		result_dict["exec_median"] = np.median(execution_times)
		result_dict["exec_min"] = min(execution_times)
		result_dict["exec_max"] = max(execution_times)
		result_dict["exec_std"] = np.std(execution_times)

		end_times = end_times_by_query[query]
		result_dict["end_duration"] = max(end_times) - min(end_times)
		result_dict["end_count"] = len(end_times)
		result_dict["end_throughput"] = result_dict["end_count"] / result_dict["end_duration"]
		result.append(result_dict)

	return result


def run_benchmark(max_users, resultfilename, settingsfile):
	resultdir = "./result_concurrenct_users/"+settingsfile+"/"
	os.mkdir(resultdir)
	for num_users in xrange(1, max_users+1):
		print ""
		print "Executing with " + str(num_users) + " Users..."
		print "######################################"
		#execute
		b.script(num_users = num_users, time_factor = 5)
		# rename result dir so its not overwritten
		shutil.move("./resultsqueued_1k_idx_1", resultdir+"resultsqueued_1k_idx_1_users_"+str(num_users))
	crunch_numbers(max_users, resultfilename, resultdir)

def start_server(settingsfile):
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
	proc = subprocess.Popen(["."+bin_dir+"hyrise_server"], stdout=devnull, stderr=devnull, env=exp_env) #stdout=subprocess.PIPE, stderr=subprocess.PIPE,   |     
	os.chdir(cwd)
	time.sleep(1)
	return proc


def kill_server(pid):
	subprocess.call(["kill", "-SIGINT", "%d" % p.pid]) 
	p.communicate()
	

result_dir = "./result_concurrenct_users"
if os.path.exists(result_dir):
	raise Exception("Result dir already exists")
else:
	os.mkdir(result_dir)

settingsfiles = ["bufferedlogger.mk", "nvram.mk", "nologger.mk"]

for s in settingsfiles:
	build = Build(s)
	print ""
	p = start_server(s)
	try:
		run_benchmark(max_users=3, resultfilename=result_dir+"/result_"+s+".csv", settingsfile=s)
	finally:
		kill_server(p)





