import os
import subprocess
import shutil
from os.path import isfile, join
import re
import csv
from collections import defaultdict

from Build import *


def columns_from_csv(filename, delimiter=";"):
	columns = defaultdict(list)
	with open(filename) as f:
		reader = csv.DictReader(f, delimiter=delimiter, quotechar='|', quoting=csv.QUOTE_MINIMAL)
		for row in reader:
			for (k,v) in row.items():
				try:
					value = int(v)
				except Exception, e:
					try:
						value = float(v)
					except Exception, e:
						value = v
				columns[k].append(value)
	return columns


class ResultManager(object):
	def __init__(self):
		self.results = []

	def add_result(self, result):
		self.results.append(result)

	def write_csv(self):
		if len(self.results) == 0:
			return

		with open('result.csv', 'wb') as csvfile:
			writer = csv.writer(csvfile, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
			# take first result as header
			header = ["_exp"]
			for key,value in self.results[0].items():
				header.append(key)
			header = sorted(header)
			writer.writerow(header)

			rownumber = 0
			for result in self.results:
				row = []
				for key in header:
					if key != "_exp":
						if key in result:
							row.append(result[key])
						else:
							row.append("None")
					else:
						row.append(rownumber)				
				rownumber = rownumber + 1
				writer.writerow(row)

class RegressionExperiment(object):

	def __init__(self, settingsfile, testname):
		self.settingsfile = settingsfile
		self.testname = testname
		self.hyrise_dir = "./hyrise/"
		self.bin_dir = "./builds/"+settingsfile+"/"
		self.build = Build(settingsfile)

	def execute(self):
		print self.settingsfile + ": Executing " + self.testname
		print "#########################"
		cwd = os.getcwd()
		os.chdir(self.hyrise_dir)
		exp_env = os.environ.copy()
		exp_env["HYRISE_DB_PATH"] = "." + self.bin_dir
		exp_env["LD_LIBRARY_PATH"] = "." + self.bin_dir+":/usr/local/lib64/"

		proc = subprocess.Popen([".%sperf_regression --gtest_filter=%s" % (self.bin_dir, self.testname)], env=exp_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		(out, err) = proc.communicate()
		print out
		os.chdir(cwd)
		return self.parse_result(out)

	def parse_result(self, resultstring):
		result = {"_testname":self.testname, "_settingsfile":self.settingsfile}
		for line in resultstring.splitlines():
			line = line.replace(" ", "")
			m = re.search(r"\[(\w+)\]", line)
			if m != None and m.group(1) == "MSG":
				line = line[5:]
				keyvalue = line.split(":")
				if len(keyvalue) >=2:
					result[keyvalue[0]] = float(keyvalue[1])
		return result


