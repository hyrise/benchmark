import os
import subprocess
import shutil
import select
from os.path import isfile, join
import sys
import multiprocessing
import Queue


class Build(object):
		
	def __init__(self, settingsfile):
		self.settingsfile = settingsfile
		self.result_dir = "./builds/"+settingsfile+"/"
		self.source_dir = "./hyrise/"
		self.build_dir = "./hyrise/build/"
		self.build_dir_org = "./hyrise/build_org/"
		self.log_dir = "./logs/"
		self.logfilename = self.log_dir+self.settingsfile+"_log.txt"
		self.verbose = False
		self.build_files = ["hyrise_server", "perf_regression", "libaccess.so", "libbackward-hyr.so", "libebb.so", "libftprinter-hyr.so", "libgtest-hyr.so", "libhelper.so", "libio.so", "libjson.so", "liblayouter.so", "libnet.so", "libstorage.so", "libtaskscheduler.so", "libtesting.so"]

		if not isfile("./settings/"+self.settingsfile):
			raise Exception("Settings file not existing:" + self.settingsfile)
		if not os.path.exists(self.log_dir):
			os.mkdir(self.log_dir)

		self.setup_build()

	def cleanup(self):
		# cleanup leftover from old runs
		if os.path.exists(self.build_dir_org):
			# remove symlink
			if os.path.exists(self.build_dir[:-1]):
				os.unlink(self.build_dir[:-1])
			# move build dir_org to build
			os.rename(self.build_dir_org, self.build_dir)

	def setup_build(self):
		# copy settings file
		if os.path.isfile(self.source_dir+"settings.mk"):
			os.remove(self.source_dir+"settings.mk")
		shutil.copy2("./settings/"+self.settingsfile, self.source_dir+"settings.mk")
		
		#cleanup
		self.cleanup()

		# create build folder if necessary
		if not os.path.exists(self.result_dir):
			# clean build dir so we can copy it
			self.make_clean()
			# rename build dir to build_org so we can create a symlink
			os.rename(self.build_dir, self.build_dir_org)
			# copy build dir
			shutil.copytree(self.build_dir_org, self.result_dir, symlinks=False, ignore=None)
			# create symlink to build folder
			os.symlink("."+self.result_dir, self.build_dir[:-1])
		else:
			# rename build dir to build_org so we can create a symlink
			os.rename(self.build_dir, self.build_dir_org)
			# create symlink to build folder
			os.symlink("."+self.result_dir, self.build_dir[:-1])

	def make_clean(self):
		cwd = os.getcwd()
		os.chdir(self.source_dir)
		self.print_status("Clearing build enviroment")
		proc = subprocess.Popen(["make clean"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		(out, err) = proc.communicate()
		os.chdir(cwd)
		if self.verbose: print out

	def make_all(self):
		logfile = open(self.logfilename,"w")
		last_logfile_lines = Queue.Queue(maxsize=20)
		cwd = os.getcwd()
		os.chdir(self.source_dir)
		self.print_status("Building system")
		proc = subprocess.Popen(["make -j%s" % multiprocessing.cpu_count()], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		
		while True:
			reads = [proc.stdout.fileno(), proc.stderr.fileno()]
			ret = select.select(reads, [], [])
			for fd in ret[0]:
				if fd == proc.stdout.fileno():
					read = proc.stdout.readline()
					short = "."
				if fd == proc.stderr.fileno():
					read = "ERROR: " + proc.stderr.readline()
					short = "|"

				if self.verbose:
					sys.stdout.write(read)
				else:
					sys.stdout.write(short)
					sys.stdout.flush()
				
				if last_logfile_lines.full():
					last_logfile_lines.get()
				last_logfile_lines.put(read)

				logfile.write(read)
				logfile.flush()

			if proc.poll() != None:
				break
		# proc.wait()
		os.chdir(cwd)
		logfile.close()
		print ""
		if proc.returncode != 0 or not self.check_build_results():
			print "######################"
			print "Last lines from " + self.logfilename
			print "######################"
			while not last_logfile_lines.empty():
				sys.stdout.write(last_logfile_lines.get())
			raise Exception("Something went wrong building Hyrise. Settings: " + self.settingsfile)

	def check_build_results(self):
		for f in self.build_files:
			if not os.path.isfile(self.result_dir+f):
				print "Error: Missing File " + self.result_dir+f
				return False
		return True

	def print_status(self, status):
		print self.settingsfile + ": " + status + "..."

	def log(self, command, message):
		with open(self.logfile,"a+") as f:
			f.write(command)
			f.write("\n")
			f.write("###################")
			f.write("\n")
			f.write(message)
			f.write("\n")


			