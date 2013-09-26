import threading
import envoy
import time
import os

class LayoutUser(threading.Thread):

	def __init__(self,userid, table, layout, core=7, prefix="", port=5000):
		threading.Thread.__init__(self)
		self._userid = userid
		self._table = table
		self._layout = layout
		self._core = core
		self._prefix = prefix
                self._port = port

	def basePath(self):
		return self._prefix


	def run(self):
		cmdline = "ruby -rubygems -I. layout.rb %s %s %s %d" % (self._table, self._layout, self._core, self._port)
		print "Starting Layout Change"
		begin = time.time()
		r = envoy.run(cmdline)
		end = time.time()
		filename = os.path.join(self.basePath(), "merge_times_%s.txt" % self._table)
		with open(filename, "w+") as f:
			f.write("%f %f\n" %(begin, end))
		print "Finished Layout Change %d" % r.status_code
