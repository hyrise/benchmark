import requests
import time
import re
import json
import os
import signal
import sys

r = requests.post("http://localhost:6666/statistics", data="1")
answer = json.loads(r.text)
print answer
oldTimestamp = answer['timestamp']
oldWrites = answer['write']
devnull = open('/dev/null', 'w')
myenv = os.environ.copy()
os.kill(int(sys.argv[1]), signal.SIGKILL)
while True:
	r = requests.post("http://localhost:6666/statistics", data="1")
	answer = json.loads(r.text)
	print answer
	# if answer['write'] <> oldWrites:
		# break

# print answer
# print "It happened at %f" % float(answer['timestamp']) - oldTimestamp