
import os
import sys
import re
import numpy
import collections
import matplotlib as mpl
from helper.ResultTree import ResultTree


def show_or_save(plt, fig, use_x11, filename):
	if use_x11:
		plt.show()
	else:
		pp = PdfPages(filename)
		pp.savefig(fig)
		pp.close()

# Prepare Results
#####################################

if len(sys.argv) < 2:
	raise Exception("Please specify result dictionary")

if len(sys.argv) == 3 and sys.argv[2] == "x11":
	use_x11 = True
else:
	use_x11 = False
	mpl.use('pdf')

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

result_dir = sys.argv[1]
if not os.path.exists(result_dir):
	raise Exception("Result dir does not exist")

results = ResultTree(result_dir, file_types="*q13insert.json").parse()
exec_times = results.filter(levels=[1,2,4], leafs=[4]).aggregate(levels=[4])
builds = exec_times.get_values(filter = lambda node: node.level==1, y = lambda node: node.name.split("/")[2])


# Plot Throughput for #users
######################################

fig=plt.figure()
for buildname in builds:
	result_dict = exec_times.get_key_value(
		filter = lambda node: node.level==4 and buildname in node.name,
		x = lambda node: int(re.sub("\D", "", node.name.split("/")[3])),
		y = lambda node: node)
	# plt.fill_between(result_dict.keys(), [max(x.median-x.std,0) for x in result_dict.values()], [max(x.median+x.std,0) for x in result_dict.values()], color='gray', edgecolor='none', alpha=0.3)
	plt.plot(result_dict.keys(), [len(x.result_dict["exec_time"]) for x in result_dict.values()] , label=buildname)

plt.legend(loc='upper right', prop={'size':10})
plt.ylim(ymin=0)
plt.xlim(1)
plt.ylabel('Total Query Throughput')
plt.xlabel('# parallel Users')
plt.title("Throughput for #users")
show_or_save(plt, fig, use_x11, os.path.join(result_dir,"throuput.pdf"))



# # Plot Latencies for #users
# #####################################

fig=plt.figure()
for buildname in builds:
	result_dict = exec_times.get_key_value(
		filter = lambda node: node.level==4 and buildname in node.name,
		x = lambda node: int(re.sub("\D", "", node.name.split("/")[3])),
		y = lambda node: node)
	lower = [numpy.median(x.result_dict["exec_time"])-numpy.std(x.result_dict["exec_time"]) for x in result_dict.values()]
	upper = [numpy.median(x.result_dict["exec_time"])+numpy.std(x.result_dict["exec_time"]) for x in result_dict.values()]
	plt.fill_between(result_dict.keys(), lower, upper, color='gray', edgecolor='none', alpha=0.3)
	plt.plot(result_dict.keys(), [numpy.median(x.result_dict["exec_time"]) for x in result_dict.values()] , label=buildname)

plt.legend(loc='upper right', prop={'size':10})
plt.ylim(ymin=0)
plt.xlim(1)
plt.ylabel('Median Query Latency in ns')
plt.xlabel('# parallel Users')
plt.title("Latency for #users")
show_or_save(plt, fig, use_x11, os.path.join(result_dir,"latencies_users.pdf"))



# Plot Latencies over time
#####################################

fig=plt.figure()

result_dict = exec_times.get_key_value(
	filter = lambda node: node.level==4 and "nvram.mk" in node.name,
	x = lambda node: int(re.sub("\D", "", node.name.split("/")[3])),
	y = lambda node: node)

for num_users in [1,2,4,8]:
	endtimes = result_dict[num_users].result_dict["endtime"]
	durations = result_dict[num_users].result_dict["exec_time"]
	endtime_min = int(min(endtimes))
	clustered_dict = collections.defaultdict(list)
	i=0
	for endtime in endtimes:
		clustered_dict[int(endtime)-endtime_min].append(durations[i])
		i = i + 1
	lower = [numpy.median(x)-numpy.std(x) for x in clustered_dict.values()]
	upper = [numpy.median(x)+numpy.std(x) for x in clustered_dict.values()]

	plt.fill_between(clustered_dict.keys(), lower, upper, color='gray', edgecolor='none', alpha=0.3)
	plt.plot(clustered_dict.keys(), [numpy.median(x) for x in clustered_dict.values()] , label=str(num_users) + " user")

plt.legend(loc='upper right', prop={'size':10})
plt.ylim(ymin=0)
plt.xlim(1)
plt.ylabel('Median Query Latency in ns')
plt.xlabel('Time in seconds')
plt.title("Latency over time")
show_or_save(plt, fig, use_x11, os.path.join(result_dir,"latencies_time.pdf"))





