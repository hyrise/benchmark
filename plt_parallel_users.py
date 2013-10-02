
import os
import sys
import re
import matplotlib as mpl
from helper.ResultTree import ResultTree


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

results = ResultTree(result_dir, file_types="*.json").parse()
exec_times = results.filter(levels=[1,2,5], leafs=[5], filters={5: "*/exec_time"}).aggregate(levels=[5])

builds = exec_times.get_values(filter = lambda node: node.level==1, y = lambda node: node.name.split("/")[2])



# Plot Throughput for #users
#####################################

fig=plt.figure()
for buildname in builds:
	result_dict = exec_times.get_key_value(
		filter = lambda node: node.level==5 and buildname in node.name,
		x = lambda node: re.sub("\D", "", node.name.split("/")[3]),
		y = lambda node: node)
	# plt.fill_between(result_dict.keys(), [max(x.median-x.std,0) for x in result_dict.values()], [max(x.median+x.std,0) for x in result_dict.values()], color='gray', edgecolor='none', alpha=0.3)
	plt.plot(result_dict.keys(), [x.count for x in result_dict.values()] , label=buildname)

plt.legend(loc='upper right', prop={'size':10})
plt.ylim(ymin=0)
plt.xlim(1)
plt.ylabel('Total Query Throughput')
plt.xlabel('# parallel Users')
plt.title("Throughput for #users")

if use_x11:
	plt.show()
else:
	pp = PdfPages(os.path.join(result_dir,'throughput.pdf'))
	pp.savefig(fig)
	pp.close()




# Plot Latencies for #users
#####################################

fig=plt.figure()
for buildname in builds:
	result_dict = exec_times.get_key_value(
		filter = lambda node: node.level==5 and buildname in node.name,
		x = lambda node: re.sub("\D", "", node.name.split("/")[3]),
		y = lambda node: node)
	plt.fill_between(result_dict.keys(), [max(x.median-x.std,0) for x in result_dict.values()], [max(x.median+x.std,0) for x in result_dict.values()], color='gray', edgecolor='none', alpha=0.3)
	plt.plot(result_dict.keys(), [x.median for x in result_dict.values()] , label=buildname)

plt.legend(loc='upper right', prop={'size':10})
plt.ylim(ymin=0)
plt.xlim(1)
plt.ylabel('Median Query Latency in ns')
plt.xlabel('# parallel Users')
plt.title("Latency for #users")

if use_x11:
	plt.show()
else:
	pp = PdfPages(os.path.join(result_dir,'latencies.pdf'))
	pp.savefig(fig)
	pp.close()






