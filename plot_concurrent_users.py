
import matplotlib as mpl
mpl.use('pdf')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
from helper.RegressionExperiment import *
import os



result_dir = "./result_concurrenct_users"
if not os.path.exists(result_dir):
	raise Exception("Result dir does not exist")

resultfiles = [ os.path.join(result_dir,f) for f in os.listdir(result_dir) if os.path.isfile(os.path.join(result_dir,f))]

fig=plt.figure()

for resultfile in resultfiles:
	columns = columns_from_csv(resultfile, delimiter=",")
	minimum = [i-20 for i in columns["end_throughput"]]
	maximum = [i+20 for i in columns["end_throughput"]]
	x_min = min(columns["num_users"])
	x_max = max(columns["num_users"])
	plt.fill_between(columns["num_users"], minimum, maximum, color='gray', edgecolor='none', alpha=0.5)
	plt.plot(columns["num_users"], columns["end_throughput"], label=os.path.basename(resultfile))


plt.legend(loc='upper right', prop={'size':10})

# plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.ylim(ymin=0)
plt.xlim(x_min, x_max)
plt.ylabel('Total Query Throughput')
plt.xlabel('# parallel Users')

pp = PdfPages('result_concurrent_users.pdf')
pp.savefig(fig)
pp.close()

