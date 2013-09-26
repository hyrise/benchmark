
import matplotlib as mpl
mpl.use('pdf')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
from helper.RegressionExperiment import *




result_dir = "./result_concurrenct_users"
if not os.path.exists(result_dir):
	raise Exception("Result dir does not exist")

resultfiles = [ os.path.join(result_dir,f) for f in os.listdir(result_dir) if os.path.isfile(os.path.join(result_dir,f))]

fig=plt.figure()

for resultfile in resultfiles:
	print resultfile
	columns = columns_from_csv(resultfile, delimiter=",")
	print columns
	plt.plot(columns["num_users"], columns["end_throughput"])
	print columns["num_users"]

# plt.bar(columns["_exp"], columns["RUNTIME_MEAN"], width=0.5,  color='r', align='center', alpha=0.4)
# plt.xticks(columns["_exp"], columns["_settingsfile"])
# plt.ylabel('uSecs')

pp = PdfPages('result_concurrent_users.pdf')
pp.savefig(fig)
pp.close()