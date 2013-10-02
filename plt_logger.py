import matplotlib as mpl
mpl.use('pdf')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
from helper.RegressionExperiment import *

plt.ioff()

fig=plt.figure()
columns = columns_from_csv('result.csv')
plt.bar(columns["_exp"], columns["RUNTIME_MEAN"], width=0.5,  color='r', align='center', alpha=0.4)
plt.xticks(columns["_exp"], columns["_settingsfile"])
plt.ylabel('uSecs')

pp = PdfPages('foo_runtime.pdf')
pp.savefig(fig)
pp.close()