
import csv
import sys
import matplotlib as mpl
mpl.use('pdf')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
from scipy.stats import gaussian_kde
import os

start_address = int("0x7fcf4a3f6000", 0)
end_address = int("0x7fd132876000", 0)

def plot(data, name):
  plt.ioff()
  fig=plt.figure()
  hist, bins = np.histogram(data, bins=200)
  width = 0.7 * (bins[1] - bins[0])
  center = (bins[:-1] + bins[1:]) / 2
  plt.bar(center, hist, align='center', width=width)
  pp = PdfPages(sys.argv[1] + "/" + name)
  pp.savefig(fig)
  pp.close()

def plot_density(data, name):

  plt.ioff()
  # plt.yscale('log')
  fig=plt.figure()
  
  
  density = gaussian_kde(data)
  xs = np.linspace(1,2000,200)
  density.covariance_factor = lambda : .25
  density._compute_covariance()
  
  plt.plot(xs,density(xs))
  
  pp = PdfPages(name)
  pp.savefig(fig)
  pp.close()

types = []
data = {}
alldata = []

if len(sys.argv) != 2:
  print "ERROR: please specify result folder"
  exit(0)

filename = sys.argv[1] + '/perf.txt'
fails = 0

with open(filename, 'rb') as csvfile:
  print "before"
  sys.stdout.flush()
  spamreader = csv.reader(csvfile, delimiter=';')
  fsize = os.path.getsize(filename)
  print "after"
  sys.stdout.flush()
  for row in spamreader:

    q=csvfile.tell()/fsize
    # if q%100==0:
    # print q

    if len(row)<1 or row[0][0] == "#":
      continue

    # if len(row)<7:
      # fails = fails + 1
      # continue

    load_type = row[3].strip()
    latency = row[2].strip()
    # address = row[6].strip()

    # if len(address) > 4:
      # address = address[4:]
    # if address[:2] == "0x": 
      # address = int(address, 0)
      # if address >= start_address and address <= end_address:
        # load_type = load_type + "_spec"

    if load_type not in types:
      types.append(load_type)
      data[load_type] = []

    data[load_type].append(float(latency))
    alldata.append(float(latency))

print "Failed rows:", fails

for load_type in types:
  print load_type
  plot(data[load_type], "CMH_"+load_type+".pdf")

# print "alldata"
# plot_density(alldata, "CMH_all.pdf")


plt.ioff()

fig=plt.figure()
plt.xscale('log', basex=2)
plt.yscale('log', basey=2)
plt.ylim([1,100])
# plt.xlim([0,700])

for load_type in types:
  print load_type, len(data[load_type])
  if len(data[load_type]) < 10:
    print "Skipping: ", load_type
  else:
    density = gaussian_kde(data[load_type])
    xs = np.linspace(1,4096,10000)
    density.covariance_factor = lambda : .25
    density._compute_covariance()
    plt.plot(xs,density(xs)*1000, label=load_type)


plt.legend(loc='upper right')

name = sys.argv[1] + "/" + "CMH_all.pdf"
pp = PdfPages(name)
pp.savefig(fig)
pp.close()






