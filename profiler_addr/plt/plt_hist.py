
import csv
import sys
import os
import argparse

import matplotlib as mpl
mpl.use('pdf')
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from scipy.stats import gaussian_kde
from scipy.integrate import simps, trapz
from collections import defaultdict
from itertools import cycle
import seaborn as sns

index = {}
latencies = {}
latencies_all = []
latencies["L1"] = []
latencies["L2"] = []
latencies["L3"] = []
latencies["Mem"] = []
latency_max = 0.0

stats = {}

def next_power_of_two(num):
  x = 1
  while (2**x < num):
    x = x + 1
  return 2**x

def process_row(row):
  global latency_max

  mem_level = row[index["memory_level_str"]].strip()
  latency   = int(row[index["data_latency"]].strip())
  address   = row[index["data_address"]].strip()
  ip        = row[index["ip"]].strip()

  latencies_all.append(latency)

  if latency > latency_max:
    latency_max = latency

  if mem_level == "L1-hit":
    latencies["L1"].append(latency)
  elif mem_level == "L2-hit":
    latencies["L2"].append(latency)
  elif mem_level == "L3-hit":
    latencies["L3"].append(latency)
  elif mem_level == "L3-miss":
    latencies["Mem"].append(latency)
  elif mem_level == "LFB-hit":
    if latency < 10:
      latencies["L1"].append(latency)
    elif latency < 32:
      latencies["L2"].append(latency)
    elif latency < 128:
      latencies["L3"].append(latency)
    else:
      latencies["Mem"].append(latency)
  else:
    print "unknown mem-level:", mem_level

def plot_hist():
  print "Plotting histogram..."
  plt.ioff()
  fig=plt.figure()
  plt.xscale('log', basex=2)
  plt.xlabel("Miss Latency in Cycles")
  plt.ylabel("Percent of Total Cycles Waiting for Loads")
  # plt.yscale('log', basey=2)
  # plt.ylim([1,100])
  xmin = 2
  xmax = next_power_of_two(latency_max)
  plt.xlim([xmin,xmax])
  for mem_level in latencies:
    if len(latencies[mem_level]) < 10:
      continue
    density = gaussian_kde(latencies[mem_level])
    density.covariance_factor = lambda : .05
    density._compute_covariance()
    xs = np.linspace(xmin,xmax,5000)
    plt.plot(xs, [density(x)*x for x in xs], label=mem_level)

    y = [(float)(density(x))*x for x in xs]
    print mem_level, "area:", trapz(y=y, x=xs), "sum:", sum(latencies[mem_level])

  plt.legend(loc='upper right')
  pp = PdfPages("hist_lat_all.pdf")
  pp.savefig(fig)
  pp.close()

def plot_sorted():
  print "Plotting sorted..."
  plt.ioff()
  fig=plt.figure()
  plt.yscale('log', basey=2)
  latencies_all_sorted = sorted(latencies_all)
  plt.plot(latencies_all_sorted)
  pp = PdfPages("sorted_lat_all.pdf")
  pp.savefig(fig)
  pp.close()

def plot_violin():
  print "Plotting violin..."
  plt.ioff()
  fig=plt.figure()

  sns.set(style="ticks")
  d = [[1,2,3,4,2,3,2,3], [3,4,2,3,5,6,7,7,7]]


  rs = np.random.RandomState(0)

  n, p = 40, 8
  d = rs.normal(0, 1, (n, p))
  d += np.log(np.arange(1, p + 1)) * -5 + 10


  f, ax = plt.subplots()
  sns.offset_spines()
  sns.violinplot(d)
  sns.despine(trim=True)


def plot_hist2(resultfile):

  global stats

  percent_mem_bound = 100 * 130 * (int)(stats["llc_miss"]) / (int)(stats["cycles_ref"])
  percent_mem_bound = 23
  print "percent_mem_bound", percent_mem_bound

  filename = resultfile[:-8] + "_hist2.pdf"
  print "Plotting %s..." % filename
  plt.ioff()
  fig=plt.figure()


  min, max = (0, 104)
  step = 2
  # Setting up a colormap that's a simple transtion
  mymap = mpl.colors.LinearSegmentedColormap.from_list('mycolors',['blue','red'])

  # Using contourf to provide my colorbar info, then clearing the figure
  Z = [[0,0],[0,0]]
  levels = range(min,max+step,step)
  levels=[0,percent_mem_bound,100]
  CS3 = plt.contourf(Z, levels, cmap=mymap)
  plt.clf()

  # Plotting what I actually want
  # plt.xscale('log', basex=2)
  plt.xlabel("Miss Latency in Cycles")
  plt.ylabel("Total Number of Cycles Waiting for Loads")
  markers = cycle(["o", "s", "D", "x", "v", "*"])
  
  width = 0.7

  max_xvalues = []
  for mem_level in latencies:
    if len(latencies[mem_level]) < 10: continue
    hist = defaultdict(int)
    for x in latencies[mem_level]: hist[x] += 1
    hist2 = defaultdict(int)
    aggregate = [(int)(1.2**x) for x in xrange(32)]
    i_start = 4
    i = i_start
    xvalues = []
    for aggr in aggregate:
      summe = 0
      for j in xrange(aggr):
        summe += hist[i+j]     
      hist2[i] = summe
      xvalues.append(i)
      i += aggr
    if len(xvalues) > len(max_xvalues):
      max_xvalues = xvalues
    plt.bar([x for x in range(len(xvalues))], [hist2[x]*x+100 for x in xvalues], width, label=mem_level)
    # plt.plot(xvalues, [hist2[x]*x for x in xvalues], label=mem_level, marker=next(markers), fillstyle="none")

  plt.legend(loc='upper right', numpoints=1)

  max_xvalues_new = []
  step = 4
  i = 0
  for x in max_xvalues:
    if i % step is 0:
      max_xvalues_new.append(x)
    i = i + 1

  plt.xticks([x*step for x in range(len(max_xvalues_new))], max_xvalues_new)
  
  # plotting the colorbar using the colorbar info I got from contourf
  plt.colorbar(CS3) 

  pp = PdfPages(filename)
  pp.savefig(fig)
  pp.close()


def plot_box():
  print "Plotting boxplot..."
  plt.ioff()
  fig=plt.figure()
  plt.yscale('log', basey=2)
  data = []
  for mem_level in latencies:
    if len(latencies[mem_level]) < 10:
      continue
    data.append(latencies[mem_level])
  plt.boxplot(data)
  pp = PdfPages("box_lat_all.pdf")
  pp.savefig(fig)
  pp.close()

def read_data(filename):
  failed_rows = 0
  processed_rows = 0
  total_rows = 0
  q = 0
  fails = 0

  sys.stdout.write("Loading data file: " + filename)
  sys.stdout.flush()
  
  with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    fsize = os.path.getsize(filename)
    for row in spamreader:
      q_new = (int)(((float)(csvfile.tell())/fsize)*100)
      if not q_new is q:
        q = q_new
        sys.stdout.write(".")
        sys.stdout.flush()

      total_rows = total_rows + 1

      # build index for name translation
      if row[0][0] == "#":
        i = 0
        for header in row:
          index[header.strip()] = i
          i = i + 1

      # process row
      elif len(row) is 15:
        process_row(row)
        processed_rows = processed_rows + 1
      
      # something went wrong
      else:
        fails = fails + 1
        continue

      # data[load_type].append(float(latency))
      # alldata.append(float(latency))

  print "\nProcessed %d samples. %s failed." % (total_rows, failed_rows)

def read_stats(filename):
  global stats
  sys.stdout.write("Loading data file: " + filename)
  sys.stdout.flush()

  with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    for row in spamreader:
      if row[0][0] == "#":
        header = row
      else:
        values = row

    if len(header) is not len(values):
      print "Error while reading resultfile. Mismatch between header and values."
    
    for i in range(len(header)):
      header_item = header[i]
      if header_item[0] == "#":
        header_item = header_item[1:]
      header_item = header_item.strip()
      stats[header_item] = values[i]
  print "\nStats:", stats

parser = argparse.ArgumentParser(description='Hyrise Latency Profiler - Result Plotter.')
parser.add_argument('resultfile', metavar='resultfile', type=str,
                   help='The resultfile to process')

args = parser.parse_args()
read_data(args.resultfile)
read_stats(args.resultfile[:-14]+"stats.perf.csv")

# plot_hist()
plot_hist2(args.resultfile)
# plot_box()
# plot_sorted()
# plot_violin()



