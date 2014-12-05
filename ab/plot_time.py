import numpy as np
import scipy.stats as stats
import pylab as pl
import csv

def import_data(filename):
  """Import data in the second column of the supplied filename as floats."""
  x = []
  first = 0
  with open(filename, 'rb') as inf:
    content = inf.readlines()
    for line in content:
      if (line[0] == '1'):
        if first == 0:
          first = int(line)
        x.append(int(line)-first)
  return x

# print import_data('ts.csv')

h = sorted(import_data('ts.csv'))

pl.hist(h, bins=100)
pl.savefig('time.png')
# pl.show() 