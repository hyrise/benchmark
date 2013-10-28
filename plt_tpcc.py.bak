#!/usr/bin/env python

import numpy.numarray as na
import os

from pylab import *
from collections import defaultdict

data = {}
base_dir = './results/tpcc_testrun/'


for group in xrange(1,31):
	for build in os.listdir(base_dir + str(group)):
		if not build in data:
			data[build] = defaultdict(list)

		for num_users_str in os.listdir(base_dir + str(group) + '/' + build):
			num_users = int(num_users_str)
			new_orders = 0
			for user_str in os.listdir(base_dir + str(group) + '/' + build + '/' + num_users_str):
				f = open(base_dir + str(group) + '/' + build + '/' + num_users_str + '/' + user_str, 'r')
				for line in f.readlines():

					if(line.split(',')[0] != 'NEW_ORDER'):
						continue
					new_orders += 1

			data[build][group].append(new_orders)


locator_params(nbins=len(data.itervalues().next()), axis='x')

for build in data:
	y = [sum(x) for x in data[build].values()]
	plot(data[build].keys(), y, label=build)


xlim(1)
title("TPCC #users")
xlabel("Number of Users")
plt.ylabel('Total New Order Transactions per Second')
plt.legend(loc='upper left', prop={'size':10})
gca().get_xaxis().tick_bottom()
gca().get_yaxis().tick_left()
show()