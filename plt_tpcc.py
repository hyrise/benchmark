import argparse
import os

import matplotlib as mpl
mpl.use('Agg')
from pylab import *

def collect(groupId):
	"""
	Returns a collection dictionary with information about a benchmark group.
	structure:
	{
		<Run ID>: {
			<Build ID>: [
				{<Transaction1>: [<runtime 1>, <runtime 2>, ...], ...}, // User 1
				{<Transaction2>: [<runtime 1>, <runtime 2>, ...], ...}, // User 2
				...
			]
		}
	}
	"""
	runs = {}
	dirResults = os.path.join(os.getcwd(), "results", groupId)
	if not os.path.isdir(dirResults):
		raise Exception("Group result directory '%s' not found!" % dirResults)
	for run in os.listdir(dirResults):
		dirRun = os.path.join(dirResults, run)
		if os.path.isdir(dirRun):
			runs[run] = {}
			for build in os.listdir(dirRun):
				dirBuild = os.path.join(dirRun, build)
				if os.path.isdir(dirBuild):
					runs[run][build] = []
					for user in os.listdir(dirBuild):
						dirUser = os.path.join(dirBuild, user)
						if os.path.isdir(dirUser):
							userStats = {}
							if not os.path.isfile(os.path.join(dirUser, "transactions.log")):
								print "WARNING: no transaction log found for user %s in run %s!" % (user, run)
								continue
							for rawline in open(os.path.join(dirUser, "transactions.log")):
								linedata = rawline.split(",")
								if len(linedata) < 2:
									continue
								transaction = linedata[0]
								runtime = linedata[1]
								userStats.setdefault(transaction,[]).append(float(runtime))
							runs[run][build].append(userStats)
	return runs

def aggregateUsers(runs, runId, buildId):
	buildData = runs[runId][buildId]
	numUsers = len(buildData)
	stats = {}
	for userStats in runs[runId][buildId]:
		for txId, txData in userStats.iteritems():
			stats.setdefault(txId, {})
			stats[txId].setdefault("total", 0)
			stats[txId].setdefault("min", 0.0)
			stats[txId].setdefault("max", 0.0)
			stats[txId].setdefault("average", 0.0)
			stats[txId].setdefault("median", 0.0)
			stats[txId]["total"] += len(txData)
			stats[txId]["min"] += amin(txData) / numUsers
			stats[txId]["max"] += average(txData) / numUsers
			stats[txId]["average"] += average(txData) / numUsers
			stats[txId]["median"] += median(txData) / numUsers
	return stats

def printStatistics(runs):
	for runId, runData in runs.iteritems():
		numUsers = len(runData[runData.keys()[0]])
		print "Run ID: %s [%s users]" % (runId, numUsers)
		print "=============================="
		for buildId, buildData in runData.iteritems():
			print "|\n+-- Build ID: %s" % buildId
			print "|"
			print "|     Transaction       #      min     max     avg     median"
			print "|     --------------------------------------------------------"
			for txId, txData in aggregateUsers(runs, runId, buildId).iteritems():
				print "|     {:16s}  {:5s}  {:1.4f}  {:1.4f}  {:1.4f}  {:1.4f}".format(txId, str(txData["total"]), txData["min"], txData["max"], txData["average"], txData["median"])
			print "|\n"


if __name__ == "__main__":
	aparser = argparse.ArgumentParser(description='Plotter for HYRISE TPC-C Benchmark results')
	aparser.add_argument('groupId', type=str, metavar="GROUP",
	                     help='Group ID for benchmark results to be plotted')
	aparser.add_argument('--statistics', action='store_true',
	                     help='Print run statistics and exit')
	args = vars(aparser.parse_args())

	runs = collect(args["groupId"])

	if args["statistics"]:
		printStatistics(runs)
		exit()

	dirOutput = os.path.join(os.getcwd(), "plots", args["groupId"])
	if not os.path.isdir(dirOutput):
		os.makedirs(dirOutput)

	# total transaction plot
	for runId, runData in runs.iteritems():
		#barData = {"DELIVERY": [], "ORDER_STATUS": [], "STOCK_LEVEL": [], "PAYMENT": [], "NEW_ORDER": []}
		barData = {}
		buildIds = []
		for buildId, buildData in runData.iteritems():
			buildIds.append(buildId)
			aggData = aggregateUsers(runs, runId, buildId)
			for txId, txData in aggData.iteritems():
				barData.setdefault(txId, []).append(txData["total"])
		width = 0.35
		ind = np.arange(len(buildIds))
		bottoms = [0]*len(buildIds)
		p1 = plt.bar(ind, barData["DELIVERY"],     width, color="red")
		bottoms = [x+y for x,y in zip(bottoms, barData["DELIVERY"])]
		p2 = plt.bar(ind, barData["ORDER_STATUS"], width, color="green",  bottom=bottoms)
		bottoms = [x+y for x,y in zip(bottoms, barData["ORDER_STATUS"])]
		p3 = plt.bar(ind, barData["STOCK_LEVEL"],  width, color="blue",   bottom=bottoms)
		bottoms = [x+y for x,y in zip(bottoms, barData["STOCK_LEVEL"])]
		p4 = plt.bar(ind, barData["PAYMENT"],      width, color="yellow", bottom=bottoms)
		bottoms = [x+y for x,y in zip(bottoms, barData["PAYMENT"])]
		p5 = plt.bar(ind, barData["NEW_ORDER"],    width, color="orange", bottom=bottoms)

		plt.title("Total transactions for Run %s" % runId)
		plt.ylabel("Total number of transactions")
		plt.xlabel("Build name")
		plt.xticks(arange(len(buildIds)), buildIds)
		plt.xlim(xmax=len(buildIds)+3)
		plt.legend([p1[0], p2[0], p3[0], p4[0], p5[0]], barData.keys())

		fname = os.path.join(dirOutput, "%s.pdf" % runId)
		plt.savefig(fname)
		plt.close()
