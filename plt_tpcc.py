import argparse
import benchmark

if __name__ == "__main__":
	aparser = argparse.ArgumentParser(description='Plotter for HYRISE TPC-C Benchmark results')
	aparser.add_argument('groupId', type=str, metavar="GROUP",
	                     help='Group ID for benchmark results to be plotted')
	aparser.add_argument('--statistics', action='store_true',
	                     help='Print run statistics and exit')
	args = vars(aparser.parse_args())

	plotter = benchmark.Plotter(args["groupId"])

	if args["statistics"]:
		plotter.printStatistics()
		exit()

	plotter.plotResponseTimesVaryingUsers()

	plotter.plotResponseTimeFrequencies()

	# total transaction plot
	if False:
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
