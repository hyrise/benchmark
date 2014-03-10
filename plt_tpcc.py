import argparse
import benchmark

if __name__ == "__main__":
	aparser = argparse.ArgumentParser(description='Plotter for HYRISE TPC-C Benchmark results')
	aparser.add_argument('--stats', action='store_true',
	                     help='Print run statistics and exit')

	args = vars(aparser.parse_args())

	groupId = "tpcc_clients_tmp"
	plotter = benchmark.Plotter(groupId, use_ab = True, preview=0)

	if args["stats"]:
		plotter.printStatistics()
		exit()

	plotter.plotTotalThroughput(xtitle="Number Parallel Users", x_parameter="numUsers")

	# plotter.plotResponseTimesVaryingUsers()
	# plotter.plotResponseTimeFrequencies()
	# plotter.plotTotalFails()
	# plotter.plotTransactionResponseTimes()

