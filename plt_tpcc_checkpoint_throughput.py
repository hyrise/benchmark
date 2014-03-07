import argparse
import benchmark

if __name__ == "__main__":

  groupId = "tpcc_checkpoint_throughput_tmp"

  aparser = argparse.ArgumentParser(description='Plotter for HYRISE TPC-C Benchmark results')

  args = vars(aparser.parse_args())

  plotter = benchmark.Plotter(groupId, use_ab = True)

  # plotter.plotResponseTimesVaryingUsers()
  plotter.printStatistics()
  plotter.plotTotalThroughput()