import argparse
import benchmark

if __name__ == "__main__":

  groupId = "tpcc_logger_windowsize"

  aparser = argparse.ArgumentParser(description='Plotter for HYRISE TPC-C Benchmark results')

  args = vars(aparser.parse_args())

  plotter = benchmark.Plotter(groupId, use_ab = True)

  # plotter.plotResponseTimesVaryingUsers()
  # plotter.printStatistics()

  plotter.plotTotalThroughput(xtitle="Logger Windowsize [ms]", x_parameter="windowsize_ms")