import argparse
import benchmark

if __name__ == "__main__":

  groupId = "tpcc_checkpoint_throughput_tmp"

  aparser = argparse.ArgumentParser(description='Plotter for HYRISE TPC-C Benchmark results')

  args = vars(aparser.parse_args())

  plotter = benchmark.Plotter(groupId, use_ab = True)

  xtitle = "Checkpointing Frequency [s]"
  xtitle_converter = lambda x: x/1000
  x_parameter = "checkpoint_interval"
  
  plotter.plotTotalThroughput(xtitle=xtitle, x_parameter=x_parameter, xtitle_converter = xtitle_converter)
  plotter.plotOverTime()
  plotter.plotTotalFails(xtitle=xtitle, x_parameter=x_parameter, xtitle_converter = xtitle_converter)
  