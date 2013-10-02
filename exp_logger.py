import sys
from helper.RegressionExperiment import *

test = "InsertScanBase.insert_single_tx_commit"
builds = ["bufferedlogger.mk", "nvram.mk", "nologger.mk"]

results = ResultManager()
for build in builds:
	exp = RegressionExperiment(build, test)
	result = exp.execute()
	results.add_result(result)

results.write_csv()



