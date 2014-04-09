import matplotlib as mpl
mpl.use('Agg')
from pylab import *
import os

def collectRecoveryTimes(groupId):
    resultDir = os.path.join(os.getcwd(), "results", groupId)
    runs = os.listdir(resultDir)
    builds = os.listdir(os.path.join(resultDir, runs[0]))
    results = {}
    for build in builds:
        results[build] = {"recoveryTimes": [], "runTimes": []}
    for run in runs:
        for build in builds:
            resultFile = os.path.join(resultDir, run, build, "recoverytime.txt")
            recoveryTime = int(open(resultFile).read())
            results[build]["recoveryTimes"].append(recoveryTime / 1000.0)
            results[build]["runTimes"].append(int(run.replace("deltaFilltime","")))
    return results

if __name__ == "__main__":
    groupId = "tpcc_recovery"
    results = collectRecoveryTimes(groupId)
    fig = plt.figure()
    plt.title("TPC-C Recovery")
    plt.ylabel("Recovery Time in ms")
    plt.xlabel("Delta Filltime in s")

    for build in results:
        plotX = []
        plotY = []
        plotX, plotY = (list(t) for t in zip(*sorted(zip(results[build]["runTimes"], results[build]["recoveryTimes"]))))
        plt.plot(plotX, plotY, label=build)
    plt.legend(loc='upper left', prop={'size':10})
    plt.savefig("recovery.pdf")
    plt.close()