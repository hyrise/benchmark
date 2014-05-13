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
        results[build] = {"recoveryTimes": [], "runTimes": [], "fillLevels": []}
    for run in runs:
        for build in builds:
            resultFile = os.path.join(resultDir, run, build, "recoverytime.txt")
            result = open(resultFile).readline().split(";")
            recoveryTime = int(float(result[0]))
            stockSize = int(result[1])
            results[build]["recoveryTimes"].append(recoveryTime / 1000.0)
            results[build]["runTimes"].append(int(run.replace("deltaFilltime","")))
            results[build]["fillLevels"].append(stockSize / 1000.0)
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
    plt.legend(loc='lower right', prop={'size':10})
    plt.savefig("recovery.pdf")
    plt.close()

    fig = plt.figure()
    plt.title("TPC-C Recovery")
    plt.ylabel("Recovery Time in ms")
    plt.xlabel("STOCK Delta Size in k")

    for build in results:
        plotX = []
        plotY = []
        plotX, plotY = (list(t) for t in zip(*sorted(zip(results[build]["fillLevels"], results[build]["recoveryTimes"]))))
        plt.plot(plotX, plotY, label=build)
    plt.legend(loc='lower right', prop={'size':10})
    plt.savefig("recovery_size.pdf")
    plt.close()
